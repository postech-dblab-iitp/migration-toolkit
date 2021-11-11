/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */
package com.cubrid.common.ui.swt.table;

import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.CheckboxCellEditor;
import org.eclipse.jface.viewers.IBaseLabelProvider;
import org.eclipse.jface.viewers.ICellEditorValidator;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TableViewerColumn;
import org.eclipse.jface.viewers.TextCellEditor;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.TableCursor;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;

import com.cubrid.common.ui.listener.KeyStateMaskAdapter;
import com.cubrid.common.ui.listener.PopControlContextMenuListener;
import com.cubrid.common.ui.swt.Resources;
import com.cubrid.common.ui.swt.table.listener.AutoLocateTableCursorForTableViewerSelectionChangedListener;
import com.cubrid.common.ui.swt.table.listener.CheckStyleTableSelectionListener;
import com.cubrid.common.ui.swt.table.listener.SpaceKeyOnTableCursorToEditListener;
import com.cubrid.common.ui.swt.table.listener.TableCursorMouseClickListener;
import com.cubrid.common.ui.swt.table.listener.TableCursorSelectionListener;
import com.cubrid.common.ui.swt.table.listener.TextCellEditorForTableCursorFocusListener;
import com.cubrid.common.ui.swt.table.listener.TextCellEditorUpDownKeyListener;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * An utility to build a table viewer
 * 
 * @author Kevin Cao
 * 
 */
public class TableViewerBuilder {

	/**
	 * The lasted selection will be set into table's data
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private static final class SaveLastSelectionListener implements
			ISelectionChangedListener {
		public void selectionChanged(SelectionChangedEvent event) {
			TableViewer tableViewer = (TableViewer) event.getSelectionProvider();
			setTableLastSelection(tableViewer.getTable());
		}

		private void setTableLastSelection(Table table) {
			if (table.getSelectionCount() == 0) {
				table.setData(LAST_SELECTION, null);
				return;
			}
			table.setData(LAST_SELECTION, table.getSelection()[0]);
		}
	}

	private static final Color COLOR_BACKGROUND = Resources.getInstance().getColor(221, 221, 221);
	private static final Color COLOR_BLACK = Resources.getInstance().getColor(SWT.COLOR_BLACK);
	private static final String LAST_SELECTION = "lastSelection";

	private String[] columnNames;
	private int[] columnStyles;
	private String[] columnImageFiles;
	private String[] tooltips;
	private int[] columnWidths;
	private CellEditorFactory[] cellEditorFactorys;
	private boolean tableCursorSupported;
	private ICellModifier cellModifier;
	private KeyStateMaskAdapter keyStateMaskAdapter;
	private ICellEditorValidator[] cellEditorValidators;
	private IStructuredContentProvider contentProvider;
	private IBaseLabelProvider labelProvider;
	private boolean rowIsDataArray = true;

	/**
	 * Create table cursor for the table viewer, an the table cursor will be set
	 * into the data property "tableCursor"
	 */
	private void addTableCursorForTableViewer(final TableViewer tableViewer) {
		if (!tableCursorSupported) {
			return;
		}
		final Table table = tableViewer.getTable();
		final TableCursor tableCursor = new TableCursor(table, SWT.NONE);
		tableCursor.setForeground(COLOR_BLACK);
		tableCursor.setBackground(COLOR_BACKGROUND);

		tableCursor.addMouseListener(new TableCursorMouseClickListener(tableViewer));
		tableCursor.addKeyListener(keyStateMaskAdapter);
		tableCursor.addKeyListener(new SpaceKeyOnTableCursorToEditListener(tableViewer));
		tableCursor.addSelectionListener(new TableCursorSelectionListener(tableViewer));

		tableViewer.setData(TextCellEditorUpDownKeyListener.TABLE_CURSOR_KEY, tableCursor);
		table.setData(TextCellEditorUpDownKeyListener.TABLE_CURSOR_KEY, tableCursor);
		table.addMouseListener(new PopControlContextMenuListener());
		tableViewer.addSelectionChangedListener(new AutoLocateTableCursorForTableViewerSelectionChangedListener());

		CellEditor[] cellEditors = tableViewer.getCellEditors();
		if (cellEditors == null) {
			return;
		}
		for (int i = 0; i < cellEditors.length; i++) {
			CellEditor ce = cellEditors[i];
			if (ce instanceof TextCellEditor) {
				ce.getControl().addKeyListener(new TextCellEditorUpDownKeyListener(tableViewer));
				ce.getControl().addFocusListener(new TextCellEditorForTableCursorFocusListener(i));
			}
		}
	}

	/**
	 * 
	 * Build a table viewer using the input options.
	 * 
	 * @param parent Composite
	 * @param style Table's style
	 * @return TableViewer
	 */
	public TableViewer buildTableViewer(Composite parent, int style) {
		//Each table viewer has a different keyStateMaskAdapter
		keyStateMaskAdapter = new KeyStateMaskAdapter();
		TableViewer tv = new TableViewer(parent, style);
		tv.setContentProvider(contentProvider);
		Table table = tv.getTable();
		table.setLinesVisible(true);
		table.setHeaderVisible(true);
		table.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		setTableViewerColumnProperties(tv);
		setTableViewerEditors(tv);
		setTableViewerLabelProvider(tv);
		addTableCursorForTableViewer(tv);
		addCheckStyleTableListener(tv);
		return tv;
	}

	/**
	 * If the table has CHECK style, then add a selection listener for select
	 * all or clear all.
	 * 
	 * @param tv TableViewer
	 */
	private void addCheckStyleTableListener(TableViewer tv) {
		int style = tv.getTable().getStyle();
		if (isCheckStyle(style)) {
			CheckStyleTableSelectionListener.addListenerToCheckStyleTable(tv.getTable(), false);
		}
	}

	/**
	 * if the table has check style
	 * 
	 * @param style of table
	 * @return true if has check style
	 */
	private boolean isCheckStyle(int style) {
		return (style & SWT.CHECK) != 0;
	}

	/**
	 * @param tv
	 */
	private void setTableViewerColumnProperties(TableViewer tv) {
		for (int i = 0; i < columnNames.length; i++) {
			String colName = columnNames[i];

			int cstyle = columnStyles == null ? SWT.NONE : columnStyles[i];
			final TableViewerColumn tvColumn = new TableViewerColumn(tv, cstyle);
			TableColumn column = tvColumn.getColumn();
			column.setText(colName);

			Image image = columnImageFiles == null ? null
					: MigrationUIPlugin.getImage(columnImageFiles[i]);
			column.setImage(image);

			int width = columnWidths == null ? 50 : columnWidths[i];
			column.setWidth(width);

			String tooltip = tooltips == null ? colName : tooltips[i];
			column.setToolTipText(tooltip);
		}
	}

	/**
	 * @param tv
	 * @param table
	 */
	private void setTableViewerEditors(TableViewer tv) {
		if (cellEditorFactorys == null) {
			return;
		}
		Table table = tv.getTable();
		CellEditor[] cellEditors = new CellEditor[cellEditorFactorys.length];
		String[] props = new String[cellEditors.length];

		boolean isNeedToSaveLastSelection = false;
		for (int i = 0; i < cellEditorFactorys.length; i++) {
			if (cellEditorFactorys[i] == null) {
				cellEditors[i] = null;
			} else {
				cellEditors[i] = cellEditorFactorys[i].getCellEditor(table);
			}
			props[i] = String.valueOf(i);
			if (cellEditors[i] instanceof CheckboxCellEditor) {
				isNeedToSaveLastSelection = true;
			}
		}
		tv.setCellEditors(cellEditors);
		tv.setColumnProperties(props);

		tv.getTable().addKeyListener(keyStateMaskAdapter);
		if (cellModifier == null && rowIsDataArray) {
			cellModifier = new ObjectArrayRowCellModifier();
		}
		if (cellModifier instanceof ObjectArrayRowCellModifier) {
			((ObjectArrayRowCellModifier) cellModifier).setStateMaskListener(keyStateMaskAdapter);
			((ObjectArrayRowCellModifier) cellModifier).setCellEditors(cellEditors);
			((ObjectArrayRowCellModifier) cellModifier).setVerifiers(cellEditorValidators);
		}
		tv.setCellModifier(cellModifier);
		//The modify is happened before the event selection changed 
		//So save last selected table item to support batch update with shift key pressed
		if (isNeedToSaveLastSelection) {
			tv.addSelectionChangedListener(new SaveLastSelectionListener());
		}
	}

	/**
	 * @param tv
	 * @param cellEditors
	 */
	private void setTableViewerLabelProvider(TableViewer tv) {
		if (labelProvider == null && rowIsDataArray) {
			labelProvider = new ObjectArrayRowTableLabelProvider();
		}
		if (labelProvider instanceof ObjectArrayRowTableLabelProvider) {
			((ObjectArrayRowTableLabelProvider) labelProvider).setTableViewer(tv);
		}
		tv.setLabelProvider(labelProvider);
	}

	public void setCellEditorClasses(CellEditorFactory[] cellEditorFactorys) {
		this.cellEditorFactorys = cellEditorFactorys;
	}

	public void setCellModifier(ICellModifier cellModifier) {
		this.cellModifier = cellModifier;

	}

	public void setCellValidators(ICellEditorValidator[] cellEditorValidators) {
		this.cellEditorValidators = cellEditorValidators;

	}

	public void setColumnImages(String[] columnImageFiles) {
		this.columnImageFiles = columnImageFiles;
	}

	public void setColumnNames(String[] columnNames) {
		this.columnNames = columnNames;
	}

	public void setColumnStyles(int[] columnStyles) {
		this.columnStyles = columnStyles;
	}

	public void setColumnTooltips(String[] tooltips) {
		this.tooltips = tooltips;
	}

	public void setColumnWidths(int[] columnWidths) {
		this.columnWidths = columnWidths;
	}

	public void setContentProvider(IStructuredContentProvider contentProvider) {
		this.contentProvider = contentProvider;
	}

	public void setLabelProvider(IBaseLabelProvider labelProvider) {
		this.labelProvider = labelProvider;
	}

	public void setTableCursorSupported(boolean tableCursorSupported) {
		this.tableCursorSupported = tableCursorSupported;
	}

	public void setTableViewerCellModifier(ICellModifier cm) {
		this.cellModifier = cm;
	}

	public void setRowIsDataArray(boolean rowIsDataArray) {
		this.rowIsDataArray = rowIsDataArray;
	}

	//	/**
	//	 * @param event
	//	 * @return
	//	 */
	//	private boolean isMultiModeKeyDown(KeyEvent event) {
	//		return event.keyCode == SWT.MOD1 || event.keyCode == SWT.MOD2
	//				|| (event.stateMask & SWT.MOD1) != 0 || (event.stateMask & SWT.MOD2) != 0;
	//	}
	//	/**
	//	 * if the table has check style
	//	 * 
	//	 * @param style of table
	//	 * @return true if has check style
	//	 */
	//	private boolean isMultiStyle(int style) {
	//		return (style & SWT.MULTI) != 0;
	//	}
}