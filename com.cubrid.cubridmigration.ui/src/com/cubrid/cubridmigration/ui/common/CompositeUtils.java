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
package com.cubrid.cubridmigration.ui.common;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.TableCursor;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.common.ui.swt.table.listener.CheckBoxColumnSelectionListener;
import com.cubrid.common.ui.swt.table.listener.TextCellEditorUpDownKeyListener;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * CompositeUtils provide several methods to create or modify composites
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-4-18 created by Kevin Cao
 */
public final class CompositeUtils {

	private static final Logger LOG = LogUtil.getLogger(CompositeUtils.class);

	public static final String LAST_SELECTION = "lastSelection";
	public static final Image CHECK_IMAGE = MigrationUIPlugin.getImage("icon/checked.gif");
	public static final Image UNCHECK_IMAGE = MigrationUIPlugin.getImage("icon/unchecked.gif");

	/**
	 * Make a shell in the monitor center
	 * 
	 * @param dlg shell
	 */
	public static void centerDialog(Shell dlg) {
		Rectangle bounds = Display.getDefault().getPrimaryMonitor().getBounds();
		Rectangle rect = dlg.getBounds();
		int x = bounds.x + (bounds.width - rect.width) / 2;
		int y = bounds.y + (bounds.height - rect.height) / 2;
		dlg.setLocation(x, y);
	}

	/**
	 * Create tab item
	 * 
	 * @param parent CTabFolder
	 * @param name String
	 * @param image String
	 * @return control of the tab item
	 */
	public static Composite createTabItem(CTabFolder parent, String name, String image) {
		final CTabItem tabItem = new CTabItem(parent, SWT.NONE);
		tabItem.setText(name);
		if (StringUtils.isNotBlank(image)) {
			tabItem.setImage(MigrationUIPlugin.getImage(image));
		}
		Composite container = new Composite(parent, SWT.BORDER);
		container.setLayout(new GridLayout());
		container.setLayoutData(new GridData(SWT.FILL));

		tabItem.setControl(container);

		return container;
	}

	/**
	 * Retrieves the image by check status
	 * 
	 * @param checked status
	 * @return CHECK_IMAGE if checked is true
	 */
	public static Image getCheckImage(boolean checked) {
		return checked ? CHECK_IMAGE : UNCHECK_IMAGE;
	}

	/**
	 * Get table cursor of the table .
	 * 
	 * @param table Table
	 * @return TableCursor
	 */
	public static TableCursor getTableCursor(Table table) {
		return (TableCursor) table.getData(TextCellEditorUpDownKeyListener.TABLE_CURSOR_KEY);
	}

	/**
	 * Get table cursor of the table viewer.
	 * 
	 * @param tableViewer TableViewer
	 * @return TableCursor
	 */
	public static TableCursor getTableCursor(TableViewer tableViewer) {
		return (TableCursor) tableViewer.getData(TextCellEditorUpDownKeyListener.TABLE_CURSOR_KEY);
	}

	/**
	 * 
	 * @param table Table component
	 * @return Retrieves the last selection which was saved in table's data
	 *         property
	 */
	public static TableItem getTableLastSelection(Table table) {
		return (TableItem) table.getData(LAST_SELECTION);
	}

	/**
	 * Hide or bring the composite onto the screen top.
	 * 
	 * @param com to be controlled
	 * @param hide true if the composite is need to be hide.
	 */
	public static void hideOrShowComposite(Composite com, boolean hide) {
		if (com == null) {
			return;
		}
		final GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
		gd.exclude = hide;
		com.setLayoutData(gd);
		com.setVisible(!hide);
		if (!hide) {
			com.getParent().layout(true);
		}
	}

	/**
	 * 
	 * Initialize the image of column which has check box cell editor
	 * 
	 * @param tv TableViewer
	 */
	public static void initTableViewerCheckColumnImage(TableViewer tv) {
		for (int i = 0; i < tv.getTable().getColumnCount(); i++) {
			TableColumn tc = tv.getTable().getColumn(i);
			boolean flag = false;
			boolean isBoolean = false;
			for (TableItem ti : tv.getTable().getItems()) {
				Object[] obj = (Object[]) ti.getData();
				if (!(obj[i] instanceof Boolean)) {
					break;
				}
				isBoolean = true;
				if ((Boolean) obj[i]) {
					flag = true;
					break;
				}
			}
			if (!isBoolean) {
				continue;
			}
			tc.setImage(getCheckImage(flag));
		}
	}

	/**
	 * Running in progress bar.
	 * 
	 * @param fork boolean
	 * @param cancelable boolean
	 * @param mtd IRunnableWithProgress
	 */
	public static void runMethodInProgressBar(final boolean fork, final boolean cancelable,
			final IRunnableWithProgress mtd) {
		Display display = Display.getDefault();
		display.syncExec(new Runnable() {
			public void run() {
				try {
					new ProgressMonitorDialog(null).run(fork, cancelable, mtd);
				} catch (Exception ex) {
					LOG.error("", ex);
				}
			}
		});
	}

	/**
	 * Set column selection listener
	 * 
	 * @param tv TableViewer
	 * @param iSelectionListeners SelectionListener[] null if column doesn't
	 *        need listener
	 */
	public static void setTableColumnSelectionListener(TableViewer tv,
			SelectionListener[] iSelectionListeners) {
		for (int i = 0; i < tv.getTable().getColumnCount(); i++) {
			TableColumn tc = tv.getTable().getColumn(i);
			if (iSelectionListeners[i] == null) {
				continue;
			}
			tc.addSelectionListener(iSelectionListeners[i]);
			if (iSelectionListeners[i] instanceof CheckBoxColumnSelectionListener) {
				tc.setImage(getCheckImage(true));
			}
		}
	}

	/**
	 * Update table's selection into his data property
	 * 
	 * @param table to be save selection
	 */
	public static void setTableLastSelection(Table table) {
		if (table.getSelectionCount() == 0) {
			table.setData(LAST_SELECTION, null);
			return;
		}
		table.setData(LAST_SELECTION, table.getSelection()[0]);
	}

	/**
	 * Retrieves if a viewer(tree viewer or table viewer) available
	 * 
	 * @param viewer Viewer
	 * @return true if available
	 */
	public static boolean isViewerAvailable(Viewer viewer) {
		return viewer != null && viewer.getControl() != null && !viewer.getControl().isDisposed();
	}

	/**
	 * Apply current editor's value to table viewer's items.
	 * 
	 * @param tv TableViewer
	 */
	public static void applyTableViewerEditing(TableViewer tv) {
		if (!tv.isCellEditorActive()) {
			return;
		}
		CellEditor[] cellEditors = tv.getCellEditors();
		Object[] columnProperties = tv.getColumnProperties();
		for (int i = 0; i < cellEditors.length; i++) {
			CellEditor ce = cellEditors[i];
			if (ce == null || !ce.isActivated()) {
				continue;
			}
			if (i >= columnProperties.length) {
				return;
			}
			ICellModifier cellModifier = tv.getCellModifier();
			if (cellModifier == null) {
				return;
			}
			cellModifier.modify(tv.getTable().getSelection()[0], (String) columnProperties[i],
					ce.getValue());
		}
	}

	private CompositeUtils() {
		//Do nothing here
	}
}
