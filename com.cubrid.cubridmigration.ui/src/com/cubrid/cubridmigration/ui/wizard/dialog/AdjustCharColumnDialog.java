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
package com.cubrid.cubridmigration.ui.wizard.dialog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.listener.CheckStyleTableSelectionListener;
import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;

/**
 * 
 * A dialog which can make changing the length of the columns which data type
 * are char more conveniently.
 * 
 * @author cn13425
 * @version 1.0 - 2012-04-18 created by cn13425
 */
public class AdjustCharColumnDialog extends
		Dialog {

	private static final int[] COLUMN_STYLES = new int[] {SWT.CENTER, SWT.LEFT, SWT.LEFT, SWT.LEFT,
			SWT.LEFT, SWT.LEFT, SWT.LEFT, SWT.LEFT};
	private final MigrationCfgUtils util;
	private TableViewer tvCharColumns;
	private TableViewer tvVarCharColumns;
	private Spinner spFactor;
	private TabFolder tabFolder;

	public AdjustCharColumnDialog(Shell parentShell, MigrationCfgUtils util) {
		super(parentShell);
		this.util = util;
	}

	/**
	 * isHelpAvailable
	 * 
	 * @return false
	 */
	public boolean isHelpAvailable() {
		return false;
	}

	/**
	 * getColumnWidth
	 * 
	 * @return int[]
	 */
	private int[] getColumnWidth() {
		int[] colWidths = new int[] {30, 0, 105, 105, 115, 105, 105, 105};
		if (util.getMigrationConfiguration().getSourceDBType().isSupportMultiSchema()) {
			colWidths[1] = 100;
		}
		return colWidths;
	}

	/**
	 * Retrieves current active table viewer
	 * 
	 * @return TableViewer
	 */
	private TableViewer getCurrentTableViewer() {
		if (tabFolder.getSelectionIndex() == 0) {
			return tvCharColumns;
		}
		return tvVarCharColumns;
	}

	/**
	 * createDialogArea
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		final Composite composite = new Composite(parent, SWT.NONE);
		composite.setLayout(new GridLayout());
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));

		tabFolder = new TabFolder(composite, SWT.NONE);
		tabFolder.setLayout(new GridLayout());
		tabFolder.setLayoutData(new GridData(GridData.FILL_BOTH));

		createTable1(tabFolder);
		createTable2(tabFolder);
		createButtons(composite);

		fillTable();
		fillTable2();
		tabFolder.setSelection(0);
		tabFolder.layout();
		return parent;
	}

	/**
	 * Fill the data into table view
	 * 
	 */
	private void fillTable() {
		List<Column[]> tableContent = new ArrayList<Column[]>();
		final MigrationConfiguration cfg = util.getMigrationConfiguration();
		List<SourceTableConfig> columnMap = new ArrayList<SourceTableConfig>(
				cfg.getExpEntryTableCfg());
		columnMap.addAll(cfg.getExpSQLCfg());
		List<String> names = new ArrayList<String>();
		for (SourceTableConfig setc : columnMap) {
			if (!setc.isCreateNewTable() && !setc.isMigrateData()) {
				continue;
			}
			if (names.indexOf(setc.getTarget()) >= 0) {
				continue;
			}
			names.add(setc.getTarget());
			Table stable = cfg.getSrcTableSchema(setc.getOwner(), setc.getName());
			Table ttable = cfg.getTargetTableSchema(setc.getTarget());
			for (SourceColumnConfig scc : setc.getColumnConfigList()) {
				if (!scc.isCreate()) {
					continue;
				}
				Column scol = stable.getColumnByName(scc.getName());
				Column tcol = ttable.getColumnByName(scc.getTarget());
				if (scol == null || tcol == null) {
					continue;
				}
				DBDataTypeHelper srcDTHelper = cfg.getSourceDBType().getDataTypeHelper(null);
				DBDataTypeHelper tarDTHelper = DatabaseType.CUBRID.getDataTypeHelper(null);
				if (srcDTHelper.isGenericString(scol.getDataType())
						&& tarDTHelper.isChar(tcol.getDataType())) {
					Column[] row = new Column[] {null, scol, tcol};
					tableContent.add(row);
				}
			}
		}
		Collections.sort(tableContent, new Comparator<Column[]>() {

			public int compare(Column[] o1, Column[] o2) {
				final int first = o1[1].getTableOrView().getName().compareTo(
						o2[1].getTableOrView().getName());
				if (first == 0) {
					return o1[2].getName().compareTo(o2[2].getName());
				}
				return first;
			}

		});
		tvCharColumns.setInput(tableContent);
		refreshSelectAllStatus(tvCharColumns, true);

	}

	/**
	 * Fill the data into table view
	 * 
	 */
	private void fillTable2() {
		List<Column[]> tableContent = new ArrayList<Column[]>();
		final MigrationConfiguration cfg = util.getMigrationConfiguration();
		List<SourceTableConfig> columnMap = new ArrayList<SourceTableConfig>(
				cfg.getExpEntryTableCfg());
		columnMap.addAll(cfg.getExpSQLCfg());
		List<String> names = new ArrayList<String>();
		for (SourceTableConfig setc : columnMap) {
			if (!setc.isCreateNewTable() && !setc.isMigrateData()) {
				continue;
			}
			if (names.indexOf(setc.getTarget()) >= 0) {
				continue;
			}
			names.add(setc.getTarget());
			Table stable = cfg.getSrcTableSchema(setc.getOwner(), setc.getName());
			Table ttable = cfg.getTargetTableSchema(setc.getTarget());
			for (SourceColumnConfig scc : setc.getColumnConfigList()) {
				if (!scc.isCreate()) {
					continue;
				}
				Column scol = stable.getColumnByName(scc.getName());
				Column tcol = ttable.getColumnByName(scc.getTarget());
				if (scol == null || tcol == null) {
					continue;
				}
				DBDataTypeHelper srcDTHelper = util.getMigrationConfiguration().getSourceDBType().getDataTypeHelper(
						null);
				DBDataTypeHelper tarDTHelper = DatabaseType.CUBRID.getDataTypeHelper(null);
				if (srcDTHelper.isString(scol.getDataType())
						&& tarDTHelper.isVarchar(tcol.getDataType())) {
					Column[] row = new Column[] {null, scol, tcol};
					tableContent.add(row);
				}
			}
		}
		Collections.sort(tableContent, new Comparator<Column[]>() {

			public int compare(Column[] o1, Column[] o2) {
				final int first = o1[1].getTableOrView().getName().compareTo(
						o2[1].getTableOrView().getName());
				if (first == 0) {
					return o1[2].getName().compareTo(o2[2].getName());
				}
				return first;
			}

		});
		tvVarCharColumns.setInput(tableContent);
		refreshSelectAllStatus(tvVarCharColumns, true);
	}

	/**
	 * Create control buttons
	 * 
	 * @param composite of the buttons
	 */
	private void createButtons(Composite composite) {
		final Composite comButtons = new Composite(composite, SWT.RIGHT);
		comButtons.setLayout(new GridLayout(3, false));
		comButtons.setLayoutData(new GridData(SWT.FILL, SWT.BOTTOM, true, false));

		Button btnDefault = new Button(comButtons, SWT.NONE);
		btnDefault.setText(Messages.btnDefaultColumnLength);
		btnDefault.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				boolean flag = false;

				TableViewer tv = getCurrentTableViewer();
				for (TableItem ti : tv.getTable().getItems()) {
					if (!ti.getChecked()) {
						continue;
					}
					flag = true;
					Column[] data = (Column[]) ti.getData();
					util.setCharTypeColumnToDefaultMapping(data[1], data[2]);
				}
				tv.refresh();

				if (flag) {
					tvVarCharColumns.refresh();
				} else {
					MessageDialog.openError(getShell(), Messages.msgError, Messages.msgErrNoColumn);
				}
			}
		});

		Button btnMultiply = new Button(comButtons, SWT.NONE);
		btnMultiply.setText(Messages.btnMultiply);
		btnMultiply.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				boolean flag = false;
				TableViewer tv = getCurrentTableViewer();
				for (TableItem ti : tv.getTable().getItems()) {
					if (!ti.getChecked()) {
						continue;
					}
					flag = true;
					Column[] data = (Column[]) ti.getData();
					util.multiplyCharColumn(data[1], data[2], spFactor.getSelection());
				}
				tv.refresh();

				if (flag) {
					tvVarCharColumns.refresh();
				} else {
					MessageDialog.openError(getShell(), Messages.msgError, Messages.msgErrNoColumn);
				}
			}
		});

		spFactor = new Spinner(comButtons, SWT.BORDER);
		spFactor.setSize(50, 26);
		spFactor.setValues(util.getMigrationConfiguration().getCharsetFactor(), 1, 10, 0, 1, 1);

	}

	/**
	 * Create the char table view
	 * 
	 * @param parent of the table view
	 */
	private void createTable1(TabFolder parent) {
		TabItem ti = new TabItem(parent, SWT.NONE);
		ti.setText(Messages.tabCharColumns);
		final Composite composite = new Composite(parent, SWT.NONE);
		composite.setLayout(new GridLayout());
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));
		ti.setControl(composite);

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(getTableHeaders());
		tvBuilder.setColumnStyles(COLUMN_STYLES);
		tvBuilder.setColumnWidths(getColumnWidth());
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new MyLabelProvider());
		tvCharColumns = tvBuilder.buildTableViewer(composite, SWT.BORDER | SWT.FULL_SELECTION
				| SWT.CHECK);
	}

	/**
	 * Create the varchar table view
	 * 
	 * @param parent of the table view
	 */
	private void createTable2(TabFolder parent) {
		TabItem ti = new TabItem(parent, SWT.NONE);
		ti.setText(Messages.tabVarcharColumns);
		final Composite composite = new Composite(parent, SWT.NONE);
		composite.setLayout(new GridLayout());
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));
		ti.setControl(composite);

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(getTableHeaders());
		tvBuilder.setColumnStyles(COLUMN_STYLES);
		tvBuilder.setColumnWidths(getColumnWidth());
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new MyLabelProvider());
		tvVarCharColumns = tvBuilder.buildTableViewer(composite, SWT.BORDER | SWT.FULL_SELECTION
				| SWT.CHECK);
	}

	/**
	 * Retrieves the table headers string array.
	 * 
	 * @return string array
	 */
	private String[] getTableHeaders() {
		return new String[] {"", "Schema", Messages.tblColSourceTable, Messages.tblColSourceColumn,
				Messages.tblColType, Messages.tblColTargetTable, Messages.tblColTargetColumn,
				Messages.tblColType};
	}

	/**
	 * constrainShellSize
	 */

	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setSize(820, 540);
		getShell().setText(Messages.titleSettingCharTypeColumns);
	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, false);
		getButton(IDialogConstants.OK_ID).setEnabled(true);
	}

	/**
	 * @return shell style
	 */
	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE | SWT.MAX;
	}

	//	/**
	//	 * buttonPressed
	//	 * 
	//	 * @param buttonId int
	//	 */
	//	protected void buttonPressed(int buttonId) {
	//		if (buttonId == IDialogConstants.OK_ID) {
	//		} else {
	//			super.buttonPressed(buttonId);
	//		}
	//
	//	}

	/**
	 * 
	 * MyLabelProvider
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-04-18
	 */
	private static class MyLabelProvider extends
			LabelProvider implements
			ITableLabelProvider {

		/**
		 * Return null
		 * 
		 * @param element the object to be shown
		 * @param columnIndex the column index
		 * @return column image
		 */
		public Image getColumnImage(Object element, int columnIndex) {
			return null;
		}

		/**
		 * @param element the object to be shown
		 * @param columnIndex the column index
		 * @return column text
		 */
		public String getColumnText(Object element, int columnIndex) {
			Column[] cols = (Column[]) element;
			switch (columnIndex) {
			case 0:
				return "";
			case 1:
				return cols[1].getTableOrView().getOwner() == null ? ""
						: cols[1].getTableOrView().getOwner();
			case 2:
				return cols[1].getTableOrView().getName();
			case 3:
				return cols[1].getName();
			case 4:
				return cols[1].getShownDataType();
			case 5:
				return cols[2].getTableOrView().getName();
			case 6:
				return cols[2].getName();
			case 7:
				return cols[2].getShownDataType();
			default:
				return "";
			}
		}
	}

	/**
	 * Set the button's image by status of selectAll variable.
	 * 
	 * @param tv table view
	 * @param selectAll true to select all
	 */
	private void refreshSelectAllStatus(TableViewer tv, boolean selectAll) {
		CheckStyleTableSelectionListener.refreshSelectAllStatus(tv.getTable(), selectAll);
	}
}
