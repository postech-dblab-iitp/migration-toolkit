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
package com.cubrid.cubridmigration.ui.wizard.page.view;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.eclipse.jface.viewers.ICellEditorValidator;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.osgi.util.NLS;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.ObjectArrayRowCellModifier;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.TextCellEditorFactory;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.navigator.node.SQLTableNode;
import com.cubrid.cubridmigration.ui.common.tableviewer.cell.validator.CUBRIDDataTypeValidator;
import com.cubrid.cubridmigration.ui.common.tableviewer.cell.validator.CUBRIDNameValidator;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * SQLTableMappingView response to show SQL table configuration
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-25 created by Kevin Cao
 */
public class SQLTableMappingView extends
		AbstractMappingView {
	private final CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);

	/**
	 * Do something when the target text was changed and lost focus
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-9-19 created by Kevin Cao
	 */
	private class TargetFocusAdapter extends
			FocusAdapter {

		/**
		 * Focus lost
		 * 
		 * @param ex FocusEvent
		 */
		public void focusLost(FocusEvent ex) {
			final String newName = txtTarget.getText().trim();
			if (newName.equalsIgnoreCase(sstc.getTarget())) {
				return;
			}
			Table tt = config.getTargetTableSchema(newName);
			if (tt == null) {
				return;
			}
			sstc.setTarget(newName);
			tvTableColumns.setInput(sstc.getColumnConfigList());
		}

	}

	/**
	 * 
	 * TableColumnContentProvider of table view
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-9-19 created by Kevin Cao
	 */
	private class TableColumnContentProvider extends
			StructuredContentProviderAdaptor {

		/**
		 * Retrieves the elements used by table viewer
		 * 
		 * @param inputElement List<SourceColumnConfig>
		 * @return Object[]
		 */
		@SuppressWarnings("unchecked")
		public Object[] getElements(Object inputElement) {
			List<Object[]> result = new ArrayList<Object[]>();
			Table srcTable = config.getSrcSQLSchema(sstc.getName());
			Table tt = config.getTargetTableSchema(sstc.getTarget());
			if (srcTable == null || tt == null) {
				return super.getElements(result);
			}
			for (SourceColumnConfig scc : (List<SourceColumnConfig>) inputElement) {
				Column scol = srcTable.getColumnByName(scc.getName());
				Column tcol = tt.getColumnByName(scc.getTarget());
				if (scol == null) {
					continue;
				}
				//Auto mapping column
				if (tcol == null) {
					tcol = config.getDBTransformHelper().getCUBRIDColumn(scol, config);
				}
				result.add(new Object[] {scol.getName(), scol.getShownDataType(), scc.getTarget(),
						tcol.getShownDataType(), scc});
			}
			return super.getElements(result);
		}
	}

	private Composite container;
	private Text txtSource;
	private Text txtTarget;
	private TableViewer tvTableColumns;
	private SourceSQLTableConfig sstc;

	private Button btnCreate;
	private Button btnReplace;
	private Button btnMigrateData;

	public SQLTableMappingView(Composite parent) {
		super(parent);
	}

	/**
	 * Create controls
	 * 
	 * @param parent of the controls
	 */
	protected void createControl(Composite parent) {
		container = new Composite(parent, SWT.BORDER);
		container.setVisible(false);
		container.setLayout(new GridLayout());
		final GridData gdTabFolder = new GridData(SWT.FILL, SWT.FILL, true, true);
		gdTabFolder.exclude = true;
		container.setLayoutData(gdTabFolder);

		createDetailOfColumns(container);
	}

	/**
	 * Create columns controls
	 * 
	 * @param parent of the controls
	 */
	private void createDetailOfColumns(Composite parent) {
		Composite comCheck = new Composite(parent, SWT.NONE);
		comCheck.setLayout(new GridLayout(3, false));
		comCheck.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));
		btnCreate = new Button(comCheck, SWT.CHECK);
		btnCreate.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnCreate.setText(Messages.lblCreateNewTable);
		btnCreate.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				setPageControlStatus();
			}
		});

		btnReplace = new Button(comCheck, SWT.CHECK);
		btnReplace.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnReplace.setText(Messages.lblReplaceTable);

		btnMigrateData = new Button(comCheck, SWT.CHECK);
		btnMigrateData.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnMigrateData.setText(Messages.lblMigrateData);
		btnMigrateData.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				setPageControlStatus();
			}
		});

		Composite nameContainer = new Composite(parent, SWT.NONE);
		nameContainer.setLayout(new GridLayout(4, false));
		nameContainer.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));

		Label lblSource = new Label(nameContainer, SWT.NONE);
		lblSource.setText(Messages.lblSourceTalbeName);

		txtSource = new Text(nameContainer, SWT.BORDER);
		txtSource.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtSource.setEditable(false);

		Label lblTarget = new Label(nameContainer, SWT.NONE);
		lblTarget.setText(Messages.lblTargetTableName);

		txtTarget = new Text(nameContainer, SWT.BORDER);
		txtTarget.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtTarget.addFocusListener(new TargetFocusAdapter());

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.tabTitleSourceColumn,
				Messages.tabTitleDataType, Messages.tabTitleTargetColumn, Messages.tabTitleDataType});
		tvBuilder.setColumnWidths(new int[] {150, 150, 150, 150});
		tvBuilder.setCellEditorClasses(new CellEditorFactory[] {null, null,
				new TextCellEditorFactory(), new TextCellEditorFactory()});
		tvBuilder.setCellValidators(new ICellEditorValidator[] {null, null,
				new CUBRIDNameValidator(), new CUBRIDDataTypeValidator()});
		tvBuilder.setContentProvider(new TableColumnContentProvider());
		tvBuilder.setCellModifier(new ObjectArrayRowCellModifier());
		tvBuilder.setTableCursorSupported(true);
		tvTableColumns = tvBuilder.buildTableViewer(parent, SWT.BORDER | SWT.FULL_SELECTION);
	}

	/**
	 * Set page controls' status
	 * 
	 */
	private void setPageControlStatus() {
		btnReplace.setEnabled(btnCreate.getSelection());
		btnReplace.setSelection(btnCreate.getSelection());

		final boolean status = btnCreate.getSelection() || btnMigrateData.getSelection();
		txtTarget.setEditable(status);
		tvTableColumns.getTable().setEnabled(status);
	}

	/**
	 * Hide this view
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(container, true);
	}

	/**
	 * Bring this view onto top
	 */
	public void show() {
		CompositeUtils.hideOrShowComposite(container, false);
	}

	/**
	 * Show data SourceSQLTableConfig
	 * 
	 * @param obj SourceSQLTableConfig
	 */
	public void showData(Object obj) {
		super.showData(obj);
		if (!(obj instanceof SQLTableNode)) {
			throw new IllegalArgumentException("Can't show the data in this view.");
		}
		SQLTableNode node = (SQLTableNode) obj;
		sstc = node.getSstc();
		txtSource.setText(sstc.getName());
		txtTarget.setText(sstc.getTarget());

		Table srcTable = config.getSrcSQLSchema(sstc.getName());
		final boolean invalidSQL = (srcTable == null);
		btnCreate.setEnabled(!invalidSQL);
		btnReplace.setEnabled(!invalidSQL);
		btnMigrateData.setEnabled(!invalidSQL);
		txtTarget.setEditable(!invalidSQL);
		tvTableColumns.getTable().setEnabled(!invalidSQL);
		if (invalidSQL) {
			tvTableColumns.setInput(new ArrayList<SourceColumnConfig>());
			throw new RuntimeException("Can not get SQL schema. Please check the SQL.");
		}
		tvTableColumns.setInput(sstc.getColumnConfigList());

		btnCreate.setSelection(sstc.isCreateNewTable());
		btnReplace.setEnabled(btnCreate.getSelection());
		btnReplace.setSelection(btnCreate.getSelection() && sstc.isReplace());
		btnMigrateData.setSelection(sstc.isMigrateData());
		txtTarget.setEditable(sstc.isCreateNewTable() || sstc.isMigrateData());
		tvTableColumns.getTable().setEnabled(sstc.isCreateNewTable() || sstc.isMigrateData());

		CompositeUtils.initTableViewerCheckColumnImage(tvTableColumns);
	}

	/**
	 * Save UI
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		if (sstc == null) {
			return super.save();
		}

		//If don't create and migrate data, do not save.
		if (!btnCreate.getSelection() && !btnMigrateData.getSelection()) {
			sstc.setCreateNewTable(btnCreate.getSelection());
			sstc.setReplace(btnReplace.getSelection());
			sstc.setMigrateData(btnMigrateData.getSelection());
			return super.save();
		}
		VerifyResultMessages result = validate();
		if (result.hasError()) {
			return result;
		}
		sstc.setCreateNewTable(btnCreate.getSelection());
		sstc.setReplace(btnReplace.getSelection());
		sstc.setMigrateData(btnMigrateData.getSelection());

		String newTableName = txtTarget.getText().trim().toLowerCase(Locale.US);
		config.changeTarget(sstc, newTableName);
		Table tt = config.getTargetTableSchema(sstc.getTarget());
		if (tt == null) {
			return super.save();
		}
		Table srcTable = config.getSrcSQLSchema(sstc.getName());
		for (int i = 0; i < tvTableColumns.getTable().getItemCount(); i++) {
			Object[] obj = (Object[]) tvTableColumns.getElementAt(i);
			SourceColumnConfig scc = (SourceColumnConfig) obj[obj.length - 1];
			scc.setCreate(true);
			scc.setReplace(true);
			config.changeTarget(scc, (String) obj[2]);
			Column col = tt.getColumnByName(scc.getTarget());
			if (col == null) {
				Column scol = srcTable.getColumnByName(scc.getName());
				if (scol == null) {
					continue;
				}
				col = config.getDBTransformHelper().getCUBRIDColumn(scol, config);
				col.setName(scc.getTarget());
				tt.addColumn(col);
			}
			dataTypeHelper.setColumnDataType((String) obj[3], col);
		}
		return super.save();
	}

	/**
	 * Validation
	 * 
	 * @return VerifyResultMessages
	 */
	private VerifyResultMessages validate() {
		String newName = txtTarget.getText().trim().toLowerCase(Locale.US);
		if (!MigrationCfgUtils.verifyTargetDBObjName(newName)) {
			return new VerifyResultMessages(
					Messages.bind(Messages.msgErrInvalidTableName, newName), null, null);
		}
		List<String> names = new ArrayList<String>();
		for (int i = 0; i < tvTableColumns.getTable().getItemCount(); i++) {
			Object[] obj = (Object[]) tvTableColumns.getElementAt(i);
			String name = obj[2].toString().toLowerCase(Locale.US);
			if (!MigrationCfgUtils.verifyTargetDBObjName(name)) {
				return new VerifyResultMessages(NLS.bind(Messages.msgErrInvalidColumnName, name,
						sstc.getTarget()), null, null);
			}
			if (names.indexOf(name) >= 0) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrDupColumnName,
						sstc.getTarget(), name), null, null);
			}
			names.add(name);
			String dataType = obj[3].toString().toLowerCase(Locale.US);
			if (!dataTypeHelper.isValidDatatype(dataType)) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidDataType,
						sstc.getTarget(), name), null, null);
			}
		}
		return new VerifyResultMessages();
	}
}
