/*
 * Copyright (C) 2012 Search Solution Corporation. All rights reserved by Search Solution. 
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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.jface.viewers.ICellEditorValidator;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.TableCursor;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.ObjectArrayRowCellModifier;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.CheckboxCellEditorFactory;
import com.cubrid.common.ui.swt.table.celleditor.TextCellEditorFactory;
import com.cubrid.common.ui.swt.table.listener.CheckBoxColumnSelectionListener;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceViewConfig;
import com.cubrid.cubridmigration.core.engine.listener.ISQLTableChangedListener;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.navigator.node.DatabaseNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SQLTablesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SchemaNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SequencesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TablesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ViewsNode;
import com.cubrid.cubridmigration.ui.common.tableviewer.cell.validator.CUBRIDNameValidator;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.IMigrationWizardStatus;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * GeneralObjMappingView is response to change source database's objects
 * exporting settings. Including tables,views and serials.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-25 created by Kevin Cao
 */
public class GeneralObjMappingView extends
		AbstractMappingView {
	private static final int SOURCE_NAME_INDEX = 0;
	private static final int TARGET_NAME_INDEX = SOURCE_NAME_INDEX + 1;
	private static final int DATA_COLUMN_INDEX = TARGET_NAME_INDEX + 1;
	private static final int CONDITION_COLUMN_INDEX = DATA_COLUMN_INDEX + 1;
	private static final int CREATE_COLUMN_INDEX = CONDITION_COLUMN_INDEX + 1;
	private static final int REPLACE_COLUMN_INDEX = CREATE_COLUMN_INDEX + 1;
	private static final int PK_COLUMN_INDEX = REPLACE_COLUMN_INDEX + 1;

	private CTabFolder tabSchemaDetailFolder;
	private TableViewer tvTables;
	private TableViewer tvViews;
	private TableViewer tvSerials;
	private SQLTableManageView sqlMgrView;

	public GeneralObjMappingView(Composite parent) {
		super(parent);
	}

	/**
	 * Create controls
	 * 
	 * @param parent Composite
	 */
	protected void createControl(Composite parent) {
		tabSchemaDetailFolder = new CTabFolder(parent, SWT.BORDER);
		final GridData gdTabFolder = new GridData(SWT.FILL, SWT.FILL, true, true);
		gdTabFolder.exclude = false;
		tabSchemaDetailFolder.setLayoutData(gdTabFolder);
		tabSchemaDetailFolder.setSimple(false);
		tabSchemaDetailFolder.setUnselectedCloseVisible(false);
		tabSchemaDetailFolder.setTabHeight(24);

		createDetailOfTables(tabSchemaDetailFolder);
		createDetailOfViews(tabSchemaDetailFolder);
		createDetailOfSerials(tabSchemaDetailFolder);
		createSQLManager(tabSchemaDetailFolder);
	}

	/**
	 * Create SQL table manager tab.
	 * 
	 * @param parent CTabFolder
	 */
	private void createSQLManager(CTabFolder parent) {
		Composite container = CompositeUtils.createTabItem(parent,
				Messages.objectMapPageTabFolderSqls, "icon/db/SQL.png");
		sqlMgrView = new SQLTableManageView(container);
		sqlMgrView.show();
	}

	/**
	 * Create table to list database serial mappings.
	 * 
	 * @param parent CTabFolder
	 */
	private void createDetailOfSerials(CTabFolder parent) {
		Composite container = CompositeUtils.createTabItem(parent,
				Messages.objectMapPageTabFolderSequences, "icon/db/serial.png");

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.tabTitleSourceSerial,
				Messages.tabTitleTargetSerial, Messages.lblCreate, Messages.lblReplace});
		initSourceConfigTableBuilder(tvBuilder);
		tvSerials = tvBuilder.buildTableViewer(container, SWT.BORDER | SWT.FULL_SELECTION);

		initSourceConfigTableViewer(tvSerials);
		tvSerials.setData(CONTENT_TYPE, CT_SERIAL);
	}

	/**
	 * Create table to list database table mappings.
	 * 
	 * @param parent CTabFolder
	 */
	private void createDetailOfTables(CTabFolder parent) {
		Composite container = CompositeUtils.createTabItem(parent,
				Messages.objectMapPageTabFolderTables, "icon/db/tables.png");
		final Text txtFilter = new Text(container, SWT.BORDER);
		txtFilter.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		txtFilter.setToolTipText(Messages.msgTTLocateSourceTable);
		txtFilter.addModifyListener(new ModifyListener() {

			public void modifyText(ModifyEvent ev) {
				String exp = txtFilter.getText().replaceAll("\\*", ".*") + ".*";
				for (TableItem ti : tvTables.getTable().getItems()) {
					Object[] obj = (Object[]) ti.getData();
					String name = (String) obj[SOURCE_NAME_INDEX];
					if (name.matches(exp)) {
						tvTables.getTable().setSelection(ti);
						TableCursor cursor = CompositeUtils.getTableCursor(tvTables);
						cursor.setSelection(ti, cursor.getColumn());
						break;
					}
				}
			}
		});

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.tabTitleSourceTable,
				Messages.tabTitleTargetTable, Messages.tabTitleData, Messages.tabTitleCondition,
				Messages.lblCreate, Messages.lblReplace, Messages.tabTitlePK});
		tvBuilder.setColumnWidths(new int[] {160, 160, 70, 150, 80, 90, 60});
		tvBuilder.setColumnTooltips(new String[] {Messages.tabTitleSourceTableDes,
				Messages.tabTitleTargetTableDes, Messages.tabTitleDataDes,
				Messages.tabTitleConditionDes, Messages.lblCreateDes, Messages.lblReplaceDes,
				Messages.tabTitlePKDes});
		final CellEditorFactory[] cellEditors = new CellEditorFactory[] {null,
				new TextCellEditorFactory(), new CheckboxCellEditorFactory(),
				new TextCellEditorFactory(), new CheckboxCellEditorFactory(),
				new CheckboxCellEditorFactory(), new CheckboxCellEditorFactory()};
		tvBuilder.setCellEditorClasses(cellEditors);
		tvBuilder.setCellValidators(new ICellEditorValidator[] {null, new CUBRIDNameValidator(),
				null, null, null, null, null});
		tvBuilder.setContentProvider(new StructuredContentProviderAdaptor() {

			@SuppressWarnings("unchecked")
			public Object[] getElements(Object inputElement) {
				List<Object> data = new ArrayList<Object>();
				for (SourceEntryTableConfig setc : (List<SourceEntryTableConfig>) inputElement) {
					//Add the SourceConfig to the end of the object array.
					data.add(new Object[] {setc.getName(), setc.getTarget(), setc.isMigrateData(),
							setc.getCondition(), setc.isCreateNewTable(), setc.isReplace(),
							setc.isCreatePK(), setc});
				}
				return super.getElements(data);
			}

		});
		//tvBuilder.setLabelProvider();
		final ObjectArrayRowCellModifier cellModifier = new ObjectArrayRowCellModifier() {

			/**
			 * If source database is XML dump, the condition input is not
			 * supported. If Data migration is not checked, the condition input
			 * is not supported.
			 * 
			 * @param element the table item's data: Object[]
			 * @param property column property == column index
			 * 
			 * @return can modify
			 */
			public boolean canModify(Object element, String property) {
				final int idx = Integer.parseInt(property);
				Object[] obj = (Object[]) element;
				if (idx == CONDITION_COLUMN_INDEX) {
					//Controlled by isMigrateData option
					return config.sourceIsOnline() && (Boolean) obj[DATA_COLUMN_INDEX];
				}
				return super.canModify(element, property);
			}

			/**
			 * If the first column is checked or unchecked, the other column's
			 * checking status should be according with it.
			 * 
			 * @param element the table item object
			 * @param property column index
			 * @param value the input value
			 * 
			 */
			protected void modify(TableItem ti, Object[] element, int columnIdx, Object value) {
				Object[] obj = (Object[]) ti.getData();
				final int crtIdx = CREATE_COLUMN_INDEX;
				if (columnIdx == crtIdx) {
					for (int i = crtIdx + 1; i < obj.length; i++) {
						if (!(obj[i] instanceof Boolean)) {
							continue;
						}
						obj[i] = value;
						ti.setImage(i, CompositeUtils.getCheckImage((Boolean) value));
						updateColumnImage(value, ti, i);
					}
				} else if (columnIdx > crtIdx && !(Boolean) obj[crtIdx] && (Boolean) value) {
					//If check create other part of the table, 
					//the create table option will be auto  checked.
					obj[crtIdx] = value;
					ti.setImage(crtIdx, CompositeUtils.getCheckImage((Boolean) value));
					updateColumnImage(value, ti, crtIdx);
				}
				super.modify(ti, element, columnIdx, value);
			}
		};
		tvBuilder.setCellModifier(cellModifier);
		tvBuilder.setTableCursorSupported(true);
		tvTables = tvBuilder.buildTableViewer(container, SWT.BORDER | SWT.FULL_SELECTION);
		tvTables.setData(CONTENT_TYPE, CT_TABLE);

		final SelectionListener[] selectionListeners = new SelectionListener[] {
				null,
				null,
				new CheckBoxColumnSelectionListener(),
				null,
				new CheckBoxColumnSelectionListener(new int[] {REPLACE_COLUMN_INDEX,
						PK_COLUMN_INDEX}, true, true),
				new CheckBoxColumnSelectionListener(new int[] {CREATE_COLUMN_INDEX}, true, false),
				new CheckBoxColumnSelectionListener(new int[] {CREATE_COLUMN_INDEX}, true, false)};
		CompositeUtils.setTableColumnSelectionListener(tvTables, selectionListeners);
	}

	/**
	 * Create table to list database view mappings.
	 * 
	 * @param parent CTabFolder
	 */
	private void createDetailOfViews(CTabFolder parent) {
		Composite container = CompositeUtils.createTabItem(parent,
				Messages.objectMapPageTabFolderViews, "icon/db/views.png");

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.tabTitleSourceView,
				Messages.tabTitleTargetView, Messages.lblCreate, Messages.lblReplace});
		initSourceConfigTableBuilder(tvBuilder);
		tvViews = tvBuilder.buildTableViewer(container, SWT.BORDER | SWT.FULL_SELECTION);

		initSourceConfigTableViewer(tvViews);
		tvViews.setData(CONTENT_TYPE, CT_VIEW);
	}

	/**
	 * Hide this mapping view
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(tabSchemaDetailFolder, true);
	}

	/**
	 * Save UI to migration configuration
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		CompositeUtils.applyTableViewerEditing(tvTables);
		CompositeUtils.applyTableViewerEditing(tvSerials);
		CompositeUtils.applyTableViewerEditing(tvViews);
		VerifyResultMessages result = validate();
		if (result.hasError()) {
			return result;
		}
		//Save UI to configuration object
		for (int i = 0; i < tvTables.getTable().getItemCount(); i++) {
			TableItem ti = tvTables.getTable().getItem(i);
			Object[] obj = (Object[]) ti.getData();
			SourceEntryTableConfig setc = (SourceEntryTableConfig) obj[obj.length - 1];
			//If target name is changed
			final String name = (String) obj[TARGET_NAME_INDEX];
			config.changeTarget(setc, name);
			setc.setMigrateData((Boolean) obj[DATA_COLUMN_INDEX]);
			setc.setCondition(obj[CONDITION_COLUMN_INDEX].toString());
			setc.setCreateNewTable((Boolean) obj[CREATE_COLUMN_INDEX]);
			setc.setReplace((Boolean) obj[REPLACE_COLUMN_INDEX]);
			setc.setCreatePK((Boolean) obj[PK_COLUMN_INDEX]);
		}
		for (int i = 0; i < tvViews.getTable().getItemCount(); i++) {
			TableItem ti = tvViews.getTable().getItem(i);
			Object[] obj = (Object[]) ti.getData();
			SourceConfig setc = (SourceConfig) obj[obj.length - 1];
			View vw = config.getTargetViewSchema(setc.getTarget());
			final String name = obj[1].toString();
			if (vw != null) {
				vw.setName(name);
			}
			setc.setTarget(name);
			setc.setCreate((Boolean) obj[2]);
			setc.setReplace((Boolean) obj[3]);
		}
		for (int i = 0; i < tvSerials.getTable().getItemCount(); i++) {
			TableItem ti = tvSerials.getTable().getItem(i);
			Object[] obj = (Object[]) ti.getData();
			SourceConfig setc = (SourceConfig) obj[obj.length - 1];
			Sequence ts = config.getTargetSerialSchema(setc.getTarget());
			final String name = obj[1].toString();
			if (ts != null) {
				ts.setName(name);
			}
			setc.setTarget(name);
			setc.setCreate((Boolean) obj[2]);
			setc.setReplace((Boolean) obj[3]);
		}
		return super.save();
	}

	/**
	 * Bring this mapping view onto the top.
	 */
	public void show() {
		CompositeUtils.hideOrShowComposite(tabSchemaDetailFolder, false);
	}

	/**
	 * Display input data.
	 * 
	 * @param obj should be a MigrationConfiguration object
	 */
	public void showData(Object obj) {
		super.showData(obj);
		SchemaNode schema = null;
		if (obj instanceof DatabaseNode) {
			tabSchemaDetailFolder.setSelection(0);
		} else if (obj instanceof SchemaNode) {
			schema = (SchemaNode) obj;
			tabSchemaDetailFolder.setSelection(0);
		} else if (obj instanceof TablesNode) {
			if (((TablesNode) obj).getParent() instanceof SchemaNode) {
				schema = (SchemaNode) ((TablesNode) obj).getParent();
			}
			tabSchemaDetailFolder.setSelection(0);
		} else if (obj instanceof ViewsNode) {
			if (((ViewsNode) obj).getParent() instanceof SchemaNode) {
				schema = (SchemaNode) ((ViewsNode) obj).getParent();
			}
			tabSchemaDetailFolder.setSelection(1);
		} else if (obj instanceof SequencesNode) {
			if (((SequencesNode) obj).getParent() instanceof SchemaNode) {
				schema = (SchemaNode) ((SequencesNode) obj).getParent();
			}
			tabSchemaDetailFolder.setSelection(2);
		} else if (obj instanceof SQLTablesNode) {
			tabSchemaDetailFolder.setSelection(3);
		}
		List<SourceEntryTableConfig> ipTables = config.getExpEntryTableCfg();
		List<SourceViewConfig> ipViews = config.getExpViewCfg();
		List<SourceSequenceConfig> ipSerials = config.getExpSerialCfg();
		if (schema != null) {
			Iterator<SourceEntryTableConfig> itTables = ipTables.iterator();
			while (itTables.hasNext()) {
				SourceEntryTableConfig setc = itTables.next();
				String owner = setc.getOwner();
				if (owner == null || schema.getName().equals(owner)) {
					continue;
				}
				itTables.remove();
			}
			Iterator<SourceViewConfig> itViews = ipViews.iterator();
			while (itViews.hasNext()) {
				String owner = itViews.next().getOwner();
				if (owner == null || schema.getName().equals(owner)) {
					continue;
				}
				itViews.remove();
			}
			Iterator<SourceSequenceConfig> itSerials = ipSerials.iterator();
			while (itSerials.hasNext()) {
				String owner = itSerials.next().getOwner();
				if (owner == null || schema.getName().equals(owner)) {
					continue;
				}
				itSerials.remove();
			}
		}
		tvTables.setInput(ipTables);
		tvViews.setInput(ipViews);
		tvSerials.setInput(ipSerials);
		sqlMgrView.showData(obj);

		CompositeUtils.initTableViewerCheckColumnImage(tvTables);
		CompositeUtils.initTableViewerCheckColumnImage(tvViews);
		CompositeUtils.initTableViewerCheckColumnImage(tvSerials);

	}

	/**
	 * Validate duplicated names
	 * 
	 * @return VerifyResultMessages
	 */
	private VerifyResultMessages validate() {

		Map<String, Boolean> names = new TreeMap<String, Boolean>();
		for (int i = 0; i < tvTables.getTable().getItemCount(); i++) {
			TableItem ti = tvTables.getTable().getItem(i);
			Object[] obj = (Object[]) ti.getData();
			final String name = (String) obj[TARGET_NAME_INDEX];
			final Boolean isMigData = (Boolean) obj[DATA_COLUMN_INDEX];
			final Boolean isCreate = (Boolean) obj[CREATE_COLUMN_INDEX];
			final boolean isSelected = isMigData || isCreate;
			if (isSelected && !MigrationCfgUtils.verifyTargetDBObjName(name)) {
				return new VerifyResultMessages(
						Messages.bind(Messages.msgErrInvalidTableName, name), null, null);
			}
			names.put(name.toLowerCase(Locale.US), isSelected);
			//Validate condition
			if (!MigrationCfgUtils.checkEntryTableCondition(config, isMigData,
					(String) obj[SOURCE_NAME_INDEX], (String) obj[CONDITION_COLUMN_INDEX])) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidCondition,
						(String) obj[SOURCE_NAME_INDEX]), null, null);
			}
		}
		for (int i = 0; i < tvViews.getTable().getItemCount(); i++) {
			TableItem ti = tvViews.getTable().getItem(i);
			Object[] obj = (Object[]) ti.getData();
			final String name = obj[1].toString();
			final Boolean isCreate = (Boolean) obj[2];
			if (isCreate) {
				if (!MigrationCfgUtils.verifyTargetDBObjName(name)) {
					return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidViewName,
							name), null, null);
				}
				if (names.get(name) != null && names.get(name)) {
					return new VerifyResultMessages(
							Messages.bind(Messages.msgErrDupViewName, name), null, null);
				}
			}
			names.put(name.toLowerCase(Locale.US), isCreate);
		}

		//Serial name can be duplicated with view or table.
		names.clear();
		for (int i = 0; i < tvSerials.getTable().getItemCount(); i++) {
			TableItem ti = tvSerials.getTable().getItem(i);
			Object[] obj = (Object[]) ti.getData();
			final String name = obj[1].toString();
			if (!MigrationCfgUtils.verifyTargetDBObjName(name)) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidSerialName,
						name), null, null);
			}
			if (names.get(name) != null) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrDupSerialName, name),
						null, null);
			}
			names.put(name.toLowerCase(Locale.US), true);
		}
		return sqlMgrView.save();
	}

	/**
	 * Add table viewers double click listener
	 * 
	 * @param iDoubleClickListener IDoubleClickListener
	 */
	public void addDoubleClickListener(IDoubleClickListener iDoubleClickListener) {
		tvTables.addDoubleClickListener(iDoubleClickListener);
		tvViews.addDoubleClickListener(iDoubleClickListener);
		tvSerials.addDoubleClickListener(iDoubleClickListener);
	}

	/**
	 * Add SQL changed listener
	 * 
	 * @param listener ISQLTableChangedListener
	 */
	public void addSQLChangedListener(ISQLTableChangedListener listener) {
		sqlMgrView.addSQLChangedListener(listener);
	}

	/**
	 * Set the migration configuration.It should not be NULL.
	 * 
	 * @param config MigrationConfiguration
	 */
	public void setMigrationConfig(MigrationConfiguration config) {
		super.setMigrationConfig(config);
		this.sqlMgrView.setMigrationConfig(config);
	}

	/**
	 * @param wizardStatus IMigrationWizardStatus
	 */
	public void setWizardStatus(IMigrationWizardStatus wizardStatus) {
		super.setWizardStatus(wizardStatus);
		this.sqlMgrView.setWizardStatus(wizardStatus);
	}

}
