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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.eclipse.jface.viewers.ICellEditorValidator;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.ObjectArrayRowCellModifier;
import com.cubrid.common.ui.swt.table.ObjectArrayRowTableLabelProvider;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.CheckboxCellEditorFactory;
import com.cubrid.common.ui.swt.table.celleditor.TextCellEditorFactory;
import com.cubrid.common.ui.swt.table.listener.CheckBoxColumnSelectionListener;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceFKConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceIndexConfig;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.navigator.node.ColumnsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FKsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.IndexesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.PartitionsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TableNode;
import com.cubrid.cubridmigration.ui.common.tableviewer.cell.validator.CUBRIDDataTypeValidator;
import com.cubrid.cubridmigration.ui.common.tableviewer.cell.validator.CUBRIDNameValidator;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * TableMappingView response to show entry table configuration
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-25 created by Kevin Cao
 */
public class TableMappingView extends
		AbstractMappingView {
	private final CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);

	/**
	 * TableColumnViewerContentProvider Description
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-9-19 created by Kevin Cao
	 */
	private class TableColumnViewerContentProvider extends
			StructuredContentProviderAdaptor {

		/**
		 * Retrieves the elements list of table viewer
		 * 
		 * @param inputElement (List<SourceColumnConfig>)
		 * @return Object[]
		 */
		@SuppressWarnings("unchecked")
		public Object[] getElements(Object inputElement) {
			List<Object[]> result = new ArrayList<Object[]>();
			if (setc == null) {
				return super.getElements(result);
			}
			Table srcTable = config.getSrcTableSchema(setc.getOwner(), setc.getName());
			Table tt = config.getTargetTableSchema(setc.getTarget());
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
				result.add(new Object[] {scc.isCreate(), scol.getName(), scol.getShownDataType(),
						scc.getTarget(), tcol.getShownDataType(), scc});
			}
			return super.getElements(result);
		}
	}

	/**
	 * TableColumnViewerLabelProvider Description
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-9-19 created by Kevin Cao
	 */
	private class TableColumnViewerLabelProvider extends
			ObjectArrayRowTableLabelProvider {

		/**
		 * Get column items image
		 * 
		 * @param element Object[]
		 * @param columnIndex int
		 * @return Image
		 */
		public Image getColumnImage(Object element, int columnIndex) {
			return super.getColumnImage(element, columnIndex);
		}
	}

	/**
	 * TargetFocusAdapter Description
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-9-19 created by Kevin Cao
	 */
	private class TargetFocusAdapter extends
			FocusAdapter {

		/**
		 * Focus lost event handle
		 * 
		 * @param ex FocusEvent
		 */
		public void focusLost(FocusEvent ex) {
			final String newName = txtTarget.getText().trim();
			if (newName.equalsIgnoreCase(setc.getTarget())) {
				return;
			}
			Table tt = config.getTargetTableSchema(newName);
			if (tt == null) {
				return;
			}
			setc.setTarget(newName);
			fillTabContent(setc);
		}

	}

	//	private static final int PK_COL_IDX = 3;
	//	private static final Image IMAGE_PK = MigrationUIPlugin.getImage("icon/primary_key.png");
	private final static Map<String, Integer> NODE2PAGEMAP = new HashMap<String, Integer>();
	static {
		NODE2PAGEMAP.put(ColumnsNode.class.getName(), 0);
		NODE2PAGEMAP.put(FKsNode.class.getName(), 2);
		NODE2PAGEMAP.put(IndexesNode.class.getName(), 3);
		NODE2PAGEMAP.put(PartitionsNode.class.getName(), 4);
	}
	private CTabFolder tabTableDetailFolder;
	private PartitionMappingView partitionMappingView;
	private Text txtSource;
	private Text txtTarget;
	private TableViewer tvTableColumns;
	private TableViewer tvTableFKs;
	private TableViewer tvTableIndexes;
	private SourceEntryTableConfig setc;
	private Button btnCreate;
	private Button btnReplace;

	private Button btnCreatePK;

	private Button btnMigrateData;

	private Button btnReuseOID;

	private Text txtUserSQL;

	private Text txtUserSQL2;

	private org.eclipse.swt.widgets.List allColumnList;

	private org.eclipse.swt.widgets.List pkColumnList;

	private Text txtSrcPKColumns;
	private Text txtSrcPKName;
	private Text txtTarPKName;

	private Button btnMvRight;

	private Button btnMvLeft;
	private Button btnAutoStartByTargetMax;
	private Button btnEnableExpOpt;
	private Group grpExportingOpt;

	public TableMappingView(Composite parent) {
		super(parent);
	}

	/**
	 * Add table viewers double click listener.
	 * 
	 * @param listener IDoubleClickListener
	 */
	public void addDoubleClickListener(IDoubleClickListener listener) {
		tvTableColumns.addDoubleClickListener(listener);
		tvTableFKs.addDoubleClickListener(listener);
		tvTableIndexes.addDoubleClickListener(listener);
	}

	/**
	 * Create controls of this view
	 * 
	 * @param parent of the controls
	 */
	protected void createControl(Composite parent) {
		tabTableDetailFolder = new CTabFolder(parent, SWT.BORDER);
		tabTableDetailFolder.setVisible(false);
		final GridData gdTabFolder = new GridData(SWT.FILL, SWT.FILL, true, true);
		gdTabFolder.exclude = true;
		tabTableDetailFolder.setLayoutData(gdTabFolder);
		tabTableDetailFolder.setSimple(false);
		tabTableDetailFolder.setUnselectedCloseVisible(false);
		tabTableDetailFolder.setTabHeight(24);

		createDetailOfColumns(tabTableDetailFolder);
		createDetailOfPKs(tabTableDetailFolder);
		createDetailOfFKs(tabTableDetailFolder);
		createDetailOfIndexes(tabTableDetailFolder);
		createDetailOfPartition(tabTableDetailFolder);
		createDetailOfUserDefine(tabTableDetailFolder);
	}

	/**
	 * Export options tab. It is shown only when source is online CUBRID.
	 * 
	 * @param parent tabTableDetailFolder2
	 */
	private void createDetailOfExpOpts(Composite parent) {
		grpExportingOpt = new Group(parent, SWT.NONE);
		grpExportingOpt.setText(Messages.grpExportingOpt);
		grpExportingOpt.setLayout(new GridLayout());
		grpExportingOpt.setLayoutData(new GridData(SWT.FILL, SWT.BOTTOM, true, false));

		btnEnableExpOpt = new Button(grpExportingOpt, SWT.CHECK);
		btnEnableExpOpt.setText(Messages.btnEnableExpOpt);
		btnEnableExpOpt.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				setExpOptStatus(btnEnableExpOpt.getSelection(),
						btnAutoStartByTargetMax.getSelection());
			}

		});

		btnAutoStartByTargetMax = new Button(grpExportingOpt, SWT.CHECK);
		btnAutoStartByTargetMax.setText(Messages.btnAutoStartByTargetMax);
		btnAutoStartByTargetMax.setToolTipText(Messages.btnAutoStartByTargetMaxTip);
	}

	/**
	 * Create detail of primary keys
	 * 
	 * @param parent CTabFolder
	 */
	private void createDetailOfPKs(CTabFolder parent) {
		Composite container = CompositeUtils.createTabItem(parent, "PK", "icon/primary_key.png");
		Composite comCheck = new Composite(container, SWT.NONE);
		comCheck.setLayout(new GridLayout(5, false));
		comCheck.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));

		btnCreatePK = new Button(comCheck, SWT.CHECK);
		btnCreatePK.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnCreatePK.setText(Messages.lblCreatePK);
		btnCreatePK.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ex) {
				setCreatePKStatus(btnCreatePK.getSelection());
			}
		});
		Label lblSrcPKName = new Label(comCheck, SWT.NONE);
		lblSrcPKName.setText("Source PK Name:");
		txtSrcPKName = new Text(comCheck, SWT.BORDER | SWT.READ_ONLY);
		txtSrcPKName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));

		Label lblTarPKName = new Label(comCheck, SWT.NONE);
		lblTarPKName.setText("Target PK Name:");

		txtTarPKName = new Text(comCheck, SWT.BORDER);
		txtTarPKName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));

		Label lblSrcPKColumns = new Label(comCheck, SWT.NONE);
		lblSrcPKColumns.setText(Messages.sourcePKColumns);
		txtSrcPKColumns = new Text(comCheck, SWT.BORDER | SWT.READ_ONLY);
		txtSrcPKColumns.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 4, 1));

		Composite comDetail = new Composite(container, SWT.NONE);
		comDetail.setLayout(new GridLayout(3, false));
		comDetail.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Group grpAllCols = new Group(comDetail, SWT.NONE);
		grpAllCols.setLayout(new GridLayout());
		grpAllCols.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		grpAllCols.setText(Messages.allTargetColumns);

		allColumnList = new org.eclipse.swt.widgets.List(grpAllCols, SWT.BORDER | SWT.V_SCROLL);
		allColumnList.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Composite comMoveButton = new Composite(comDetail, SWT.NONE);
		comMoveButton.setLayout(new GridLayout());
		comMoveButton.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false));

		btnMvRight = new Button(comMoveButton, SWT.NONE);
		btnMvRight.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false));
		btnMvRight.setText(" > ");
		btnMvRight.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent se) {
				if (allColumnList.getSelectionCount() == 0) {
					return;
				}
				int ss = allColumnList.getSelectionIndex();
				String[] tobeMoved = allColumnList.getSelection();
				for (String str : tobeMoved) {
					pkColumnList.add(str);
					allColumnList.remove(str);
				}
				setListSelection(allColumnList, ss);
				setListSelection(pkColumnList, pkColumnList.getItemCount());
			}

		});

		btnMvLeft = new Button(comMoveButton, SWT.NONE);
		btnMvLeft.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false));
		btnMvLeft.setText(" < ");
		btnMvLeft.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent se) {
				if (pkColumnList.getSelectionCount() == 0) {
					return;
				}
				int sp = pkColumnList.getSelectionIndex();
				String[] tobeMoved = pkColumnList.getSelection();
				for (String str : tobeMoved) {
					allColumnList.add(str);
					pkColumnList.remove(str);
				}
				setListSelection(pkColumnList, sp);
				setListSelection(allColumnList, allColumnList.getItemCount());
			}

		});

		Group grpPKCols = new Group(comDetail, SWT.NONE);
		grpPKCols.setLayout(new GridLayout());
		grpPKCols.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		grpPKCols.setText(Messages.targetPKColumns);
		pkColumnList = new org.eclipse.swt.widgets.List(grpPKCols, SWT.BORDER | SWT.V_SCROLL);
		pkColumnList.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
	}

	/**
	 * Update column name when column name changed
	 * 
	 * @param oldName of target column
	 * @param newName of target column
	 */
	private void updatePKTabColumnName(String oldName, String newName) {
		int idx = allColumnList.indexOf(oldName);
		if (idx >= 0) {
			int sel = allColumnList.getSelectionIndex();
			allColumnList.remove(idx);
			allColumnList.add(newName, idx);
			setListSelection(allColumnList, sel);
			return;
		}
		idx = pkColumnList.indexOf(oldName);
		if (idx < 0) {
			return;
		}
		int sel = pkColumnList.getSelectionIndex();
		pkColumnList.remove(idx);
		pkColumnList.add(newName, idx);
		setListSelection(pkColumnList, sel);
	}

	/**
	 * Set the list's selection item
	 * 
	 * @param list to be set
	 * @param selection index
	 */
	private void setListSelection(org.eclipse.swt.widgets.List list, int selection) {
		if (list.getItemCount() == 0) {
			return;
		}
		if (selection < 0) {
			list.setSelection(0);
			return;
		}
		if (selection >= list.getItemCount()) {
			list.setSelection(list.getItemCount() - 1);
			return;
		}
		list.setSelection(selection);
	}

	/**
	 * Create table's general information tab.
	 * 
	 * @param parent CTabFolder
	 */
	private void createDetailOfColumns(CTabFolder parent) {
		Composite container = CompositeUtils.createTabItem(parent, Messages.tabTitleGeneral,
				"icon/db/serial.png");
		Composite comCheck = new Composite(container, SWT.NONE);
		comCheck.setLayout(new GridLayout(5, false));
		comCheck.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));

		btnCreate = new Button(comCheck, SWT.CHECK);
		btnCreate.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnCreate.setText(Messages.lblCreateNewTable);
		btnCreate.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ex) {
				setPageControlStatus();
				setc.setCreatePK(btnCreate.getSelection());
				setc.setCreatePartition(btnCreate.getSelection());
				if (btnCreate.getSelection()) {
					selectAllColumns();
					selectAllFSs();
					selectAllIndexes();
					partitionMappingView.showData(setc);
				}
				setCreatePKStatus(btnCreatePK.getSelection());
			}
		});

		btnReplace = new Button(comCheck, SWT.CHECK);
		btnReplace.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnReplace.setText(Messages.lblReplaceTable);

		btnReuseOID = new Button(comCheck, SWT.CHECK);
		btnReuseOID.setText(Messages.lblReuseOID);
		btnReuseOID.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		btnMigrateData = new Button(comCheck, SWT.CHECK);
		btnMigrateData.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnMigrateData.setText(Messages.lblMigrateData);
		btnMigrateData.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				final boolean status = btnCreate.getSelection() || btnMigrateData.getSelection();
				txtTarget.setEditable(status);
				tvTableColumns.getTable().setEnabled(status);
				if (btnMigrateData.getSelection()) {
					selectAllColumns();
				}
				setExpOptStatus(btnEnableExpOpt.getSelection(),
						btnAutoStartByTargetMax.getSelection());
			}
		});

		Composite nameContainer = new Composite(container, SWT.NONE);
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

		Composite columnContainer = new Composite(container, SWT.NONE);
		columnContainer.setLayout(new GridLayout(2, false));
		columnContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		//Table columns
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {"", Messages.tabTitleSourceColumn,
				Messages.tabTitleDataType, Messages.tabTitleTargetColumn, Messages.tabTitleDataType});
		tvBuilder.setColumnWidths(new int[] {32, 150, 150, 150, 150});
		tvBuilder.setCellEditorClasses(new CellEditorFactory[] {new CheckboxCellEditorFactory(),
				null, null, new TextCellEditorFactory(), new TextCellEditorFactory()});
		tvBuilder.setCellValidators(new ICellEditorValidator[] {null, null, null,
				new CUBRIDNameValidator(), new CUBRIDDataTypeValidator()});
		tvBuilder.setContentProvider(new TableColumnViewerContentProvider());
		tvBuilder.setLabelProvider(new TableColumnViewerLabelProvider());
		final ObjectArrayRowCellModifier cellModifier = new ObjectArrayRowCellModifier() {

			public boolean canModify(Object element, String property) {
				int idx = Integer.parseInt(property);
				if (idx == 0) {
					return true;
				}
				//If column is not selected, can't modify name and type 
				return (Boolean) ((Object[]) element)[0];
			}

			protected void modify(TableItem ti, Object[] element, int columnIdx, Object value) {
				String oldName = element[3].toString();
				super.modify(ti, element, columnIdx, value);
				if (columnIdx == 3) {
					updatePKTabColumnName(oldName, value.toString());
				}
			}

		};
		tvBuilder.setCellModifier(cellModifier);
		tvBuilder.setTableCursorSupported(true);
		tvTableColumns = tvBuilder.buildTableViewer(columnContainer, SWT.BORDER
				| SWT.FULL_SELECTION);
		tvTableColumns.setData(CONTENT_TYPE, CT_COLUMN);
		CompositeUtils.setTableColumnSelectionListener(tvTableColumns, new SelectionListener[] {
				new CheckBoxColumnSelectionListener(), null, null, null, null});
		//tvTableColumns.getTable().addKeyListener(new SpaceKey2CheckAdapter(0));

		Composite comColumnOrder = new Composite(columnContainer, SWT.NONE);
		comColumnOrder.setLayout(new GridLayout());
		comColumnOrder.setLayoutData(new GridData(SWT.RIGHT, SWT.FILL, false, true));
		Button btnUp = new Button(comColumnOrder, SWT.NONE);
		btnUp.setText(Messages.btnUp);
		btnUp.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnUp.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ex) {
				moveColumn(true);
			}

		});
		Button btnDown = new Button(comColumnOrder, SWT.NONE);
		btnDown.setText(Messages.btnDown);
		btnDown.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnDown.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ex) {
				moveColumn(false);
			}

		});

		createDetailOfExpOpts(container);
	}

	/**
	 * Create table's foreign keys information tab.
	 * 
	 * @param parent CTabFolder
	 */
	private void createDetailOfFKs(CTabFolder parent) {
		Composite container = CompositeUtils.createTabItem(parent, Messages.tabTitleFK,
				"icon/db/tables.png");
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.tabTitleSourceFK,
				Messages.tabTitleTargetFK, Messages.lblCreate, Messages.lblReplace});
		initSourceConfigTableBuilder(tvBuilder);
		tvTableFKs = tvBuilder.buildTableViewer(container, SWT.BORDER | SWT.FULL_SELECTION);

		super.initSourceConfigTableViewer(tvTableFKs);
		tvTableFKs.setData(CONTENT_TYPE, CT_FK);
		//tvTableFKs.getTable().addKeyListener(new SpaceKey2CheckAdapter(2, new int[] {3}, true, true));
	}

	/**
	 * Create table's indexes information tab.
	 * 
	 * @param parent CTabFolder
	 */
	private void createDetailOfIndexes(CTabFolder parent) {
		Composite container = CompositeUtils.createTabItem(parent, Messages.tabTitleIndexes,
				"icon/db/tables.png");
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.tabTitleSourceIndex,
				Messages.tabTitleTargetIndex, Messages.lblCreate, Messages.lblReplace});
		initSourceConfigTableBuilder(tvBuilder);
		tvTableIndexes = tvBuilder.buildTableViewer(container, SWT.BORDER | SWT.FULL_SELECTION);

		super.initSourceConfigTableViewer(tvTableIndexes);
		tvTableIndexes.setData(CONTENT_TYPE, CT_INDEX);
		//tvTableIndexes.getTable().addKeyListener(new SpaceKey2CheckAdapter(2, new int[] {3}, true, true));
	}

	/**
	 * Create table's partition information tab.
	 * 
	 * @param parent CTabFolder
	 */
	private void createDetailOfPartition(CTabFolder parent) {
		Composite partCom = CompositeUtils.createTabItem(parent, Messages.tabTitlePartitions,
				"icon/db/tables.png");
		partitionMappingView = new PartitionMappingView(partCom);
	}

	/**
	 * Create user definition SQL tab.
	 * 
	 * @param parent CTabFolder
	 */
	private void createDetailOfUserDefine(CTabFolder parent) {
		Composite partCom = CompositeUtils.createTabItem(parent, Messages.tabTitleUserSQL,
				"icon/db/SQL.png");
		Group comButtons = new Group(partCom, SWT.NONE);
		comButtons.setText(Messages.lblUserSQL1);
		comButtons.setLayout(new GridLayout());
		comButtons.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		txtUserSQL = new Text(comButtons, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER);
		txtUserSQL.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Group comButtons2 = new Group(partCom, SWT.NONE);
		comButtons2.setText(Messages.lblUserSQL2);
		comButtons2.setLayout(new GridLayout());
		comButtons2.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		txtUserSQL2 = new Text(comButtons2, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER);
		txtUserSQL2.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
	}

	/**
	 * Fill table mapping information into UI
	 * 
	 * @param stc SourceTableConfig
	 */
	private void fillData(SourceEntryTableConfig stc) {
		btnCreate.setSelection(stc.isCreateNewTable());
		btnReplace.setSelection(stc.isReplace());
		btnMigrateData.setSelection(stc.isMigrateData());

		txtSource.setText(stc.getName());
		txtTarget.setText(stc.getTarget());

		txtUserSQL.setText(stc.getSqlBefore());
		txtUserSQL2.setText(stc.getSqlAfter());

		Table tt = config.getTargetTableSchema(stc.getTarget());
		if (tt != null) {
			btnReuseOID.setSelection(tt.isReuseOID());
		}
		fillTabContent(stc);

	}

	/**
	 * File table viewers
	 * 
	 * @param stc SourceEntryTableConfig
	 */
	private void fillTabContent(SourceEntryTableConfig stc) {
		tvTableColumns.setInput(stc.getColumnConfigList());
		tvTableFKs.setInput(stc.getFKConfigList());
		tvTableIndexes.setInput(stc.getIndexConfigList());
		partitionMappingView.showData(stc);

		CompositeUtils.initTableViewerCheckColumnImage(tvTableColumns);
		CompositeUtils.initTableViewerCheckColumnImage(tvTableFKs);
		CompositeUtils.initTableViewerCheckColumnImage(tvTableIndexes);

		//Fill source PK columns text
		Table st = config.getSrcTableSchema(stc.getOwner(), stc.getName());
		if (st == null) {
			return;
		}
		PK spk = st.getPk();
		txtSrcPKColumns.setText("");
		if (spk != null) {
			int i = 0;
			for (String col : spk.getPkColumns()) {
				if (i > 0) {
					txtSrcPKColumns.append(",");
				}
				txtSrcPKColumns.append(col);
				i++;
			}
			txtSrcPKName.setText(spk.getName() == null ? "" : spk.getName());
		}
		//Fill target PK columns editor.
		allColumnList.removeAll();
		pkColumnList.removeAll();
		Table tt = config.getTargetTableSchema(stc.getTarget());
		if (tt == null) {
			return;
		}
		PK pk = tt.getPk();
		final List<String> pkCols;
		if (pk == null) {
			pkCols = new ArrayList<String>();
		} else {
			pkCols = pk.getPkColumns();
			txtTarPKName.setText(pk.getName() == null ? "" : pk.getName());
		}
		for (Column col : tt.getColumns()) {
			if (pkCols.indexOf(col.getName()) < 0) {
				allColumnList.add(col.getName());
			}
		}
		for (String col : pkCols) {
			pkColumnList.add(col);
		}
		setListSelection(allColumnList, 0);
		setListSelection(pkColumnList, 0);
	}

	/**
	 * Hide this view
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(tabTableDetailFolder, true);
	}

	/**
	 * Save UI into model
	 * 
	 * @return VerifyResultMessages
	 */
	@SuppressWarnings("unchecked")
	public VerifyResultMessages save() {
		if (setc == null) {
			return super.save();
		}
		//If don't create and migrate data, do not save.
		if (!btnCreate.getSelection() && !btnMigrateData.getSelection()) {
			setc.setCreateNewTable(btnCreate.getSelection());
			setc.setReplace(btnReplace.getSelection());
			setc.setMigrateData(btnMigrateData.getSelection());
			return super.save();
		}
		VerifyResultMessages result = validate();
		if (result.hasError()) {
			return result;
		}
		setc.setCreateNewTable(btnCreate.getSelection());
		setc.setReplace(btnReplace.getSelection());
		setc.setMigrateData(btnMigrateData.getSelection());
		setc.setSqlBefore(txtUserSQL.getText().trim());
		setc.setSqlAfter(txtUserSQL2.getText().trim());

		String newTableName = txtTarget.getText().trim().toLowerCase(Locale.US);
		config.changeTarget(setc, newTableName);

		Table srcTable = config.getSrcTableSchema(setc.getOwner(), setc.getName());
		Table tt = config.getTargetTableSchema(setc.getTarget());
		if (tt == null) {
			return super.save();
		}
		tt.setReuseOID(btnReuseOID.getSelection());

		for (int i = 0; i < tvTableColumns.getTable().getItemCount(); i++) {

			Object[] obj = (Object[]) tvTableColumns.getElementAt(i);
			SourceColumnConfig scc = (SourceColumnConfig) obj[obj.length - 1];
			scc.setCreate((Boolean) obj[0]);
			config.changeTarget(scc, (String) obj[3]);
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
			dataTypeHelper.setColumnDataType((String) obj[4], col);
		}
		List<SourceColumnConfig> columns = (List<SourceColumnConfig>) tvTableColumns.getInput();
		MigrationCfgUtils.changeColumnOrder(setc, tt, columns);

		if (btnEnableExpOpt.isEnabled() && btnEnableExpOpt.getSelection()) {
			setc.setEnableExpOpt(true);
			setc.setStartFromTargetMax(btnAutoStartByTargetMax.getEnabled()
					&& btnAutoStartByTargetMax.getSelection());
		} else {
			setc.setEnableExpOpt(false);
			setc.setStartFromTargetMax(false);
		}

		if (!setc.isCreateNewTable()) {
			return super.save();
		}
		List<String> pkColumns = new ArrayList<String>();
		for (String pkCol : pkColumnList.getItems()) {
			pkColumns.add(pkCol);
		}
		//Update PK information
		if (pkColumns.isEmpty()) {
			setc.setCreatePK(false);
			//tt.setPk(null);
		} else {
			PK pk = tt.getPk();
			if (pk == null) {
				pk = new PK(tt);
				tt.setPk(pk);
			}
			pk.setPkColumns(pkColumns);
			pk.setName(txtTarPKName.getText().trim());
			setc.setCreatePK(btnCreatePK.getSelection());
		}
		//foreign key
		for (int i = 0; i < tvTableFKs.getTable().getItemCount(); i++) {
			Object[] obj = (Object[]) tvTableFKs.getElementAt(i);
			SourceFKConfig scc = (SourceFKConfig) obj[obj.length - 1];
			FK fk = tt.getFKByName(scc.getTarget());
			if (fk == null) {
				fk = new FK(tt);
				fk.copyFrom(srcTable.getFKByName(scc.getName()));
				tt.addFK(fk);
			}
			fk.setName((String) obj[1]);
			scc.setTarget((String) obj[1]);
			scc.setCreate((Boolean) obj[2]);
			scc.setReplace((Boolean) obj[3]);
		}
		//indexes
		for (int i = 0; i < tvTableIndexes.getTable().getItemCount(); i++) {
			Object[] obj = (Object[]) tvTableIndexes.getElementAt(i);
			SourceIndexConfig sic = (SourceIndexConfig) obj[obj.length - 1];
			Index idx = tt.getIndexByName(sic.getTarget());
			if (idx == null) {
				idx = new Index(tt);
				idx.copyFrom(srcTable.getIndexByName(sic.getName()));
				tt.addIndex(idx);
			}
			idx.setName((String) obj[1]);
			sic.setTarget((String) obj[1]);
			sic.setCreate((Boolean) obj[2]);
			sic.setReplace((Boolean) obj[3]);
		}

		return partitionMappingView.save();
	}

	/**
	 * Select all columns only all columns are not selected
	 * 
	 */
	private void selectAllColumns() {
		boolean flag = false;
		for (TableItem ti : tvTableColumns.getTable().getItems()) {
			Object[] obj = (Object[]) ti.getData();
			if ((Boolean) obj[0]) {
				flag = true;
				break;
			}
		}
		if (flag) {
			return;
		}
		final Image img = CompositeUtils.getCheckImage(true);
		for (TableItem ti : tvTableColumns.getTable().getItems()) {
			Object[] obj = (Object[]) ti.getData();
			obj[0] = true;

			ti.setImage(0, img);
		}
		tvTableColumns.getTable().getColumn(0).setImage(img);
	}

	/**
	 * Select all columns only all columns are not selected
	 * 
	 */
	private void selectAllFSs() {
		boolean flag = false;
		for (TableItem ti : tvTableFKs.getTable().getItems()) {
			Object[] obj = (Object[]) ti.getData();
			if ((Boolean) obj[2]) {
				flag = true;
				break;
			}
		}
		if (flag) {
			return;
		}
		final Image img = CompositeUtils.getCheckImage(true);
		for (TableItem ti : tvTableFKs.getTable().getItems()) {
			Object[] obj = (Object[]) ti.getData();
			obj[2] = true;
			obj[3] = true;
			ti.setImage(2, img);
			ti.setImage(3, img);
		}
		tvTableFKs.getTable().getColumn(2).setImage(img);
		tvTableFKs.getTable().getColumn(3).setImage(img);
	}

	/**
	 * Select all columns only all columns are not selected
	 * 
	 */
	private void selectAllIndexes() {
		boolean flag = false;
		for (TableItem ti : tvTableIndexes.getTable().getItems()) {
			Object[] obj = (Object[]) ti.getData();
			if ((Boolean) obj[2]) {
				flag = true;
				break;
			}
		}
		if (flag) {
			return;
		}
		final Image img = CompositeUtils.getCheckImage(true);
		for (TableItem ti : tvTableIndexes.getTable().getItems()) {
			Object[] obj = (Object[]) ti.getData();
			obj[2] = true;
			obj[3] = true;
			ti.setImage(2, img);
			ti.setImage(3, img);
		}
		tvTableIndexes.getTable().getColumn(2).setImage(img);
		tvTableIndexes.getTable().getColumn(3).setImage(img);
	}

	/**
	 * Set page controls' status
	 * 
	 */
	private void setPageControlStatus() {
		btnReplace.setEnabled(btnCreate.getSelection());
		btnReplace.setSelection(btnCreate.getSelection());
		btnCreatePK.setEnabled(btnCreate.getSelection());
		btnCreatePK.setSelection(btnCreate.getSelection());
		btnReuseOID.setEnabled(btnCreate.getSelection());

		tvTableFKs.getTable().setEnabled(btnCreate.getSelection());
		tvTableIndexes.getTable().setEnabled(btnCreate.getSelection());
		partitionMappingView.setEditable(btnCreate.getSelection());

		final boolean status = btnCreate.getSelection() || btnMigrateData.getSelection();
		txtTarget.setEditable(status);
		tvTableColumns.getTable().setEnabled(status);

		txtUserSQL.setEditable(status);
		txtUserSQL2.setEditable(status);

		btnReplace.setSelection(setc.isReplace() && btnCreate.getSelection());
		btnCreatePK.setSelection(setc.isCreatePK() && btnCreate.getSelection());
		setCreatePKStatus(btnCreatePK.getSelection());

		//Set tab 6's status
		setExpOptStatus(setc.isEnableExpOpt(), setc.isStartFromTargetMax());
	}

	/**
	 * setExpOptStatus
	 * 
	 * @param eo if enable exporting optimization
	 * @param sft if start from target value.
	 */
	private void setExpOptStatus(boolean eo, boolean sft) {
		if (!grpExportingOpt.isVisible()) {
			return;
		}
		Table st = config.getSrcTableSchema(setc.getOwner(), setc.getName());
		if (st == null) {
			return;
		}
		final PK pk = st.getPk();
		int pkColCount = (pk == null) ? 0 : pk.getPkColumns().size();
		boolean flag = btnMigrateData.getSelection() && pkColCount > 0;
		btnEnableExpOpt.setEnabled(flag);
		btnEnableExpOpt.setSelection(eo);

		//Only single-column primary key supports this option.
		btnAutoStartByTargetMax.setEnabled(flag && config.targetIsOnline()
				&& btnEnableExpOpt.getSelection() && !btnReplace.getSelection() && pkColCount == 1);
		btnAutoStartByTargetMax.setSelection(sft && config.targetIsOnline());
	}

	/**
	 * Set Exporting Optimization part's visible.
	 * 
	 */
	private void setExportOptVisible() {
		final boolean supportExpOpt = isSupportExpOpt();
		grpExportingOpt.setVisible(supportExpOpt);
		GridData gd = (GridData) grpExportingOpt.getLayoutData();
		gd.exclude = !supportExpOpt;
		grpExportingOpt.getParent().layout();
	}

	/**
	 * Bring this view onto the UI top
	 */
	public void show() {
		CompositeUtils.hideOrShowComposite(tabTableDetailFolder, false);
		setExportOptVisible();
	}

	/**
	 * Display input model object
	 * 
	 * @param obj to be displayed.
	 */
	public void showData(Object obj) {
		if (!(obj instanceof ICUBRIDNode)) {
			throw new IllegalArgumentException("Error node in table mapping.");
		}
		ICUBRIDNode tempNode = (ICUBRIDNode) obj;
		while (!(tempNode instanceof TableNode)) {
			tempNode = tempNode.getParent();
		}
		super.showData(obj);
		partitionMappingView.setMigrationConfig(config);

		Integer idx = NODE2PAGEMAP.get(obj.getClass().getName());
		if (idx == null) {
			if (tabTableDetailFolder.getSelectionIndex() < 0) {
				tabTableDetailFolder.setSelection(0);
			}
		} else {
			tabTableDetailFolder.setSelection(idx);
		}

		Table srcTable = ((TableNode) tempNode).getTable();
		if (srcTable == null) {
			throw new RuntimeException("Error node in table mapping.");
		}
		setc = config.getExpEntryTableCfg(srcTable.getOwner(), srcTable.getName());
		if (setc == null) {
			throw new RuntimeException("Error node in table mapping.");
		}
		fillData(setc);
		setPageControlStatus();

	}

	/**
	 * Only Online CUBRID source database can support exporting optimization.
	 * 
	 * @return true if source is CUBRID online datbase.
	 */
	private boolean isSupportExpOpt() {
		return config.sourceIsOnline() && config.getSourceDBType().equals(DatabaseType.CUBRID);
	}

	/**
	 * Refresh PK tab
	 * 
	 * @param pkStatus boolean
	 */
	private void setCreatePKStatus(boolean pkStatus) {
		btnMvLeft.setEnabled(pkStatus);
		btnMvRight.setEnabled(pkStatus);
		allColumnList.setEnabled(pkStatus);
		pkColumnList.setEnabled(pkStatus);
		txtTarPKName.setEnabled(pkStatus);
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
		boolean hasColumnChecked = false;
		for (int i = 0; i < tvTableColumns.getTable().getItemCount(); i++) {
			Object[] obj = (Object[]) tvTableColumns.getElementAt(i);
			String name = obj[3].toString().toLowerCase(Locale.US);
			if (!MigrationCfgUtils.verifyTargetDBObjName(name)) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidColumnName,
						setc.getTarget(), name), null, null);
			}
			if (names.indexOf(name) >= 0) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrDupColumnName,
						setc.getTarget(), name), null, null);
			}
			names.add(name);

			String dataType = obj[4].toString();
			if (!dataTypeHelper.isValidDatatype(dataType)) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidDataType,
						setc.getTarget(), name), null, null);
			}

			hasColumnChecked = hasColumnChecked || (Boolean) obj[0];
		}
		if (!hasColumnChecked) {
			return new VerifyResultMessages(Messages.bind(Messages.msgErrNoColumnSelected,
					setc.getName()), null, null);
		}
		names.clear();
		for (int i = 0; i < tvTableFKs.getTable().getItemCount(); i++) {
			Object[] obj = (Object[]) tvTableFKs.getElementAt(i);
			String name = obj[1].toString().toLowerCase(Locale.US);
			if (names.indexOf(name) >= 0) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrDupFKName,
						setc.getTarget(), name), null, null);
			}
			names.add(name);
		}
		for (int i = 0; i < tvTableIndexes.getTable().getItemCount(); i++) {
			Object[] obj = (Object[]) tvTableIndexes.getElementAt(i);
			String name = obj[1].toString().toLowerCase(Locale.US);
			if (names.indexOf(name) >= 0) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrDupIndexName,
						setc.getTarget(), name), null, null);
			}
			names.add(name);
		}
		return new VerifyResultMessages();
	}

	/**
	 * Move column in the list.
	 * 
	 * @param isUp is direction is up.
	 */
	@SuppressWarnings("unchecked")
	private void moveColumn(boolean isUp) {
		if (tvTableColumns.getSelection().isEmpty()) {
			return;
		}
		int idx = tvTableColumns.getTable().getSelectionIndex();
		int iedge;
		int idx2;
		if (isUp) {
			iedge = 0;
			idx2 = idx - 1;
		} else {
			iedge = tvTableColumns.getTable().getItemCount() - 1;
			idx2 = idx + 1;
		}
		if (idx == iedge) {
			return;
		}
		if (save().hasError()) {
			return;
		}
		List<SourceColumnConfig> columns = (List<SourceColumnConfig>) tvTableColumns.getInput();
		SourceColumnConfig scc = columns.remove(idx);

		columns.add(idx2, scc);
		tvTableColumns.refresh();
		tvTableColumns.setSelection(new StructuredSelection(
				tvTableColumns.getTable().getItem(idx2).getData()));
	}
}
