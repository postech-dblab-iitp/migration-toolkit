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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.action.IMenuListener;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IInputValidator;
import org.eclipse.jface.dialogs.InputDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.ICellEditorValidator;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.ObjectArrayRowCellModifier;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.CheckboxCellEditorFactory;
import com.cubrid.common.ui.swt.table.celleditor.TextCellEditorFactory;
import com.cubrid.common.ui.swt.table.listener.CheckBoxColumnSelectionListener;
import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.listener.ISQLTableChangedListener;
import com.cubrid.cubridmigration.core.io.SQLParser;
import com.cubrid.cubridmigration.core.io.SQLParser.ISQLParsingCallback;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.tableviewer.cell.validator.CUBRIDNameValidator;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.dialog.SQLEditorDialog;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * Edit target view name and SQL statement
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-26 created by Kevin Cao
 */
public class SQLTableManageView extends
		AbstractMappingView {

	private Composite container;
	private TableViewer tvSQL;
	private Button btnAddSQL;
	private Button btnEditSQL;
	private Button btnRemoveSQL;

	private final IAction actNew = new Action() {

		public void run() {
			save();
			List<SourceSQLTableConfig> sstcs = SQLEditorDialog.addSQL(config);
			if (CollectionUtils.isEmpty(sstcs)) {
				return;
			}
			showData(getModel());
			tvSQL.getTable().select(tvSQL.getTable().getItemCount() - 1);
			if (listener == null) {
				return;
			}
			for (SourceSQLTableConfig sstc : sstcs) {
				listener.onAddSQL(sstc);
			}
		}
	};
	private final IAction actEdit = new Action() {

		public void run() {
			save();
			editSQLTable();
		}
	};
	private final IAction actDelete = new Action() {

		public void run() {
			if (tvSQL.getSelection().isEmpty()) {
				MessageDialog.openError(null, Messages.msgError, Messages.errNoSQLTableSelected);
				return;
			}
			if (!MessageDialog.openConfirm(null, Messages.msgTitleDeleteSQL,
					Messages.msgCfmDeleteSQL)) {
				return;
			}
			save();
			final IStructuredSelection selection = (IStructuredSelection) tvSQL.getSelection();
			final Object[] selectObjs = selection.toArray();
			for (Object so : selectObjs) {
				Object[] obj = (Object[]) so;
				SourceSQLTableConfig sstc = (SourceSQLTableConfig) obj[obj.length - 1];
				config.rmSQLConfig(sstc);
				if (listener == null) {
					return;
				}
				listener.onRemoveSQL(sstc);
			}
			showData(getModel());
		}
	};
	private final IAction actTarget = new Action() {

		public void run() {
			if (tvSQL.getSelection().isEmpty()) {
				MessageDialog.openError(null, Messages.msgError, Messages.errNoSQLTableSelected);
				return;
			}
			final IStructuredSelection selection = (IStructuredSelection) tvSQL.getSelection();
			Object[] first = (Object[]) selection.getFirstElement();
			InputDialog dialog = new InputDialog(Display.getDefault().getActiveShell(),
					Messages.lblChangeTargetTable, Messages.lblTarget, (String) first[2],
					new IInputValidator() {

						public String isValid(String newText) {
							if (MigrationCfgUtils.verifyTargetDBObjName(newText)) {
								return null;
							}
							if (StringUtils.isBlank(newText)) {
								return Messages.targetDBPageOfflineErrMsg2;
							}
							return Messages.bind(Messages.msgErrInvalidTableName, newText);
						}
					});
			if (dialog.open() != Dialog.OK) {
				return;
			}
			String newTarget = dialog.getValue().trim().toLowerCase(Locale.US);
			save();

			final Object[] selectObjs = selection.toArray();
			for (Object so : selectObjs) {
				Object[] obj = (Object[]) so;
				SourceSQLTableConfig sstc = (SourceSQLTableConfig) obj[obj.length - 1];
				config.changeTarget(sstc, newTarget);
				//Select items
				if (listener == null) {
					return;
				}
				listener.onEditSQL(sstc);
			}
			showData(getModel());
		}
	};

	private ISQLTableChangedListener listener;
	//private Button btnExport2Xls;
	private Button btnImportFromXls;

	public SQLTableManageView(Composite parent) {
		super(parent);
	}

	/**
	 * Add SQL changed listener
	 * 
	 * @param listener ISQLTableChangedListener
	 */
	public void addSQLChangedListener(ISQLTableChangedListener listener) {
		this.listener = listener;
	}

	/**
	 * Create management buttons
	 * 
	 * @param parent Composite
	 */
	private void createButtons(Composite parent) {
		Composite btnContainer = new Composite(parent, SWT.NONE);
		btnContainer.setLayoutData(new GridData(SWT.RIGHT, SWT.TOP, false, true));
		btnContainer.setLayout(new GridLayout());

		btnAddSQL = new Button(btnContainer, SWT.NONE);
		btnAddSQL.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnAddSQL.setText(Messages.lblAddSQL);
		btnAddSQL.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				actNew.run();
			}
		});

		btnEditSQL = new Button(btnContainer, SWT.NONE);
		btnEditSQL.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnEditSQL.setText(Messages.lblEditSQL);
		btnEditSQL.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent ev) {
				actEdit.run();
			}
		});

		btnRemoveSQL = new Button(btnContainer, SWT.NONE);
		btnRemoveSQL.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnRemoveSQL.setText(Messages.lblDeleteSQL);
		btnRemoveSQL.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent ev) {
				actDelete.run();
			}
		});

		btnImportFromXls = new Button(btnContainer, SWT.NONE);
		btnImportFromXls.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnImportFromXls.setText(Messages.btnImportSQL);
		btnImportFromXls.setToolTipText(Messages.ttImportSQL);
		btnImportFromXls.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent ev) {
				save();
				FileDialog dialog = new FileDialog(Display.getDefault().getActiveShell(), SWT.OPEN);
				dialog.setFilterExtensions(new String[] {"*.sql", "*.xls", "*.xlsx"});
				String fileName = dialog.open();
				importSQL(fileName);
			}
		});

		//		btnExport2Xls = new Button(btnContainer, SWT.NONE);
		//		btnExport2Xls.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true,
		//				false));
		//		btnExport2Xls.setVisible(false);
		//		btnExport2Xls.setText("Export");
		//		btnExport2Xls.setToolTipText("Export SQL tables to an XLS/XLSX file.");
		//		btnExport2Xls.addSelectionListener(new SelectionAdapter() {
		//			public void widgetSelected(SelectionEvent ev) {
		//			}
		//		});
	}

	/**
	 * Create controls
	 * 
	 * @param parent of the controls
	 */
	protected void createControl(Composite parent) {
		container = new Composite(parent, SWT.NONE);
		GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
		gd.exclude = true;
		container.setLayoutData(gd);
		container.setVisible(false);
		container.setLayout(new GridLayout(2, false));

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.tabTitleName, Messages.tabTitleSQL,
				Messages.tabTitleTargetTable, Messages.tabTitleData, Messages.lblCreate,
				Messages.lblReplace});
		tvBuilder.setColumnWidths(new int[] {130, 400, 140, 70, 80, 90});
		//tvBuilder.setColumnStyles(columnStyles);
		//tvBuilder.setColumnImages(columnImageFiles);
		tvBuilder.setColumnTooltips(new String[] {Messages.tabTitleName, Messages.tabTitleSQL,
				Messages.tabTitleTargetTableDes, Messages.tabTitleDataSQLDes,
				Messages.lblCreateDes, Messages.lblReplaceDes});
		tvBuilder.setCellEditorClasses(new CellEditorFactory[] {new TextCellEditorFactory(), null,
				new TextCellEditorFactory(), new CheckboxCellEditorFactory(),
				new CheckboxCellEditorFactory(), new CheckboxCellEditorFactory()});
		tvBuilder.setCellValidators(new ICellEditorValidator[] {new CUBRIDNameValidator(), null,
				new CUBRIDNameValidator(), null, null, null});
		tvBuilder.setContentProvider(new StructuredContentProviderAdaptor() {

			@SuppressWarnings("unchecked")
			public Object[] getElements(Object inputElement) {
				List<Object[]> result = new ArrayList<Object[]>();
				for (SourceSQLTableConfig sstc : (List<SourceSQLTableConfig>) inputElement) {
					result.add(new Object[] {sstc.getName(), sstc.getSql(), sstc.getTarget(),
							sstc.isMigrateData(), sstc.isCreateNewTable(), sstc.isReplace(), sstc});

				}
				return super.getElements(result);
			}
		});
		//tvBuilder.setLabelProvider();
		ObjectArrayRowCellModifier cellModifier = new ObjectArrayRowCellModifier() {

			protected void modify(TableItem ti, Object[] element, int columnIdx, Object value) {
				if (value instanceof Boolean) {
					boolean bv = (Boolean) value;
					if (columnIdx == 4) {
						super.modify(ti, element, columnIdx + 1, value);
						updateColumnImage(value, ti, columnIdx + 1);
					} else if (bv && columnIdx == 5) {
						super.modify(ti, element, columnIdx - 1, value);
						updateColumnImage(value, ti, columnIdx - 1);
					}
				}
				super.modify(ti, element, columnIdx, value);
			}
		};
		tvBuilder.setCellModifier(cellModifier);
		tvBuilder.setTableCursorSupported(true);
		tvSQL = tvBuilder.buildTableViewer(container, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI);

		final SelectionListener[] selectionListeners = new SelectionListener[] {null, null, null,
				new CheckBoxColumnSelectionListener(),
				new CheckBoxColumnSelectionListener(new int[] {5}, true, true),
				new CheckBoxColumnSelectionListener(new int[] {4}, true, false)};

		CompositeUtils.setTableColumnSelectionListener(tvSQL, selectionListeners);
		tvSQL.addDoubleClickListener(new IDoubleClickListener() {

			public void doubleClick(DoubleClickEvent event) {
				save();
				editSQLTable();
			}
		});

		MenuManager mm = new MenuManager();
		mm.addMenuListener(new IMenuListener() {
			public void menuAboutToShow(IMenuManager manager) {
				manager.removeAll();
				boolean flag1 = config.sourceIsOnline() && !wizardStatus.isSourceOfflineMode();
				boolean flag = flag1 && !tvSQL.getSelection().isEmpty();
				actNew.setEnabled(flag1);
				actEdit.setEnabled(flag);
				actDelete.setEnabled(!tvSQL.getSelection().isEmpty());
				actTarget.setEnabled(flag);

				actNew.setText(Messages.lblAddSQL);
				actEdit.setText(Messages.lblEditSQL);
				actDelete.setText(Messages.lblDeleteSQL);
				actTarget.setText(Messages.lblChangeTargetTable);
				manager.add(actNew);
				manager.add(actEdit);
				manager.add(actDelete);
				manager.add(new Separator());
				manager.add(actTarget);
			}
		});
		tvSQL.getTable().setMenu(mm.createContextMenu(tvSQL.getTable()));
		createButtons(container);
	}

	/**
	 * Edit SQL table
	 * 
	 */
	private void editSQLTable() {
		final ISelection selection = tvSQL.getSelection();
		if (selection.isEmpty()) {
			MessageDialog.openError(null, Messages.msgError, Messages.errNoSQLTableSelected);
			return;
		}
		Object[] obj = (Object[]) ((IStructuredSelection) selection).getFirstElement();
		SourceSQLTableConfig sstc = (SourceSQLTableConfig) obj[obj.length - 1];
		if (!SQLEditorDialog.editSQL(config, sstc)) {
			return;
		}
		TableItem item = tvSQL.getTable().getSelection()[0];
		item.setText(1, sstc.getSql()); // column 1 : SQL
		if (listener == null) {
			return;
		}
		listener.onEditSQL(sstc);
	}

	/**
	 * Hide the view.
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(container, true);

	}

	/**
	 * Import SQL from XLS/XLSX file
	 * 
	 * @param fileName XLS/XLSX
	 */
	private void importSQL(final String fileName) {
		if (StringUtils.isBlank(fileName)) {
			return;
		}
		final List<SourceSQLTableConfig> newsstcs = new ArrayList<SourceSQLTableConfig>();
		final List<String> errorSQLs = new ArrayList<String>();
		final IRunnableWithProgress mtd = new IRunnableWithProgress() {

			public void run(IProgressMonitor monitor) throws InvocationTargetException,
					InterruptedException {
				monitor.beginTask("", IProgressMonitor.UNKNOWN);
				final List<String[]> data = getSQLList(fileName);
				if (CollectionUtils.isEmpty(data)) {
					monitor.done();
					return;
				}
				monitor.beginTask("", data.size());
				int i = 0;
				int progress = 0;
				List<String> sqlids = new ArrayList<String>();
				List<String> sqls = new ArrayList<String>();
				for (String[] dd : data) {
					if (monitor.isCanceled()) {
						break;
					}
					monitor.worked(progress++);
					String sql = null;
					String sqlID = null;
					String target = null;
					if (dd.length == 1) {
						sql = dd[0].trim();
					} else if (dd.length == 2) {
						sqlID = dd[0].trim();
						sql = dd[1].trim();
						target = sqlID;
					} else if (dd.length >= 3) {
						sqlID = dd[0].trim();
						sql = dd[1].trim();
						target = dd[2].trim();
					}
					//Validate input data
					if (StringUtils.isEmpty(sql)) {
						continue;
					}
					if (sql.endsWith(";")) {
						sql = sql.substring(0, sql.length() - 1);
					}
					if (config.getExpSQLCfgBySql(sql) != null) {
						continue;
					}
					if (StringUtils.isBlank(sqlID)) {
						i++;
						String newName = "SQL" + i;
						String tableName = "table" + i;
						while (config.getExpSQLCfgByName(newName) != null) {
							i++;
							newName = "SQL" + i;
							tableName = "table" + i;
						}
						sqlID = newName;
						target = tableName;
					} else if (config.getExpSQLCfgByName(sqlID) != null || sqlids.contains(sqlID)
							|| sqls.contains(sql)) {
						//Checking duplicated id and sql
						continue;
					}
					sqlids.add(sqlID);
					sqls.add(sql);
					if (StringUtils.isBlank(target)) {
						target = sqlID.toLowerCase(Locale.US);
					}
					try {
						config.validateExpSQLConfig(sql);
					} catch (Exception ex) {
						errorSQLs.add(sql);
						continue;
					}
					//Create new SourceSQLTableConfig object
					SourceSQLTableConfig newSTC = new SourceSQLTableConfig();
					newSTC.setName(sqlID);
					newSTC.setTarget(target);
					newSTC.setSql(sql);
					newSTC.setCreateNewTable(false);
					newSTC.setReplace(false);
					newSTC.setMigrateData(true);
					newsstcs.add(newSTC);
				}
				for (SourceSQLTableConfig sstc : newsstcs) {
					config.addExpSQLTableCfgWithST(sstc);
				}
				monitor.done();
			}
		};
		CompositeUtils.runMethodInProgressBar(true, true, mtd);
		//Show result message
		if (CollectionUtils.isEmpty(newsstcs)) {
			MessageDialog.openInformation(Display.getDefault().getActiveShell(),
					Messages.msgTitleImportSQL, Messages.msgNoSQLImported);
		} else {
			showData(getModel());
			tvSQL.getTable().select(tvSQL.getTable().getItemCount() - 1);
			if (listener == null) {
				return;
			}
			for (SourceSQLTableConfig sstc : newsstcs) {
				listener.onAddSQL(sstc);
			}
			if (errorSQLs.isEmpty()) {
				MessageDialog.openInformation(Display.getDefault().getActiveShell(),
						Messages.msgTitleImportSQL,
						Messages.bind(Messages.msgSQLImported, newsstcs.size()));
			} else {
				MessageDialog.openInformation(Display.getDefault().getActiveShell(),
						Messages.msgTitleImportSQL,
						Messages.bind(Messages.msgSQLImportedWithError, newsstcs.size()));
			}
		}
	}

	/**
	 * Save
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		//Check
		List<String> names = new ArrayList<String>();
		for (TableItem ti : tvSQL.getTable().getItems()) {
			Object[] obj = (Object[]) ti.getData();
			final String nm = (String) obj[0];
			if (!MigrationCfgUtils.verifyTargetDBObjName(nm)) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidTableName, nm),
						null, null);
			}
			if (names.indexOf(nm) >= 0) {
				return new VerifyResultMessages(
						Messages.bind(Messages.msgErrSQLNameDuplicated, nm), null, null);
			}
			names.add(nm);
		}
		//Save
		for (TableItem ti : tvSQL.getTable().getItems()) {
			Object[] obj = (Object[]) ti.getData();
			SourceSQLTableConfig sstc = (SourceSQLTableConfig) obj[obj.length - 1];
			config.replaceSQL(sstc, (String) obj[0], sstc.getSql());
			config.changeTarget(sstc, (String) obj[2]);
			sstc.setMigrateData((Boolean) obj[3]);
			sstc.setCreateNewTable((Boolean) obj[4]);
			sstc.setReplace((Boolean) obj[5]);
			if (listener != null) {
				listener.onEditSQL(sstc);
			}
		}
		return super.save();
	}

	/**
	 * Bring the view onto the top.
	 * 
	 */
	public void show() {
		CompositeUtils.hideOrShowComposite(container, false);

	}

	/**
	 * Show source view DDL and target view DDL
	 * 
	 * @param obj should be a ViewNode
	 */
	public void showData(Object obj) {
		boolean flag = config.sourceIsOnline() && !wizardStatus.isSourceOfflineMode();
		btnAddSQL.setEnabled(flag);
		btnEditSQL.setEnabled(flag);
		btnRemoveSQL.setEnabled(config.sourceIsOnline());
		btnImportFromXls.setEnabled(flag);
		tvSQL.getTable().setEnabled(config.sourceIsOnline());
		tvSQL.setInput(config.getExpSQLCfg());
		CompositeUtils.initTableViewerCheckColumnImage(tvSQL);
	}

	/**
	 * Get SQL list from (.sql/.xls/.xlsx) file.
	 * 
	 * @param fileName to be read
	 * @return SQLs or SQLs with source name and target name
	 */
	private List<String[]> getSQLList(final String fileName) {
		try {
			final List<String[]> data = new ArrayList<String[]>();
			if (StringUtils.lowerCase(fileName).endsWith(".sql")) {

				final ISQLParsingCallback callBack = new ISQLParsingCallback() {

					public void executeSQLs(List<String> sqlList, long size) {
						for (String sql : sqlList) {
							data.add(new String[] {sql});
						}
					}

					public boolean isCommitNow(int sqlsSize) {
						return sqlsSize >= 100;
					}

				};
				SQLParser.executeSQLFile(fileName, "utf-8", 100, callBack);
			} else {
				List<String[]> tmp = CUBRIDIOUtils.readDataFromExcel(fileName);
				data.addAll(tmp);
			}
			return data;
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
