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
package com.cubrid.cubridmigration.ui.wizard.page;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.viewers.ICellEditorValidator;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TableViewerColumn;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.Resources;
import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.ObjectArrayRowCellModifier;
import com.cubrid.common.ui.swt.table.ObjectArrayRowTableLabelProvider;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.CheckboxCellEditorFactory;
import com.cubrid.common.ui.swt.table.celleditor.TextCellEditorFactory;
import com.cubrid.common.ui.swt.table.listener.CheckBoxColumnSelectionListener;
import com.cubrid.cubridmigration.core.common.TextFileUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.tableviewer.cell.validator.CUBRIDDataTypeValidator;
import com.cubrid.cubridmigration.ui.common.tableviewer.cell.validator.CUBRIDNameValidator;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;
import com.cubrid.cubridmigration.ui.wizard.dialog.CSVImportSettingDialog;
import com.cubrid.cubridmigration.ui.wizard.dialog.CUBRIDHadoopFSExplorer;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * 
 * New wizard step 2. choose CSV files to be executed.
 * 
 * @author caoyilin
 * @version 1.0 - 2013-03-13
 */
public class CSVSelectPage extends
		MigrationWizardPage {

	/**
	 * 
	 * Add SQL file(s) in the table view.
	 * 
	 * @author caoyilin
	 * 
	 */
	private class AddAction extends
			Action {
		public AddAction() {
			setText(Messages.btnAdd);
		}

		/**
		 * run
		 */
		public void run() {
			FileDialog dialog = new FileDialog(getShell(), SWT.OPEN | SWT.MULTI);
			dialog.setFilterExtensions(new String[] { "*.csv" });
			dialog.setFilterNames(new String[] { "*.csv" });
			if (dialog.open() == null) {
				return;
			}
			//int count = 0;
			final MigrationWizard wizard = getMigrationWizard();
			MigrationConfiguration config = wizard.getMigrationConfig();
			final Schema tschema = wizard.getTargetCatalog().getSchemas().get(0);
			for (String file : dialog.getFileNames()) {
				String fullName = dialog.getFilterPath() + File.separator + file;
				if (new File(fullName).isFile()) {
					config.addCSVFile(fullName, tschema);
					//count++;
				}
			}
			refresh();
		}
	}

	/**
	 * 
	 * Add SQL file(s) in the table view.
	 * 
	 * @author caoyilin
	 * 
	 */
	private class AddFromHDFSAction extends
			Action {
		public AddFromHDFSAction() {
			setText(Messages.btnAddFromHDFS);
		}

		/**
		 * run
		 */
		public void run() {
			CUBRIDHadoopFSExplorer dialog = new CUBRIDHadoopFSExplorer(getShell());
			//			CUBRIDHadoopFileDialog dialog = new CUBRIDHadoopFileDialog(
			//					getShell());
			if (dialog.open() != IDialogConstants.OK_ID) {
				return;
			}

			final MigrationWizard wizard = getMigrationWizard();
			MigrationConfiguration config = wizard.getMigrationConfig();
			final Schema tschema = wizard.getTargetCatalog().getSchemas().get(0);
			for (String hdfs : dialog.getHdfsFiles()) {
				config.addCSVFile(hdfs, tschema);
			}
			refresh();
		}
	}

	/**
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private final class CSVColumnsTableContentProvider extends
			StructuredContentProviderAdaptor {
		private final MigrationWizard wizard;

		private CSVColumnsTableContentProvider(MigrationWizard wizard) {
			this.wizard = wizard;
		}

		/**
		 * 
		 * Retrieves elements
		 * 
		 * @param inputElement object
		 * @return Object[]
		 */
		public Object[] getElements(Object inputElement) {
			List<Object[]> data = new ArrayList<Object[]>();
			SourceCSVConfig sc = (SourceCSVConfig) inputElement;
			if (sc != null) {
				//Add the SourceConfig to the end of the object array.
				List<SourceCSVColumnConfig> columns = sc.getColumnConfigs();
				Table tt = wizard.getMigrationConfig().getTargetTableSchema(sc.getTarget());
				for (int i = 0; i < columns.size(); i++) {
					SourceCSVColumnConfig col = columns.get(i);
					Column tc = tt == null ? null : tt.getColumnByName(col.getTarget());
					data.add(new Object[] { col.isCreate(), i + 1, col.getName(), col.getTarget(),
							tc == null ? "string" : tc.getShownDataType(), col });
				}
			}
			return super.getElements(data);
		}
	}

	/**
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private final class CSVFilesTableViewerCellModifier extends
			ObjectArrayRowCellModifier {

		/**
		 * Modify cell element and table item
		 * 
		 * @param ti TableItem
		 * @param element Object[]
		 * @param columnIdx int
		 * @param value Object
		 * 
		 */
		protected void modify(TableItem ti, Object[] element, int columnIdx, Object value) {
			if (columnIdx == 4) {
				final VerifyResultMessages save = detailView.save();
				if (save.hasError()) {
					setErrorMessage(save.getErrorMessage());
					return;
				} else {
					setErrorMessage(null);
				}
			}
			Object[] obj = (Object[]) ti.getData();
			if (value instanceof Boolean) {
				boolean bv = (Boolean) value;
				if (columnIdx == 2) {
					obj[3] = bv;
					ti.setImage(3, CompositeUtils.getCheckImage(bv));
					updateColumnImage(value, ti, columnIdx + 1);
				} else if (columnIdx == 3) {
					obj[2] = (Boolean) obj[2] || bv;
					ti.setImage(2, CompositeUtils.getCheckImage((Boolean) obj[2]));
					updateColumnImage(value, ti, columnIdx - 1);
				}
			}
			super.modify(ti, element, columnIdx, value);
			//Save to model object and refresh right part
			SourceCSVConfig scc = (SourceCSVConfig) obj[obj.length - 1];
			if (columnIdx == 1) {
				final String targetName = (String) obj[1];
				changeCSVTarget(scc, targetName);
			} else if (columnIdx == 2 || columnIdx == 3) {
				scc.setCreate((Boolean) obj[2]);
				scc.setReplace((Boolean) obj[3]);
			} else if (columnIdx == 4) {
				MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
				scc.setImportFirstRow((Boolean) obj[4]);
				if (MessageDialog.openQuestion(getShell(), "", Messages.msgRenameColumns)) {
					Table tt = config.getTargetTableSchema(scc.getTarget());
					for (SourceCSVColumnConfig sccc : scc.getColumnConfigs()) {
						Column tcol = tt.getColumnByName(sccc.getTarget());
						config.changeCSVTarget(sccc, sccc.getName().toLowerCase(Locale.US), tcol);
					}
				}
				detailView.refresh();
			}
		}
	}

	/**
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private static final class CSVFilesTableViewerContentProvider extends
			StructuredContentProviderAdaptor {

		/**
		 * @param inputElement elements
		 * @return objects display by viewer
		 */
		public Object[] getElements(Object inputElement) {
			List<Object> data = transformInputToObjectArrayList(inputElement);
			return super.getElements(data);
		}

		/**
		 * @param inputElement input set by setInput
		 * @return object list
		 */
		@SuppressWarnings("unchecked")
		private List<Object> transformInputToObjectArrayList(Object inputElement) {
			List<Object> data = new ArrayList<Object>();
			for (SourceCSVConfig sc : (List<SourceCSVConfig>) inputElement) {
				//Add the SourceConfig to the end of the object array.
				data.add(new Object[] { sc.getName(), sc.getTarget(), sc.isCreate(),
						sc.isReplace(), sc.isImportFirstRow(), sc });
			}
			return data;
		}
	}

	/**
	 * 
	 * Delete SQL file(s) in the table view.
	 * 
	 * @author caoyilin
	 * 
	 */
	private class DeleteAction extends
			Action {
		public DeleteAction() {
			setText(Messages.removeButtonLabel);
		}

		/**
		 * run
		 */
		public void run() {
			TableItem[] tis = tvFiles.getTable().getSelection();
			if (tis.length == 0) {
				return;
			}
			MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
			for (TableItem ti : tis) {
				config.removeCSVFile(ti.getText());
			}
			refresh();
		}
	}

	/**
	 * CSVMappingDetailView provides columns mapping, parsing preview and text
	 * preview.
	 * 
	 * @author Kevin Cao
	 */
	private class MappingDetailView {

		private CTabFolder tabFolder;
		private TableViewer tvColumns;
		private TableViewer tvParsingPreview;
		private Text txtPreview;

		private SourceCSVConfig currentCfg;
		private StructuredContentProviderAdaptor parsingPreviewTableContentProvider;
		private ObjectArrayRowTableLabelProvider parsingPreviewTableLabelProvider;

		MappingDetailView(Composite parent) {
			tabFolder = new CTabFolder(parent, SWT.BORDER);
			tabFolder.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
			tabFolder.setSimple(false);
			tabFolder.setUnselectedCloseVisible(false);
			tabFolder.setTabHeight(24);

			createColumnMappingTab(tabFolder);
			createParsingResultTab(tabFolder);
			createTextTab(tabFolder);

			tabFolder.setSelection(0);
		}

		/**
		 * Create column mapping tab
		 * 
		 * @param parent CTabFolder
		 */
		private void createColumnMappingTab(CTabFolder parent) {
			Composite container = CompositeUtils.createTabItem(parent, Messages.tabTitleColumn,
					null);
			TableViewerBuilder tvBuilder = new TableViewerBuilder();
			tvBuilder.setColumnNames(new String[] { "", Messages.colColumnNO,
					Messages.colColumnName, Messages.colTargetColumn, Messages.colDataType });
			tvBuilder.setColumnWidths(new int[] { 50, 80, 150, 150, 150 });
			tvBuilder.setCellEditorClasses(new CellEditorFactory[] {
					new CheckboxCellEditorFactory(), null, null, new TextCellEditorFactory(),
					new TextCellEditorFactory() });
			tvBuilder.setCellValidators(new ICellEditorValidator[] { null, null, null,
					new CUBRIDNameValidator(), new CUBRIDDataTypeValidator() });
			tvBuilder.setCellModifier(new ObjectArrayRowCellModifier());

			final MigrationWizard wizard = getMigrationWizard();
			tvBuilder.setContentProvider(new CSVColumnsTableContentProvider(wizard));
			tvBuilder.setTableCursorSupported(true);
			tvColumns = tvBuilder.buildTableViewer(container, SWT.BORDER | SWT.FULL_SELECTION);

			CompositeUtils.setTableColumnSelectionListener(tvColumns, new SelectionListener[] {
					new CheckBoxColumnSelectionListener(new int[] { 0 }, true, true), null, null,
					null, null });
		}

		/**
		 * Create parsing result tab.
		 * 
		 * @param parent CTabFolder
		 */
		private void createParsingResultTab(CTabFolder parent) {
			Composite container = CompositeUtils.createTabItem(parent,
					Messages.tabTitleParsingPreview, null);
			Label lblHint = new Label(container, SWT.NONE);
			lblHint.setText(Messages.ttTop10Rows);
			lblHint.setForeground(Resources.getInstance().getColor(SWT.COLOR_BLUE));

			TableViewerBuilder tvBuilder = new TableViewerBuilder();
			tvBuilder.setColumnNames(new String[] { "NO" });
			parsingPreviewTableContentProvider = new StructuredContentProviderAdaptor();
			parsingPreviewTableLabelProvider = new ObjectArrayRowTableLabelProvider();
			tvBuilder.setContentProvider(parsingPreviewTableContentProvider);
			tvBuilder.setLabelProvider(parsingPreviewTableLabelProvider);
			tvParsingPreview = tvBuilder.buildTableViewer(container, SWT.BORDER
					| SWT.FULL_SELECTION);
		}

		/**
		 * Create text preview tab.
		 * 
		 * @param parent CTabFolder
		 */
		private void createTextTab(CTabFolder parent) {
			Composite container = CompositeUtils.createTabItem(parent,
					Messages.tabTitleTextPreview, null);
			Label lblHint = new Label(container, SWT.NONE);
			lblHint.setText(Messages.ttTop10Rows);
			lblHint.setForeground(Resources.getInstance().getColor(SWT.COLOR_BLUE));
			txtPreview = new Text(container, SWT.BORDER | SWT.MULTI | SWT.READ_ONLY);
			txtPreview.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		}

		/**
		 * Refresh
		 * 
		 */
		void refresh() {
			if (currentCfg == null) {
				return;
			}
			setInput(currentCfg);
		}

		/**
		 * Save data to migration configuration
		 * 
		 * @return VerifyResultMessages
		 */
		VerifyResultMessages save() {
			final VerifyResultMessages result = new VerifyResultMessages();
			List<String> targets = new ArrayList<String>();
			for (TableItem ti : tvColumns.getTable().getItems()) {
				final Object[] data = (Object[]) ti.getData();
				final Boolean expFlag = (Boolean) data[0];
				String tar = (String) data[3];
				if (expFlag) {
					if (targets.indexOf(tar) >= 0) {
						result.setErrorMessage(Messages.bind(Messages.msgErrDupColumnName,
								currentCfg.getTarget(), tar));
						return result;
					}
					targets.add(tar);
				}
			}
			if (targets.isEmpty()) {
				result.setErrorMessage(Messages.errNoColumnSelected);
				return result;
			}
			CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
			//no duplicated column name
			final MigrationConfiguration cfg = getMigrationWizard().getMigrationConfig();
			Table tt = cfg.getTargetTableSchema(currentCfg.getTarget());
			for (TableItem ti : tvColumns.getTable().getItems()) {
				final Object[] data = (Object[]) ti.getData();
				SourceCSVColumnConfig sccc = (SourceCSVColumnConfig) data[data.length - 1];
				sccc.setCreate((Boolean) data[0]);
				Column tcol = tt.getColumnByName(sccc.getTarget());
				cfg.changeCSVTarget(sccc, (String) data[3], tcol);
				//The target was changed, the column may be changed.
				tcol = tt.getColumnByName(sccc.getTarget());
				dataTypeHelper.setColumnDataType((String) data[4], tcol);
			}

			return result;
		}

		/**
		 * Set the enabled property of this view.
		 * 
		 * @param enabled boolean
		 */
		void setEnabled(boolean enabled) {
			if (!enabled) {
				tvColumns.setInput(null);
				tvParsingPreview.setInput(null);
				txtPreview.setText("");
			}
		}

		/**
		 * Set the SourceCSVConfig object to be shown in the view
		 * 
		 * @param sc SourceCSVConfig
		 */
		void setInput(SourceCSVConfig sc) {
			currentCfg = sc;
			tvColumns.setInput(sc);
			txtPreview.setText(TextFileUtils.readText(sc.getName(),
					getMigrationWizard().getMigrationConfig().getCsvSettings().getCharset(), 10));
			//
			final List<String[]> previewData = sc.getPreviewData();
			tvParsingPreview.setInput(new ArrayList<String[]>());
			if (previewData.isEmpty()) {
				return;
			}
			for (TableColumn tc : tvParsingPreview.getTable().getColumns()) {
				tc.dispose();
			}
			String[] row = previewData.get(0);
			for (int i = 1; i <= row.length; i++) {
				final TableViewerColumn tvColumn = new TableViewerColumn(tvParsingPreview, SWT.NONE);
				tvColumn.getColumn().setText("col" + i);
				tvColumn.getColumn().setWidth(80);
			}
			tvParsingPreview.setContentProvider(parsingPreviewTableContentProvider);
			tvParsingPreview.setLabelProvider(parsingPreviewTableLabelProvider);
			tvParsingPreview.setInput(previewData);
		}
	}

	private static final Logger LOG = LogUtil.getLogger(CSVSelectPage.class);

	private TableViewer tvFiles;
	private Button btnAdd;
	private Button btnAddHDFS;
	private Button btnDelete;

	private MappingDetailView detailView;

	private final AddAction addAction;
	private final DeleteAction deleteAction;
	private final AddFromHDFSAction addFromHDFFSAction;

	public CSVSelectPage(String pageName) {
		super(pageName);
		addAction = new AddAction();
		deleteAction = new DeleteAction();
		addFromHDFFSAction = new AddFromHDFSAction();
	}

	/**
	 * When migration wizard displayed current page.
	 * 
	 * @param event PageChangedEvent
	 */

	protected void afterShowCurrentPage(PageChangedEvent event) {
		try {
			setTitle(getMigrationWizard().getStepNoMsg(CSVSelectPage.this)
					+ Messages.titleWizardPageSelectSQL);
			setDescription(Messages.msgSelectSQL);
			if (isFirstVisible) {
				parseAll();
			}
			isFirstVisible = false;
			getShell().setMaximized(true);
			refresh();
		} catch (Exception ex) {
			LOG.error("", ex);
		}
	}

	/**
	 * Change CSV files target
	 * 
	 * @param scc configuration object
	 * @param targetName to be changed to
	 */
	private void changeCSVTarget(SourceCSVConfig scc, String targetName) {
		//If target table changed
		if (scc.getTarget().equalsIgnoreCase(targetName)) {
			return;
		}
		final MigrationWizard wizard = getMigrationWizard();
		final MigrationConfiguration cfg = wizard.getMigrationConfig();
		Schema schema = wizard.getTargetCatalog().getSchemas().get(0);
		Table tt = schema.getTableByName(targetName);
		boolean remapCols = tt != null
				&& MessageDialog.openConfirm(getShell(), Messages.msgRemapColumns,
						Messages.msgDoRemapColumns);
		cfg.changeCSVTarget(scc, targetName, schema, remapCols);
		detailView.refresh();
	}

	/**
	 * 
	 * Create buttons
	 * 
	 * @param group Composite
	 */
	private void createButtons(Composite group) {
		Composite buttonContainer = new Composite(group, SWT.NONE);
		GridData buttonGd = new GridData(SWT.LEFT, SWT.TOP, false, false);
		buttonContainer.setLayoutData(buttonGd);
		buttonContainer.setLayout(new GridLayout(4, false));

		btnAdd = new Button(buttonContainer, SWT.NONE);
		btnAdd.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnAdd.setText(Messages.btnAdd);
		btnAdd.setToolTipText(Messages.tipAddCSVFromLocal);
		btnAdd.setAlignment(SWT.CENTER);
		btnAdd.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				addAction.run();
			}
		});

		btnAddHDFS = new Button(buttonContainer, SWT.NONE);
		btnAddHDFS.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnAddHDFS.setText(Messages.btnAddFromHDFS);
		btnAddHDFS.setToolTipText(Messages.tipAddCSVFromHDFS);
		btnAddHDFS.setAlignment(SWT.CENTER);
		btnAddHDFS.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				addFromHDFFSAction.run();
			}
		});

		btnDelete = new Button(buttonContainer, SWT.NONE);
		btnDelete.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnDelete.setText(Messages.removeButtonLabel);
		btnDelete.setToolTipText(Messages.tipRemoveSQL);
		btnDelete.setAlignment(SWT.CENTER);
		btnDelete.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				deleteAction.run();
			}
		});

		Button btnSettings = new Button(buttonContainer, SWT.NONE);
		btnSettings.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnSettings.setText(Messages.btnSettings);
		btnSettings.setAlignment(SWT.CENTER);
		btnSettings.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				final MigrationConfiguration cfg = getMigrationWizard().getMigrationConfig();
				CSVImportSettingDialog dialog = new CSVImportSettingDialog(getShell(), cfg);
				if (dialog.open() == IDialogConstants.OK_ID
						&& !dialog.getSettings().equals(cfg.getCsvSettings())) {
					cfg.getCsvSettings().copyFrom(dialog.getSettings());
					parseAll();
					refresh();
				}
			}
		});
	}

	/**
	 * Create contents of the wizard
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		SashForm container = new SashForm(parent, SWT.HORIZONTAL);
		final GridLayout gridLayoutRoot = new GridLayout(2, false);
		container.setLayout(gridLayoutRoot);
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		setControl(container);

		Composite group = new Composite(container, SWT.BORDER);
		group.setLayout(new GridLayout());
		GridData groupGridData = new GridData(SWT.FILL, SWT.FILL, true, true);
		group.setLayoutData(groupGridData);

		createButtons(group);
		createTVFiles(group);

		detailView = new MappingDetailView(container);
	}

	/**
	 * Create table pop menus
	 * 
	 * @param tablev Table of viewer
	 */
	private void createTableViewMenus(TableViewer tablev) {
		MenuManager menuManager = new MenuManager();
		menuManager.add(addAction);
		menuManager.add(addFromHDFFSAction);
		menuManager.add(new Separator());
		menuManager.add(deleteAction);
		Menu menu = menuManager.createContextMenu(tablev.getTable());
		tablev.getTable().setMenu(menu);
	}

	/**
	 * JDBC source database configuration area
	 * 
	 * @param parent Composite
	 */
	protected void createTVFiles(Composite parent) {
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] { Messages.colCSVFile, Messages.colTargetTable,
				Messages.colCreate, Messages.colReplace, Messages.colImportFirstRow });
		tvBuilder.setTableCursorSupported(true);
		tvBuilder.setColumnWidths(new int[] { 250, 150, 80, 90, 130 });
		CheckboxCellEditorFactory checkboxCellEditorFactory = new CheckboxCellEditorFactory();
		tvBuilder.setCellEditorClasses(new CellEditorFactory[] { null, new TextCellEditorFactory(),
				checkboxCellEditorFactory, checkboxCellEditorFactory, checkboxCellEditorFactory });
		tvBuilder.setCellValidators(new ICellEditorValidator[] { null, new CUBRIDNameValidator(),
				null, null, null });
		tvBuilder.setContentProvider(new CSVFilesTableViewerContentProvider());
		tvBuilder.setCellModifier(new CSVFilesTableViewerCellModifier());

		tvFiles = tvBuilder.buildTableViewer(parent, SWT.BORDER | SWT.MULTI | SWT.FULL_SELECTION);

		final CheckBoxColumnSelectionListener cbListener3 = new CheckBoxColumnSelectionListener(
				new int[] {}, true, true) {

			protected void updateCells(TableColumn tc, boolean checkstatus) {
				boolean flag = MessageDialog.openQuestion(getShell(), "", Messages.msgRenameColumns);
				super.updateCells(tc, checkstatus);
				MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
				final List<SourceCSVConfig> csvConfigs = config.getCSVConfigs();
				for (SourceCSVConfig scc : csvConfigs) {
					scc.setImportFirstRow(checkstatus);
					if (!flag) {
						continue;
					}
					Table tt = config.getTargetTableSchema(scc.getTarget());
					if (tt == null) {
						continue;
					}
					for (SourceCSVColumnConfig sccc : scc.getColumnConfigs()) {
						Column tcol = tt.getColumnByName(sccc.getTarget());
						if (tcol == null) {
							continue;
						}
						config.changeCSVTarget(sccc, sccc.getName().toLowerCase(Locale.US), tcol);
					}
				}
				detailView.refresh();
			}

			public void widgetSelected(SelectionEvent ex) {
				final VerifyResultMessages save = detailView.save();
				if (save.hasError()) {
					setErrorMessage(save.getErrorMessage());
					return;
				} else {
					setErrorMessage(null);
				}
				super.widgetSelected(ex);
			}
		};
		CompositeUtils.setTableColumnSelectionListener(tvFiles, new SelectionListener[] { null,
				null, new CheckBoxColumnSelectionListener(new int[] { 3 }, true, true),
				new CheckBoxColumnSelectionListener(new int[] { 2 }, true, false), cbListener3 });

		tvFiles.addSelectionChangedListener(new ISelectionChangedListener() {

			public void selectionChanged(SelectionChangedEvent event) {
				setButtonsStatus();
				if (event.getSelection().isEmpty()) {
					return;
				}
				VerifyResultMessages result = detailView.save();
				if (result.hasError()) {
					return;
				}
				MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
				final List<SourceCSVConfig> csvConfigs = config.getCSVConfigs();
				detailView.setInput(csvConfigs.get(tvFiles.getTable().getSelectionIndex()));
			}
		});

		createTableViewMenus(tvFiles);
	}

	/**
	 * When migration wizard will show next page or previous page.
	 * 
	 * @param event PageChangingEvent
	 */
	protected void handlePageLeaving(PageChangingEvent event) {
		// If page is not complete, it should be go to previous page.
		if (!isPageComplete()) {
			return;
		}
		if (!updateMigrationConfig() && isGotoNextPage(event)) {
			event.doit = false;
		}
	}

	/**
	 * Parse all CSV Files in the configuration with progress dialog
	 * 
	 */
	private void parseAll() {
		CompositeUtils.runMethodInProgressBar(false, false, new IRunnableWithProgress() {

			public void run(IProgressMonitor monitor) throws InvocationTargetException,
					InterruptedException {
				try {
					monitor.beginTask(Messages.msgParsingCSVFiles, IProgressMonitor.UNKNOWN); //Refresh columns information and parsing preview
					final MigrationWizard wizard = getMigrationWizard();
					MigrationConfiguration config = wizard.getMigrationConfig();
					config.reparseCSVFiles(wizard.getTargetCatalog().getSchemas().get(0));
				} finally {
					monitor.done();
				}
			}
		});
	}

	/**
	 * Refresh table viewer.
	 * 
	 */
	private void refresh() {
		MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
		final List<SourceCSVConfig> csvConfigs = config.getCSVConfigs();
		int selection = tvFiles.getTable().getSelectionIndex();
		tvFiles.setInput(csvConfigs);
		if (csvConfigs.isEmpty()) {
			detailView.setEnabled(false);
			setErrorMessage(Messages.errMsgNoCSV);
		} else {
			detailView.setEnabled(true);
			if (selection < 0) {
				selection = 0;
			} else if (selection >= csvConfigs.size()) {
				selection = csvConfigs.size() - 1;
			}
			tvFiles.getTable().setSelection(selection);
			detailView.setInput(csvConfigs.get(selection));
			setErrorMessage(null);
		}

		setPageComplete(!csvConfigs.isEmpty());
		setButtonsStatus();
	}

	/**
	 * Update buttons and actions status
	 * 
	 */
	private void setButtonsStatus() {
		boolean isEmpty = tvFiles.getSelection().isEmpty();
		btnDelete.setEnabled(!isEmpty);
		deleteAction.setEnabled(btnDelete.getEnabled());
		getShell().setDefaultButton(null);
		tvFiles.getTable().setFocus();
	}

	/**
	 * Save user input (source database connection information) to export
	 * options.
	 * 
	 * @return true if update success.
	 */
	protected boolean updateMigrationConfig() {
		MigrationWizard wzd = getMigrationWizard();
		MigrationConfiguration config = wzd.getMigrationConfig();
		config.setSourceType(MigrationConfiguration.CSV);
		if (config.getCSVConfigs().isEmpty()) {
			setErrorMessage(Messages.errMsgNoCSV);
			return false;
		}
		for (TableItem ti : tvFiles.getTable().getItems()) {
			Object[] obj = (Object[]) ti.getData();
			SourceCSVConfig scc = (SourceCSVConfig) obj[obj.length - 1];
			scc.setCreate((Boolean) obj[2]);
			scc.setReplace((Boolean) obj[3]);
			scc.setImportFirstRow((Boolean) obj[4]);
		}
		VerifyResultMessages result = detailView.save();
		if (result.hasError()) {
			setErrorMessage(result.getErrorMessage());
			return false;
		}
		MigrationCfgUtils util = new MigrationCfgUtils();
		util.setTargetCatalog(wzd.getTargetCatalog(), wzd);
		result = util.checkAll(config);
		if (result.hasError()) {
			setErrorMessage(result.getErrorMessage());
			return false;
		}
		if (result.hasWarning()) {
			MessageDialog.openWarning(getShell(), Messages.msgWarning, result.getWarningMessage());
		}
		setErrorMessage(null);
		return true;
	}

}