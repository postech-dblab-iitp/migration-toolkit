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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IMessageProvider;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.common.navigator.node.ColumnNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ColumnsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.DatabaseNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FKNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FKsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.IndexNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.IndexesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.PKNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.PartitionNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.PartitionsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SQLTableNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SQLTablesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SchemaNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SequenceNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SequencesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TableNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TablesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ViewNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ViewsNode;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;
import com.cubrid.cubridmigration.ui.wizard.dialog.AdjustCharColumnDialog;
import com.cubrid.cubridmigration.ui.wizard.dialog.TableIndexSelectorDialog;
import com.cubrid.cubridmigration.ui.wizard.page.view.AbstractMappingView;
import com.cubrid.cubridmigration.ui.wizard.page.view.ColumnMappingView;
import com.cubrid.cubridmigration.ui.wizard.page.view.FKMappingView;
import com.cubrid.cubridmigration.ui.wizard.page.view.GeneralObjMappingView;
import com.cubrid.cubridmigration.ui.wizard.page.view.IRefreshableView;
import com.cubrid.cubridmigration.ui.wizard.page.view.IndexMappingView;
import com.cubrid.cubridmigration.ui.wizard.page.view.SQLTableMappingView;
import com.cubrid.cubridmigration.ui.wizard.page.view.SequenceMappingView;
import com.cubrid.cubridmigration.ui.wizard.page.view.SourceDBExploreView;
import com.cubrid.cubridmigration.ui.wizard.page.view.TableMappingView;
import com.cubrid.cubridmigration.ui.wizard.page.view.ViewMappingView;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * Page to set up mapping from source DB objects to target DB objects
 * 
 * @author caoyilin
 * @version 1.0 - 2012-07-20
 */
public class ObjectMappingPage extends
		MigrationWizardPage implements
		IRefreshableView {
	private static final Logger LOG = LogUtil.getLogger(ObjectMappingPage.class);
	private SourceDBExploreView tvSourceDBObjects;
	private final Map<String, AbstractMappingView> node2ViewMapping = new HashMap<String, AbstractMappingView>();

	private AbstractMappingView currentView;

	private final MigrationCfgUtils util = new MigrationCfgUtils();
	
	/**
	 * Create the wizard constructor
	 */
	public ObjectMappingPage(String pageName) {
		super(pageName);
	}

	/**
	 * Initialize page
	 * 
	 * @param event PageChangedEvent
	 */
	protected void afterShowCurrentPage(PageChangedEvent event) {
		final MigrationWizard mw = getMigrationWizard();
		setTitle(mw.getStepNoMsg(ObjectMappingPage.this) + Messages.objectMapPageTitle);
		setDescription(Messages.objectMapPageDescription);
		setSourceTableNoPKWarningMessage();
		//Clear error messages.
		setErrorMessage(null);
		//Refresh some status of current wizard. 
		mw.refreshWizardStatus();
		util.setTargetCatalog(mw.getTargetCatalog(), mw);
		if (!isFirstVisible) {
			return;
		}
		try {
			//Update migration source database schema
			Catalog sourceCatalog = mw.getSourceCatalog();
			final MigrationConfiguration cfg = mw.getMigrationConfig();
			if (cfg.sourceIsOnline() && !cfg.getSourceDBType().equals(DatabaseType.CUBRID)) {
				MessageDialog.openInformation(getShell(), Messages.msgInformation,
						Messages.msgLowerCaseWarning);
			}

			if (isFirstVisible
					&& util.checkMultipleSchema(sourceCatalog, cfg)
					&& util.createAllObjectsMap(sourceCatalog)
					&& util.hasDuplicatedObjects(sourceCatalog)) {
				showDetailMessageDialog(sourceCatalog);
			}

			//GDB if targetIsGraph, draw new ui

			showLobInfo(sourceCatalog);
			cfg.setSrcCatalog(sourceCatalog, !mw.isLoadMigrationScript());

			//Reset migration configuration
			for (AbstractMappingView amv : node2ViewMapping.values()) {
				amv.setMigrationConfig(cfg);
				amv.setWizardStatus(mw);
			}
			util.setMigrationConfiguration(cfg);
			util.setDataTypeHelper();
			
			refreshTreeView();
			this.getShell().setMaximized(true);
			isFirstVisible = false;
			// select all if there have no selected tables to migrate
			if (!cfg.hasObjects2Export()) {
				cfg.setAll(true);
				refreshCurrentView();
			}
			String msg = util.getNoPKSourceTablesCheckingResult();
			if (StringUtils.isNotBlank(msg)) {
				super.setMessage(msg);
			}
			if (util.doesNeedToChangeCharacterTypeSize()
					&& UICommonTool.openConfirmBox(Messages.msgCheckCharset)) {
				openAdjustCharColumnDialog();
			}
		} catch (RuntimeException ex) {
			LOG.error(LogUtil.getExceptionString(ex));
			throw ex;
		}

	}

	/**
	 * showLobInfo
	 * @param sourceCatalog
	 */
	private void showLobInfo(Catalog sourceCatalog) {
		String lobInfo = util.getLobInfo(sourceCatalog);
		if (StringUtils.isNotEmpty(lobInfo)) {
			DetailMessageDialog.openInfo(getShell(), Messages.titleLobInformation, Messages.msgLobInformation, lobInfo);
		}
	}

	private void showDetailMessageDialog(Catalog sourceCatalog) {
		String detailMessage = getDetailMessage(sourceCatalog);
		DetailMessageDialog.openInfo(getShell(), Messages.titleDuplicateObjects, Messages.msgDuplicateObjects, detailMessage);
	}

	private String getDetailMessage(Catalog sourceCatalog) {
		StringBuffer sb = new StringBuffer();
		util.createDetailMessage(sb, sourceCatalog, DBObject.OBJ_TYPE_TABLE);
		util.createDetailMessage(sb, sourceCatalog, DBObject.OBJ_TYPE_VIEW);
		util.createDetailMessage(sb, sourceCatalog, DBObject.OBJ_TYPE_SEQUENCE);
		return sb.toString();
	}

	/**
	 * setNoPKWarnings
	 */
	private void setSourceTableNoPKWarningMessage() {
		final MigrationWizard mw = getMigrationWizard();
		Catalog sourceCatalog = mw.getSourceCatalog();
		final MigrationConfiguration cfg = mw.getMigrationConfig();
		if (cfg.isCreateConstrainsBeforeData()) {
			StringBuffer descriptionMessage = new StringBuffer();
			List<String> noPKTables = util.getNoPKTables(sourceCatalog);
			if (CollectionUtils.isNotEmpty(noPKTables)) {
				descriptionMessage.append(" Source tables without primary key: ");
				for (String noPKTableName : noPKTables) {
					descriptionMessage.append(noPKTableName).append(", ");
				}
				descriptionMessage.deleteCharAt(descriptionMessage.length() - 1);
				descriptionMessage.deleteCharAt(descriptionMessage.length() - 1);
			}
			setMessage(descriptionMessage.toString(), IMessageProvider.WARNING);
		}
	}

	/**
	 * Create contents of the wizard
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		Composite container = new Composite(parent, SWT.NONE);
		container.setLayout(new GridLayout());
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		setControl(container);

		SashForm container2 = new SashForm(container, SWT.HORIZONTAL);
		container2.setLayout(new FillLayout());
		container2.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		createTreeView(container2);
		createDetailPanel(container2);
		container2.setWeights(new int[] {1, 3});
		createToolButtons(container);
	}

	/**
	 * Create right panel
	 * 
	 * @param parent Composite
	 */
	protected void createDetailPanel(Composite parent) {
		Group detailContainer = new Group(parent, SWT.NONE);
		detailContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		detailContainer.setLayout(new GridLayout());
		detailContainer.setText(Messages.dbObjectSelectMapping);

		GeneralObjMappingView generalObjMappingView = new GeneralObjMappingView(detailContainer);
		//Double click to show detail information
		generalObjMappingView.addDoubleClickListener(new IDoubleClickListener() {

			public void doubleClick(DoubleClickEvent event) {
				if (event.getSource() == null || event.getSelection().isEmpty()) {
					return;
				}
				TableViewer tv = (TableViewer) event.getSource();
				String ct = tv.getData(AbstractMappingView.CONTENT_TYPE).toString();
				//The last element of the array is the source configuration object
				Object[] obj = (Object[]) ((StructuredSelection) event.getSelection()).getFirstElement();
				ICUBRIDNode cn = (ICUBRIDNode) currentView.getModel();

				while (cn != null) {
					if (cn instanceof SchemaNode) {
						break;
					}
					cn = cn.getParent();
					if (cn instanceof DatabaseNode) {
						break;
					}
				}
				if (cn == null) {
					return;
				}
				ICUBRIDNode selectionParent = cn;

				if (AbstractMappingView.CT_TABLE.equals(ct)) {
					for (ICUBRIDNode chn : cn.getChildren()) {
						if (chn instanceof TablesNode) {
							selectionParent = chn;
							break;
						}
					}
				} else if (AbstractMappingView.CT_VIEW.equals(ct)) {
					for (ICUBRIDNode chn : cn.getChildren()) {
						if (chn instanceof ViewsNode) {
							selectionParent = chn;
							break;
						}
					}
				} else if (AbstractMappingView.CT_SERIAL.equals(ct)) {
					for (ICUBRIDNode chn : cn.getChildren()) {
						if (chn instanceof SequencesNode) {
							selectionParent = chn;
							break;
						}
					}
				}
				//sql table column page draw part
				for (ICUBRIDNode col : selectionParent.getChildren()) {
					if (col.getName().equals((String) obj[1])) {
						tvSourceDBObjects.setSelection(col);
						showRightView(col, true);
						return;
					}
				}
			}
		});

		TableMappingView tableMappingView = new TableMappingView(detailContainer);
		//Double click to show detail information
//		tableMappingView.addDoubleClickListener(new IDoubleClickListener() {
//
//			public void doubleClick(DoubleClickEvent event) {
//				if (event.getSource() == null || event.getSelection().isEmpty()) {
//					return;
//				}
//				//The last element of the array is the source configuration object
//				Object[] obj = (Object[]) ((StructuredSelection) event.getSelection()).getFirstElement();
//				TableViewer tv = (TableViewer) event.getSource();
//				String ct = tv.getData(AbstractMappingView.CONTENT_TYPE).toString();
//				ICUBRIDNode cn = (ICUBRIDNode) currentView.getModel();
//				while (cn != null) {
//					if (cn instanceof TableNode) {
//						break;
//					}
//					cn = cn.getParent();
//				}
//				//For klocwork
//				if (cn == null) {
//					return;
//				}
//				ICUBRIDNode selectionParent = cn;
//				if (AbstractMappingView.CT_COLUMN.equals(ct)) {
//					for (ICUBRIDNode chn : cn.getChildren()) {
//						if (chn instanceof ColumnsNode) {
//							selectionParent = chn;
//							break;
//						}
//					}
//				} else if (AbstractMappingView.CT_FK.equals(ct)) {
//					for (ICUBRIDNode chn : cn.getChildren()) {
//						if (chn instanceof FKsNode) {
//							selectionParent = chn;
//							break;
//						}
//					}
//				} else if (AbstractMappingView.CT_INDEX.equals(ct)) {
//					for (ICUBRIDNode chn : cn.getChildren()) {
//						if (chn instanceof IndexesNode) {
//							selectionParent = chn;
//							break;
//						}
//					}
//				}
//				for (ICUBRIDNode col : selectionParent.getChildren()) {
//					if (col.getName().equals((String) obj[1])) {
//						tvSourceDBObjects.setSelection(col);
//						showRightView(col, true);
//						return;
//					}
//				}
//			}
//		});
		ColumnMappingView columnMappingView = new ColumnMappingView(detailContainer);
		IndexMappingView indexMappingView = new IndexMappingView(detailContainer);
		FKMappingView fkMappingView = new FKMappingView(detailContainer);
		SequenceMappingView sequenceMappingView = new SequenceMappingView(detailContainer);
		ViewMappingView viewMappingView = new ViewMappingView(detailContainer);

		generalObjMappingView.addSQLChangedListener(tvSourceDBObjects);
		//Building Tree node to Mapping view mapping
		node2ViewMapping.put(DatabaseNode.class.getName(), generalObjMappingView);
		node2ViewMapping.put(SchemaNode.class.getName(), generalObjMappingView);
		node2ViewMapping.put(TablesNode.class.getName(), generalObjMappingView);
		node2ViewMapping.put(ViewsNode.class.getName(), generalObjMappingView);
		node2ViewMapping.put(SequencesNode.class.getName(), generalObjMappingView);
		node2ViewMapping.put(TableNode.class.getName(), tableMappingView);
		node2ViewMapping.put(ViewNode.class.getName(), viewMappingView);
		node2ViewMapping.put(SequenceNode.class.getName(), sequenceMappingView);
		node2ViewMapping.put(PKNode.class.getName(), tableMappingView);
		//node2ViewMapping.put(ColumnsNode.class.getName(), tableMappingView);
		node2ViewMapping.put(FKsNode.class.getName(), tableMappingView);
		node2ViewMapping.put(IndexesNode.class.getName(), tableMappingView);
		node2ViewMapping.put(PartitionsNode.class.getName(), tableMappingView);
		node2ViewMapping.put(PartitionNode.class.getName(), tableMappingView);
		node2ViewMapping.put(ColumnNode.class.getName(), columnMappingView);
		node2ViewMapping.put(FKNode.class.getName(), fkMappingView);
		node2ViewMapping.put(IndexNode.class.getName(), indexMappingView);
		node2ViewMapping.put(SQLTableNode.class.getName(), new SQLTableMappingView(detailContainer));
		node2ViewMapping.put(SQLTablesNode.class.getName(), generalObjMappingView);
	}

	/**
	 * Create tool buttons
	 * 
	 * @param parent Composite
	 */
	private void createToolButtons(Composite parent) {
		//***********************************************************************
		final Composite bottomComposite = new Composite(parent, SWT.NONE);
		bottomComposite.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, true, false, 2, 1));
		final GridLayout gridLayout = new GridLayout();
		bottomComposite.setLayout(gridLayout);

		//***********************************************************************
		Composite grpChangeSize = new Composite(bottomComposite, SWT.BORDER);
		grpChangeSize.setLayout(new GridLayout());
		grpChangeSize.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, true));
		//grpChangeSize.setText(Messages.mappingColSizeOptTitle);

		ToolBar tbTools = new ToolBar(grpChangeSize, SWT.WRAP | SWT.RIGHT | SWT.FLAT);
		tbTools.setLayout(new GridLayout());
		tbTools.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));

		final ToolItem btnReuseOID = new ToolItem(tbTools, SWT.CHECK);
		//btnConstaintSelector.setVisible(false);
		//		btnReuseOID.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true,
		//				false));
		btnReuseOID.setText(Messages.lblReuseOID);
		btnReuseOID.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent ev) {
				if (!saveCurrentView()) {
					return;
				}
				final MigrationWizard mw = getMigrationWizard();
				//Update migration source database schema
				final MigrationConfiguration cfg = mw.getMigrationConfig();
				final List<Table> targetTableSchema = cfg.getTargetTableSchema();
				for (Table tt : targetTableSchema) {
					tt.setReuseOID(btnReuseOID.getSelection());
				}
				refreshCurrentView();
			}
		});
		new ToolItem(tbTools, SWT.SEPARATOR);
		//		Group grpSelect = new Group(bottomComposite, SWT.LEFT_TO_RIGHT);
		//		grpSelect.setLayout(new GridLayout(4, false));

		ToolItem btnSelectAll = new ToolItem(tbTools, SWT.PUSH);
		//		btnSelectAll.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true,
		//				false));
		btnSelectAll.setText(Messages.lblSelectAll);
		btnSelectAll.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent ev) {
				if (!saveCurrentView()) {
					return;
				}
				getMigrationWizard().getMigrationConfig().setAll(true);
				refreshCurrentView();
				setErrorMessage(null);
			}
		});
		new ToolItem(tbTools, SWT.SEPARATOR);
		ToolItem btnClearAll = new ToolItem(tbTools, SWT.PUSH);
		//		btnClearAll.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true,
		//				false));
		btnClearAll.setText(Messages.lblClearAll);
		btnClearAll.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent ev) {
				if (MessageDialog.openConfirm(null, Messages.lblClearAll, Messages.msgCfmClearALL)) {
					final MigrationWizard mw = getMigrationWizard();
					//Update migration source database schema
					final MigrationConfiguration cfg = mw.getMigrationConfig();
					cfg.setAll(false);
					//Fill Tree View
					refreshTreeView();
					if (cfg.getExpSQLCfg().isEmpty()) {
						setErrorMessage(Messages.errNoDBObject);
					}
				}
			}
		});
		new ToolItem(tbTools, SWT.SEPARATOR);
		ToolItem btnConstaintSelector = new ToolItem(tbTools, SWT.PUSH);
		//btnConstaintSelector.setVisible(false);
		//		btnConstaintSelector.setLayoutData(new GridData(SWT.FILL, SWT.CENTER,
		//				true, false));
		btnConstaintSelector.setText(Messages.lblIndexQuickSetting);
		btnConstaintSelector.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent ev) {
				if (!saveCurrentView()) {
					return;
				}
				final MigrationWizard mw = getMigrationWizard();
				//Update migration source database schema
				final MigrationConfiguration cfg = mw.getMigrationConfig();

				TableIndexSelectorDialog dialog = new TableIndexSelectorDialog(getShell(), cfg);
				if (dialog.open() != Dialog.OK) {
					return;
				}
				refreshCurrentView();
			}
		});

		new ToolItem(tbTools, SWT.SEPARATOR);
		ToolItem btnChangeCharColumns = new ToolItem(tbTools, SWT.NONE);
		btnChangeCharColumns.setText(Messages.btnChangeCharColumns);

		btnChangeCharColumns.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				if (!saveCurrentView()) {
					return;
				}
				openAdjustCharColumnDialog();
			}
		});
	}

	/**
	 * Create source database Tree Viewer
	 * 
	 * @param parent SashForm
	 */
	protected void createTreeView(SashForm parent) {
		Group srcDBContainer = new Group(parent, SWT.NONE);
		GridData gdTV = new GridData(SWT.LEFT, SWT.FILL, false, true);
		srcDBContainer.setLayoutData(gdTV);
		srcDBContainer.setLayout(new GridLayout());
		srcDBContainer.setText(Messages.lblSourceDBPart);

		tvSourceDBObjects = new SourceDBExploreView(srcDBContainer, SWT.BORDER);
		tvSourceDBObjects.setRefreshableView(this);
		tvSourceDBObjects.addSelectionChangedListener(new ISelectionChangedListener() {

			public void selectionChanged(SelectionChangedEvent event) {
				if (event.getSelection().isEmpty()) {
					return;
				}
				IStructuredSelection ss = (IStructuredSelection) event.getSelection();
				showRightView(ss.getFirstElement(), true);
			}
		});
	}

	/**
	 * When migration wizard will show next page or previous page.
	 * 
	 * @param event PageChangingEvent
	 */
	protected void handlePageLeaving(PageChangingEvent event) {
		if (!isGotoNextPage(event)) {
			return;
		}
		event.doit = validateConfig();
	}

	/**
	 * Open adjust char column dialog.
	 * 
	 */
	private void openAdjustCharColumnDialog() {
		AdjustCharColumnDialog dialog = new AdjustCharColumnDialog(
				ObjectMappingPage.this.getShell(), util);
		dialog.open();
		refreshCurrentView();
	}

	/**
	 * Refresh current view
	 * 
	 */
	public void refreshCurrentView() {
		if (currentView != null) {
			currentView.showData(currentView.getModel());
		}
	}

	/**
	 * Refresh the source tree viewer.
	 * 
	 * @param mw
	 * @param cfg
	 */
	private void refreshTreeView() {
		final MigrationWizard mw = getMigrationWizard();
		final MigrationConfiguration cfg = mw.getMigrationConfig();
		//Fill Tree View
		tvSourceDBObjects.setInput(mw.getSelectSourceDB(), cfg);
		//If Source DB is changed, clear the last view UI.
		if (currentView != null) {
			currentView.hide();
			currentView = null;
		}
		//Database node will not be selected.
		ICUBRIDNode node = mw.getSelectSourceDB();
		List<ICUBRIDNode> schemaNodes = node.getChildren();
		if (schemaNodes.size() == 1) {
			node = schemaNodes.get(0).getChildren().get(0);
		} else if (schemaNodes.size() > 1) {
			node = schemaNodes.get(0);
		}
		showRightView(node, true);
		tvSourceDBObjects.setFocus();
	}

	/**
	 * Set the page is first show
	 * 
	 * @param isFirstVisible boolean
	 */
	public void setFirstVisible(boolean isFirstVisible) {
		this.isFirstVisible = isFirstVisible;
	}

	/**
	 * The selection is ICubridNode or SourceSQLConfig
	 * 
	 * @param selection Object
	 * @param autoSave If save current view before show next view
	 */
	private void showRightView(Object selection, boolean autoSave) {
		if (selection == null) {
			return;
		}
		AbstractMappingView view = node2ViewMapping.get(selection.getClass().getName());
		if (view == null) {
			//Show it's parent
			showRightView(((ICUBRIDNode) selection).getParent(), autoSave);
			return;
		}
		//If old view is not null, save it firstly.
		if (autoSave && currentView != null) {
			try {
				VerifyResultMessages msg = currentView.save();
				if (msg.hasError()) {
					tvSourceDBObjects.setSelection(currentView.getModel());
					this.setErrorMessage(msg.getErrorMessage());
					return;
				}
			} catch (Exception ex) {
				LOG.error("", ex);
				if (!MessageDialog.openConfirm(getShell(), Messages.lblSaveConfig,
						Messages.msgCfmErrorSave)) {
					tvSourceDBObjects.setSelection(currentView.getModel());
					return;
				}
			}
		}
		if (currentView != null && !currentView.equals(view)) {
			currentView.hide();
		}
		this.setErrorMessage(null);
		currentView = view;
		currentView.show();
		currentView.showData(selection);
	}

	/**
	 * Save current view's data and validate the migration configuration
	 * 
	 * @return true if it can go to next step
	 */
	private boolean validateConfig() {
		if (!saveCurrentView()) {
			return false;
		}
		VerifyResultMessages result = util.checkAll(getMigrationWizard().getMigrationConfig());
		if (result.hasError()) {
			setErrorMessage(result.getErrorMessage());
			MessageDialog.openError(getShell(), Messages.msgError, result.getErrorMessage());
			return false;
		}
		//Clear error message of UI.
		setErrorMessage(null);
		//Show confirm dialog.
		StringBuffer detailMsg = new StringBuffer();
		if (result.hasConfirm()) {
			detailMsg.append(result.getConfirmMessage()).append("\r\n");
		}
		if (result.hasWarning()) {
			detailMsg.append(result.getWarningMessage()).append("\r\n");
		}
		if (detailMsg.length() > 0) {
			boolean confirm = DetailMessageDialog.openConfirm(getShell(), Messages.msgConfirmation,
					Messages.msgConfirmWithDetail, detailMsg.toString());
			if (!confirm) {
				return false;
			}
		}
		return true;
	}

	/**
	 * @return save current view result
	 */
	protected boolean saveCurrentView() {
		if (currentView != null) {
			VerifyResultMessages msg = currentView.save();
			if (msg.hasError()) {
				setErrorMessage(msg.getErrorMessage());
				MessageDialog.openError(getShell(), Messages.msgError, msg.getErrorMessage());
				return false;
			}
		}
		return true;
	}
}
