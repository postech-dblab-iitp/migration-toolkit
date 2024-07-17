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
package com.cubrid.cubridmigration.ui.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.ui.PlatformUI;

import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.CMTConParamManager;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;

/**
 * JDBCConnectionMgrView Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-4-22 created by Kevin Cao
 */
public class JDBCConnectionMgrView {

	/**
	 * 
	 * @author fulei
	 */
	private class DeleteAction extends
			Action {
		public DeleteAction() {
			setText(Messages.removeButtonLabel);
			setImageDescriptor(MigrationUIPlugin.getImageDescriptor("icon/deleteDB.png"));
		}

		/**
		 * run
		 */
		public void run() {
			removeDBConInfo();
		}
	}

	/**
	 * 
	 * @author fulei
	 * 
	 */
	private class RefreshAction extends
			Action {
		/**
		 * constructor
		 */
		public RefreshAction() {
			setText(Messages.refreshButtonLabel);
			setImageDescriptor(MigrationUIPlugin.getImageDescriptor("icon/refresh.gif"));
		}

		/**
		 * run
		 */
		public void run() {
			refreshCon();
		}
	}

	private static final Logger LOG = LogUtil.getLogger(JDBCConnectionMgrView.class);

	private final IJDBCConnectionFilter conFilter;

	private final List<DatabaseConnectionInfo> dbDataList = new ArrayList<DatabaseConnectionInfo>();
	//Current selected database node id
	private String dbID;

	private TableViewer dbTableViewer;
	private Composite grpOnline;
	private Catalog scriptCatalog;

	private final List<Integer> supportedDBType = new ArrayList<Integer>();

	public JDBCConnectionMgrView(Collection<Integer> supportedDBType,
			IJDBCConnectionFilter conFilter) {
		this.supportedDBType.addAll(supportedDBType);
		this.conFilter = conFilter;
	}

	/**
	 * add database connection info
	 */
	private void addDBConInfo() {
		ConnParameters cp = DBConnectionDialog.getCatalog(getActiveShell(), getDBTypeArray(), null);
		if (cp == null) {
			return;
		}
		for (DatabaseConnectionInfo dbCon : dbDataList) {
			dbCon.setSelected(false);
		}
		// a new db ,add to list,set it on selected
		DatabaseConnectionInfo info = new DatabaseConnectionInfo();
		info.setSelected(true);
		info.setConnParameters(cp);
		dbDataList.add(info);
		// add to Database Explorer
		CMTConParamManager.getInstance().addConnection(cp, false);
		dbID = cp.getConName();
		dbTableViewer.refresh();
	}

	/**
	 * Retrieves the current active shell for showing dialog.
	 * 
	 * @return Shell
	 */
	private Shell getActiveShell() {
		return PlatformUI.getWorkbench().getDisplay().getActiveShell();
	}

	/**
	 * Auto add ConnParameters to list.
	 * 
	 * @param cp ConnParameters
	 */
	private void autoAdd(ConnParameters cp) {
		if (conFilter != null && conFilter.doFilter(cp)) {
			return;
		}
		final CMTConParamManager cpm = CMTConParamManager.getInstance();
		//Get the name will be used.
		int i = 1;
		String tmpName = cp.getConName();
		while (cpm.isNameUsed(tmpName)) {
			tmpName = cp.getConName() + i;
		}
		//Add and select the connection
		dbID = tmpName;
		cp.setName(tmpName);

		DatabaseConnectionInfo info = new DatabaseConnectionInfo();
		info.setSelected(true);
		info.setConnParameters(cp.clone());
		dbDataList.add(info);
		cpm.addConnection(cp, false);
	}

	/**
	 * auto Select 1st
	 */
	private void autoSelect1() {
		if (dbDataList.size() == 1) {
			DatabaseConnectionInfo dci = dbDataList.get(0);
			dci.setSelected(true);
			dbID = dci.getConnParameters().getConName();
		}
	}

	/**
	 * When the selection of table viewer changed.
	 * 
	 * @param data String[]
	 */
	private void changeDBListData(Object data) {
		if (dbDataList.isEmpty() || data == null) {
			return;
		}
		for (DatabaseConnectionInfo dbCon : dbDataList) {
			dbCon.setSelected(false);
		}
		DatabaseConnectionInfo info = (DatabaseConnectionInfo) data;
		info.setSelected(true);
		dbID = info.getConnParameters().getConName();
		dbTableViewer.refresh();
	}

	/**
	 * Create the controls of this view. Only run once.
	 * 
	 * @param parent Composite
	 */
	public void createControls(Composite parent) {
		if (grpOnline != null) {
			return;
		}
		grpOnline = new Composite(parent, SWT.BORDER);
		grpOnline.setLayout(new GridLayout(2, false));
		GridData groupGridData = new GridData(SWT.FILL, SWT.FILL, true, true);
		grpOnline.setLayoutData(groupGridData);

		Composite dbTableContainer = new Composite(grpOnline, SWT.NONE);
		dbTableContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		dbTableContainer.setLayout(new GridLayout());
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {"", Messages.sourceDBPageTableConnNm,
				Messages.sourceDBPageTableDbNm, Messages.sourceDBPageTableIP,
				Messages.sourceDBPageTablePort, Messages.sourceDBPageTableDbType,
				Messages.sourceDBPageTableCharset});
		tvBuilder.setColumnWidths(new int[] {28, 150, 120, 110, 85, 121, 110});
		tvBuilder.setContentProvider(new DBContentProvider(MigrationWizard.getSupportedSrcDBTypes()));
		tvBuilder.setLabelProvider(new DBLabelProvider());
		dbTableViewer = tvBuilder.buildTableViewer(dbTableContainer, SWT.BORDER
				| SWT.FULL_SELECTION);

		Table table = dbTableViewer.getTable();
		MenuManager menuManager = new MenuManager();
		menuManager.add(new RefreshAction());
		menuManager.add(new DeleteAction());
		Menu menu = menuManager.createContextMenu(table);
		table.setMenu(menu);

		dbTableViewer.addSelectionChangedListener(new ISelectionChangedListener() {
			public void selectionChanged(final SelectionChangedEvent event) {
				IStructuredSelection selection = (IStructuredSelection) event.getSelection();
				changeDBListData(selection.getFirstElement());
			}
		});
		dbTableViewer.addDoubleClickListener(new IDoubleClickListener() {

			public void doubleClick(DoubleClickEvent event) {
				editDBConInfo();
			}
		});

		Composite buttonContainer = new Composite(grpOnline, SWT.NONE);
		GridData buttonGd = new GridData(SWT.LEFT, SWT.FILL, false, true);
		buttonGd.minimumWidth = 70;
		buttonContainer.setLayoutData(buttonGd);
		buttonContainer.setLayout(new GridLayout());

		Button btnNewDb = new Button(buttonContainer, SWT.NONE);
		btnNewDb.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnNewDb.setText(Messages.newButtonLabel);
		btnNewDb.setToolTipText(Messages.ttCreateNewConnection);
		btnNewDb.setAlignment(SWT.CENTER);
		btnNewDb.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				addDBConInfo();
			}
		});

		Button btnEditeDb = new Button(buttonContainer, SWT.NONE);
		btnEditeDb.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnEditeDb.setText(Messages.editeButtonLabel);
		btnEditeDb.setToolTipText(Messages.ttEditConnection);
		btnEditeDb.setAlignment(SWT.CENTER);
		btnEditeDb.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				editDBConInfo();
			}
		});

		Button btnRemoveDb = new Button(buttonContainer, SWT.NONE);
		btnRemoveDb.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnRemoveDb.setText(Messages.removeButtonLabel);
		btnRemoveDb.setToolTipText(Messages.ttRemoveConnection);
		btnRemoveDb.setAlignment(SWT.CENTER);
		btnRemoveDb.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				removeDBConInfo();
			}
		});

		Button btnRefresh = new Button(buttonContainer, SWT.NONE);
		btnRefresh.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnRefresh.setText(Messages.refreshButtonLabel);
		btnRefresh.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent se) {
				refreshCon();
			}

		});
	}

	/**
	 * edit database connection info
	 */
	private void editDBConInfo() {
		DatabaseConnectionInfo selDci = getSelectedDCI();
		if (selDci == null) {
			MessageDialog.openError(getActiveShell(), Messages.msgWarning,
					Messages.sourceDBPageErrNoSelectedItem);
			return;
		}
		ConnParameters oldcp = selDci.getConnParameters();
		ConnParameters cp = DBConnectionDialog.getCatalog(getActiveShell(), getDBTypeArray(), oldcp);
		if (null == cp) {
			return;
		}
		// a new db ,add to list,set it on selected
		selDci.setConnParameters(cp);
		selDci.setSelected(true);
		//Use old name to update the connection
		CMTConParamManager cpm = CMTConParamManager.getInstance();
		cpm.updateConnection(dbID, cp, false);
		//Specify new connection name and update catalog cache.
		dbID = cp.getConName();
		//If parameter changed, reset the catalog cache.
		if (!cp.isSameDB(oldcp)) {
			cpm.updateCatalog(dbID, null);
		}

		dbTableViewer.update(selDci, null);
		dbTableViewer.refresh();
	}

	/**
	 * Fill the dbDataList with all database connections
	 * 
	 */
	private void fillWithAllDBInfo() {
		dbDataList.clear();
		final List<ConnParameters> connections = CMTConParamManager.getInstance().getConnections();
		for (ConnParameters cp : connections) {
			if (null == cp) {
				continue;
			}
			if (!supportedDBType.contains(cp.getDatabaseType().getID())) {
				continue;
			}
			if (conFilter != null && conFilter.doFilter(cp)) {
				continue;
			}
			DatabaseConnectionInfo info = new DatabaseConnectionInfo();
			info.setSelected(false);
			info.setConnParameters(cp);
			dbDataList.add(info);
		}
	}

	/**
	 * get Catalog
	 * 
	 * @return Catalog
	 */
	public Catalog getCatalog() {
		if (StringUtils.isBlank(dbID)) {
			return null;
		}
		CMTConParamManager instance = CMTConParamManager.getInstance();
		final ConnParameters cp = instance.getConnection(dbID);
		if (cp == null) {
			return null;
		}
		try {
			//Get the cached catalog
			Catalog catalog = instance.getCatalog(dbID);
			//If no cached
			if (null == catalog) {
				updateConParamCatalog(cp);
				catalog = instance.getCatalog(dbID);
			} else {
				//Cache found, newer catalog information should replace the old catalog.
				if (scriptCatalog != null && cp.isSameDB(scriptCatalog.getConnectionParameters())
						&& scriptCatalog.getCreateTime() > catalog.getCreateTime()) {
					if (MessageDialog.openQuestion(getActiveShell(), Messages.msgConfirmation,
							Messages.msgIsUseNewerScriptCatalog)) {
						instance.updateCatalog(dbID, scriptCatalog);
					}
				}
			}
			if (catalog != null) {
				catalog.setConnectionParameters(cp.clone());
			}
			return catalog;
		} catch (Exception ignored) {
			LOG.error(LogUtil.getExceptionString(ignored));
		}
		return null;
	}

	/**
	 * Retrieves the root composite.
	 * 
	 * @return Composite
	 */
	public Composite getComposite() {
		return grpOnline;
	}

	/**
	 * 
	 * @return DatabaseType[]
	 */
	private DatabaseType[] getDBTypeArray() {
		DatabaseType[] result = new DatabaseType[supportedDBType.size()];
		int i = 0;
		for (Integer id : supportedDBType) {
			result[i] = DatabaseType.getDatabaseTypeByID(id);
			i++;
		}
		return result;
	}

	/**
	 * Get selected database.
	 * 
	 * @return DatabaseConnectionInfo
	 */
	public DatabaseConnectionInfo getSelectedDCI() {
		for (DatabaseConnectionInfo dci : dbDataList) {
			if (dci.getConnParameters().getConName().equals(dbID)) {
				return dci;
			}
		}
		return null;
	}

	/**
	 * Hide this view.
	 * 
	 */
	public void hide() {
		if (grpOnline == null) {
			return;
		}
		grpOnline.setVisible(false);
		((GridData) grpOnline.getLayoutData()).exclude = true;
	}

	/**
	 * Initialize the table viewer.
	 * 
	 * @param cp the initialized connection parameter
	 * @param scriptCatalog Cached catalog from other way
	 */
	public void init(ConnParameters cp, Catalog scriptCatalog) {
		fillWithAllDBInfo();
		this.scriptCatalog = scriptCatalog;
		if (dbID == null) {
			if (cp == null) {
				//If only one database connection here, auto select the database connection
				autoSelect1();
			} else {
				//check if connection parameters is in database connection list, retrieve the first element
				selectSameDB(cp);
				//if loadscript wizard and use online srcdb , but the db in script doesn't in list try to connect it
				if (dbID == null) {
					autoAdd(cp);
				}
			}
		}
		if (dbID != null) {
			for (DatabaseConnectionInfo dci : dbDataList) {
				dci.setSelected(dbID.equals(dci.getConnParameters().getConName()));
			}
		}
		dbTableViewer.setInput(dbDataList);
		dbTableViewer.refresh();
	}

	/**
	 * Refresh the schema of selected connection.
	 * 
	 */
	private void refreshCon() {
		DatabaseConnectionInfo dci = getSelectedDCI();
		if (dci == null) {
			MessageDialog.openError(getActiveShell(), Messages.msgWarning,
					Messages.sourceDBPageErrNoSelectedItem);
			return;
		}
		if (!MessageDialog.openConfirm(getActiveShell(), Messages.msgConfirmation,
				Messages.refreshDBConnActionMessage)) {
			return;
		}
		updateConParamCatalog(dci.getConnParameters());
	}

	/**
	 * removeDBConInfo
	 */
	private void removeDBConInfo() {
		DatabaseConnectionInfo dci = getSelectedDCI();
		if (dci == null) {
			MessageDialog.openError(getActiveShell(), Messages.msgWarning,
					Messages.sourceDBPageErrNoSelectedItem);
			return;
		}
		if (!MessageDialog.openConfirm(
				getActiveShell(),
				Messages.delDBConnActionTitle,
				Messages.bind(Messages.delDBConnActionMessage, dci.getConnParameters().getConName()))) {
			return;
		}
		CMTConParamManager.getInstance().removeConnection(dbID, false);
		dbDataList.remove(dci);
		dbTableViewer.refresh();
		dbID = null;
	}

	/**
	 * 
	 * @param cp ConnParameters
	 */
	private void selectSameDB(ConnParameters cp) {
		for (DatabaseConnectionInfo dci : dbDataList) {
			if (dci.getConnParameters().isSameDB(cp)) {
				dci.setSelected(true);
			}
			if (dci.isSelected()) {
				dbID = dci.getConnParameters().getConName();
				break;
			}
		}
	}

	/**
	 * Change the supported DB types
	 * 
	 * @param dts List<Integer>
	 */
	public void setSupportedDBType(List<Integer> dts) {
		supportedDBType.clear();
		supportedDBType.addAll(dts);
	}

	/**
	 * Show this view
	 * 
	 */
	public void show() {
		if (grpOnline == null) {
			return;
		}
		grpOnline.setVisible(true);
		((GridData) grpOnline.getLayoutData()).exclude = false;
	}

	/**
	 * Update the catalog cache of connection parameters.
	 * 
	 * @param cp connection parameters.
	 */
	private void updateConParamCatalog(ConnParameters cp) {
		final CMTConParamManager cpm = CMTConParamManager.getInstance();
		SchemaFetcherWithProgress fetcher = SchemaFetcherWithProgress.getInstance(cp);
		Catalog catalog = fetcher.fetch();

		//If fetch catalog successfully, update cache and return.
		if (catalog != null) {
			cpm.updateCatalog(dbID, catalog);
			return;
		}
		//GDB if database type is graph, maybe catalog is not need?
		if (cp.getDbType() == DatabaseType.NEO4J.getID() || cp.getDbType() == DatabaseType.TURBO.getID()) {
			return;
		}
		
		//Cache catalog for mapping
		final Catalog oldCatalog;
		String errorMsg = fetcher.getErrorMessage() == null ? ""
				: (fetcher.getErrorMessage() + "\r\n");
		if (scriptCatalog == null) {
			oldCatalog = cpm.getCatalog(cp.getConName());
			errorMsg = errorMsg + Messages.msgIsUseCachedCatalog;
		} else if (cp.isSameDB(scriptCatalog.getConnectionParameters())) {
			//The script's connection parameter should be as same as the input connection parameters.
			oldCatalog = scriptCatalog;
			errorMsg = errorMsg + Messages.msgIsUseScriptCatalog;
		} else {
			oldCatalog = null;
		}
		//If fetch catalog failed, and there is no old catalog
		if (oldCatalog == null) {
			errorMsg = fetcher.getErrorMessage() == null ? "" : fetcher.getErrorMessage();
			DetailMessageDialog.openError(getActiveShell(), Messages.msgError, errorMsg,
					(fetcher.getError() == null ? fetcher.getErrorMessage()
							: fetcher.getError().getMessage()));
			return;
		}
		//Query user if use old catalog
		if (!DetailMessageDialog.openConfirm(getActiveShell(), Messages.msgError, errorMsg,
				(fetcher.getError() == null ? fetcher.getErrorMessage()
						: fetcher.getError().getMessage()))) {
			cpm.updateCatalog(dbID, null);
			return;
		}
		//Update cached catalog with old catalog.
		cpm.updateCatalog(dbID, oldCatalog);
	}
}
