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
package com.cubrid.cubridmigration.ui.wizard;

import java.io.File;
import java.sql.Connection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.DialogSettings;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.wizard.IWizardPage;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.MigrationTemplateParser;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.common.navigator.event.CubridNodeManager;
import com.cubrid.cubridmigration.ui.common.navigator.node.DatabaseNode;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.MigrationScript;
import com.cubrid.cubridmigration.ui.script.MigrationScriptManager;
import com.cubrid.cubridmigration.ui.script.dialog.ScheduleMigrationTaskDialog;
import com.cubrid.cubridmigration.ui.wizard.dialog.MigrationRunModeDialog;
import com.cubrid.cubridmigration.ui.wizard.editor.MigrationProgressEditorInput;
import com.cubrid.cubridmigration.ui.wizard.page.CSVImportConfirmPage;
import com.cubrid.cubridmigration.ui.wizard.page.CSVSelectPage;
import com.cubrid.cubridmigration.ui.wizard.page.CSVTargetDBSelectPage;
import com.cubrid.cubridmigration.ui.wizard.page.ConfirmationPage;
import com.cubrid.cubridmigration.ui.wizard.page.GraphMappingPage;
import com.cubrid.cubridmigration.ui.wizard.page.GraphTableSelectPage;
import com.cubrid.cubridmigration.ui.wizard.page.ObjectMappingPage;
import com.cubrid.cubridmigration.ui.wizard.page.SQLMigrationConfirmPage;
import com.cubrid.cubridmigration.ui.wizard.page.SQLSelectPage;
import com.cubrid.cubridmigration.ui.wizard.page.SQLTargetDBSelectPage;
import com.cubrid.cubridmigration.ui.wizard.page.SelectDestinationPage;
import com.cubrid.cubridmigration.ui.wizard.page.SelectSourcePage;
import com.cubrid.cubridmigration.ui.wizard.page.SelectSrcTarTypesPage;

/**
 * 
 * Migration Wizard
 * 
 * @author moulinwang fulei caoyilin
 * @version 1.0 - 2009-10-10
 * @version 2.0 - 2011-09-21
 * @version 3.0 - 2012-07
 */
public class MigrationWizard extends
		Wizard implements
		IMigrationWizardStatus {
	private static final int[] IDX_CSV = new int[] {0, 8, 9, 10};

	private static final int[] IDX_SQL = new int[] {0, 5, 6, 7};

	private static final int[] IDX_ONLINE = new int[] {0, 1, 2, 3, 4};
	
	//GDB index of GraphDB
	//maybe need graph only confirmation page (idx no. 13)
	private static final int[] IDX_GRAPH = new int[] {0, 1, 2, 12, 13};

	//private static final int[] IDX_OFFLINE = new int[]{0, 1, 2, 11, 3, 4 };

	//public static final String SELECTED = "Selected";
	private static final Logger LOG = LogUtil.getLogger(MigrationWizard.class);

	/**
	 * Retrieves the DB types which can be source database.
	 * 
	 * @return Set<Integer> of database type ids
	 */
	public static Set<Integer> getSupportedSrcDBTypes() {
		Set<Integer> supportedDBs = new HashSet<Integer>(5);
		supportedDBs.add(DatabaseType.MYSQL.getID());
		supportedDBs.add(DatabaseType.ORACLE.getID());
		supportedDBs.add(DatabaseType.CUBRID.getID());
		supportedDBs.add(DatabaseType.MSSQL.getID());
		supportedDBs.add(DatabaseType.GRAPH.getID());
		return supportedDBs;
	}

	/**
	 * Retrieves the DB types which can be target database.
	 * 
	 * @return Set<Integer> of database type ids
	 */
	public static Set<Integer> getSupportedTarDBTypes() {
		Set<Integer> supportedDBs = new HashSet<Integer>(4);
		supportedDBs.add(DatabaseType.CUBRID.getID());
		supportedDBs.add(DatabaseType.GRAPH.getID());
		return supportedDBs;
	}

	private ObjectMappingPage objMapPage;

	protected MigrationScript migrationScript;

	protected String migrationConfigFileName = null;
	protected MigrationConfiguration migrationConfig;

	protected Catalog sourceCatalog;

	protected Catalog targetCatalog;
	
	protected GraphDictionary graphDict = new GraphDictionary();

	protected DatabaseNode sourceDBNode;

	private boolean saveSchema;

	private boolean tarOfflineMode;
	private boolean srcOfflineMode;

	public MigrationWizard() {
		setWindowTitle(Messages.wizardTitle);
		setNeedsProgressMonitor(true);
		setDialogSettings(new DialogSettings("migration information"));
		migrationConfig = new MigrationConfiguration();
		migrationConfig.setName(CUBRIDTimeUtil.defaultFormatDateTime(new Date(
				System.currentTimeMillis())));

	}

	public MigrationWizard(MigrationScript migrationScript) {
		this(migrationScript.getAbstractConfigFileName());
		this.migrationScript = migrationScript;
		migrationConfig.setName(migrationScript.getName());
	}

	public MigrationWizard(String migrationFileName) {
		setWindowTitle(Messages.wizardTitle);
		setNeedsProgressMonitor(true);
		setDialogSettings(new DialogSettings("migration information"));
		//Load migration configuration
		File settingsFile = new File(migrationFileName);
		if (!settingsFile.exists()) {
			throw new RuntimeException("File(" + migrationFileName + ") does not exist");
		}
		this.migrationConfigFileName = migrationFileName;
		migrationConfig = MigrationTemplateParser.parse(migrationConfigFileName);
		autoSetUniqueNameOfConfiguration();
	}

	/**
	 * Auto update an unique name of the parsed configuration.
	 */
	protected void autoSetUniqueNameOfConfiguration() {
		int idx = 1;
		String postFix = "_" + idx;
		String cfgName = migrationConfig.getName();
		//If name is duplicated.
		while (MigrationScriptManager.getInstance().nameExists(cfgName, null)) {
			cfgName = migrationConfig.getName() + postFix;
			idx++;
			postFix = "_" + idx;
		}
		migrationConfig.setName(cfgName);
	}

	/**
	 * add page
	 */
	public void addPages() {
		addPage(new SelectSrcTarTypesPage("0"));

		addPage(new SelectSourcePage("1"));
		addPage(new SelectDestinationPage("2"));
		objMapPage = new ObjectMappingPage("3");
		addPage(objMapPage);
		addPage(new ConfirmationPage("4"));

		addPage(new SQLSelectPage("5"));
		addPage(new SQLTargetDBSelectPage("6"));
		addPage(new SQLMigrationConfirmPage("7"));

		addPage(new CSVTargetDBSelectPage("8"));
		addPage(new CSVSelectPage("9"));
		addPage(new CSVImportConfirmPage("10"));
		
		//GDB graph mapping page
		addPage(new GraphTableSelectPage("12"));
		addPage(new GraphMappingPage("13"));
		
		//addPage(new SelectOfflineDest2Page("11"));
		
	}

	/**
	 * Retrieve the finish button's enabled status
	 * 
	 * @return boolean true if it gets to the last page.
	 */
	public boolean canFinish() {
		final IWizardPage currentPage = getContainer().getCurrentPage();
		if (currentPage instanceof SQLMigrationConfirmPage
				|| currentPage instanceof ConfirmationPage
				|| currentPage instanceof CSVImportConfirmPage) {
			return true;
		}
		return false;
	}

	public MigrationConfiguration getMigrationConfig() {
		return migrationConfig;
	}

	public MigrationScript getMigrationScript() {
		return migrationScript;
	}

	/**
	 * Retrieves the next page according to the configuration.
	 * 
	 * @param page current page
	 * @return next page
	 */
	public IWizardPage getNextPage(IWizardPage page) {
		int[] indexes = getPageNOs();
		IWizardPage currentPage = null;
		IWizardPage nextPage = null;
		for (int i : indexes) {
			if (currentPage != null) {
				nextPage = getPage(String.valueOf(i));
				break;
			}
			if (getPage(String.valueOf(i)) == page) {
				currentPage = page;
			}
		}
		return nextPage;
	}

	/**
	 * Get the pages' numbers
	 * 
	 * @return int[]
	 */
	private int[] getPageNOs() {
		if (migrationConfig.targetIsGraph()){
			return IDX_GRAPH;
		}
		if (migrationConfig.sourceIsOnline() || migrationConfig.sourceIsXMLDump()) {
			//			if (migrationConfig.targetIsOffline()) {
			//				return IDX_OFFLINE;
			//			}
			return IDX_ONLINE;
		}

		if (migrationConfig.sourceIsCSV()) {
			return IDX_CSV;
		}
		if (migrationConfig.sourceIsSQL()) {
			return IDX_SQL;
		}
		return null;
	}

	/**
	 * Retrieves the previous page according to the configuration.
	 * 
	 * @param page current page
	 * @return previous page
	 */
	public IWizardPage getPreviousPage(IWizardPage page) {
		int[] indexes = getPageNOs();
		IWizardPage currentPage = null;
		IWizardPage nextPage = null;
		for (int i = indexes.length - 1; i >= 0; i--) {
			if (currentPage != null) {
				nextPage = getPage(String.valueOf(indexes[i]));
				break;
			}
			if (getPage(String.valueOf(indexes[i])) == page) {
				currentPage = page;
			}
		}
		return nextPage;
	}

	/**
	 * Retrieves the selected source database node.
	 * 
	 * @return DatabaseNode
	 */
	public DatabaseNode getSelectSourceDB() {
		return sourceDBNode;
	}

	public Catalog getSourceCatalog() {
		return sourceCatalog;
	}

	/**
	 * Retrieves the step no of this page.
	 * 
	 * @param currentPage IWizardPage
	 * @return start with 1.
	 */
	public String getStepNoMsg(IWizardPage currentPage) {
		int[] pns = getPageNOs();
		for (int i = 0; i < pns.length; i++) {
			if (currentPage == getPage(String.valueOf(pns[i]))) {
				return Messages.bind(Messages.msgWizardStep, new String[] {String.valueOf(i + 1),
						String.valueOf(pns.length)});
			}
		}
		throw new IllegalArgumentException("Invalid wizard page.");
	}

	/**
	 * Retrieves target database's catalog.
	 * 
	 * @return null if target database is not online.
	 */
	public Catalog getTargetCatalog() {
		return targetCatalog;
	}
	
	public GraphDictionary getGraphDictionary(){
		return graphDict;
	}

	public boolean isLoadMigrationScript() {
		return migrationConfigFileName != null;
	}

	public boolean isSaveSchema() {
		return saveSchema;
	}

	/**
	 * perform Cancel
	 * 
	 * @return boolean
	 */
	public boolean performCancel() {
		return MessageDialog.openConfirm(getShell(), Messages.msgConfirmation,
				Messages.wizardCancelMsg);

	}

	/**
	 * override Wizard method
	 * 
	 * @return boolean
	 */
	public boolean performFinish() {
		try {
			//Check OOM risk
			if (migrationConfig.checkOOMRisk()) {
				if (!MessageDialog.openConfirm(getShell(), Messages.msgConfirmation,
						Messages.msgOOMWarning)) {
					return false;
				}
			}
			MigrationRunModeDialog dialog = new MigrationRunModeDialog(getShell());
			dialog.setMigrationName(migrationConfig.getName());
			dialog.setScript(migrationScript);
			int result = dialog.open();
			if (result == IDialogConstants.OK_ID) {
				migrationConfig.setName(dialog.getMigrationName());
				startMigration();
				return true;
			} else if (result == IDialogConstants.NEXT_ID) {
				migrationConfig.setName(dialog.getMigrationName());
				migrationConfig.cleanNoUsedConfigForStart();
				saveMigrationScript(false, saveSchema);
				ScheduleMigrationTaskDialog dialogTR = new ScheduleMigrationTaskDialog(getShell(),
						migrationScript);
				if (dialogTR.open() == IDialogConstants.OK_ID) {
					MigrationScriptManager.getInstance().save();
					return true;
				}
			}
			return false;
		} catch (Exception e) {
			UICommonTool.openErrorBox(getShell(), e.getMessage());
			LOG.error("", e);
		}
		return true;
	}

	/**
	 * If source/target is online database and the databases can't be connected,
	 * the migration can't be started.
	 * 
	 * @return if the source/target databases can be connected.
	 */
	private boolean checkConnectionStatus() {
		if (migrationConfig.sourceIsOnline()) {
			try {
				Connection conn = migrationConfig.getSourceConParams().createConnection();
				conn.close();
			} catch (Exception e) {
				MessageDialog.openError(getShell(), Messages.msgError,
						"Source database can't be connected.");
				return false;
			}
		}
		if (migrationConfig.targetIsOnline()) {
			try {
				Connection conn = migrationConfig.getTargetConParams().createConnection();
				conn.close();
			} catch (Exception e) {
				MessageDialog.openError(getShell(), Messages.msgError,
						"Target database can't be connected.");
				return false;
			}
		}
		return true;
	}

	/**
	 * Source DB changed, rebuild target database schema
	 * 
	 */
	public void resetBySourceDBChanged() {
		objMapPage.setFirstVisible(true);
		if (isLoadMigrationScript()) {
			//Reload the migration configuration file
			MigrationConfiguration tempConfig = migrationConfig;
			migrationConfig = MigrationTemplateParser.parse(migrationConfigFileName);
			migrationConfig.setName(tempConfig.getName());
			//Copy target DB information to new migration configuration object 
			migrationConfig.setDestType(tempConfig.getDestType());
			migrationConfig.setCommitCount(tempConfig.getCommitCount());
			if (tempConfig.targetIsOnline()) {
				migrationConfig.setTargetConParams(tempConfig.getTargetConParams());
			} else if (tempConfig.targetIsFile()) {
				migrationConfig.setOneTableOneFile(tempConfig.isOneTableOneFile());
				migrationConfig.setFileRepositroyPath(tempConfig.getFileRepositroyPath());
				migrationConfig.setTargetIndexFileName(tempConfig.getTargetIndexFileName());
				migrationConfig.setTargetSchemaFileName(tempConfig.getTargetSchemaFileName());
				migrationConfig.setTargetDataFileName(tempConfig.getTargetDataFileName());
				migrationConfig.setTargetFileTimeZone(tempConfig.getTargetFileTimeZone());
				migrationConfig.getCsvSettings().copyFrom(tempConfig.getCsvSettings());
			}
		}
	}

	/**
	 * Save migration script to local configuration
	 * 
	 * @param createNew if true, a new migration script will be created and
	 *        replace the old migration script if the old is not null
	 * @param saveSchema if save schema into script file
	 */
	public void saveMigrationScript(boolean createNew, boolean saveSchema) {
		if (migrationScript == null || createNew) {
			migrationScript = MigrationScriptManager.getInstance().newScript(migrationConfig,
					saveSchema);
		} else {
			migrationScript.setName(migrationConfig.getName());
			MigrationTemplateParser.save(migrationConfig,
					migrationScript.getAbstractConfigFileName(), saveSchema);
			MigrationScriptManager.getInstance().save();

		}
	}

	public void setSaveSchema(boolean saveSchema) {
		this.saveSchema = saveSchema;
	}

	/**
	 * setSourceCatalog
	 * 
	 * @param sourceCatalog Catalog
	 */
	public void setSourceCatalog(Catalog sourceCatalog) {
		this.sourceCatalog = sourceCatalog;
		if (sourceCatalog == null) {
			sourceDBNode = null;
		} else {
			sourceDBNode = CubridNodeManager.getInstance().createDbNode(sourceCatalog,
					migrationConfig.sourceIsXMLDump() ? "MySQL dump file" : "Online");
		}
	}

	public void setTargetCatalog(Catalog targetCatalog) {
		this.targetCatalog = targetCatalog;
	}

	public void setGraphDict(GraphDictionary graphDict){
		this.graphDict = graphDict;
	}
	
	/**
	 * Close the configuration wizard and start migration.
	 * 
	 */
	protected void startMigration() {
		try {
			migrationConfig.cleanNoUsedConfigForStart();
			saveMigrationScript(false, saveSchema);
			if (!checkConnectionStatus()) {
				return;
			}
			String id = MigrationWizardFactory.getProgressEditorPartID(migrationConfig.getSourceType());
			PlatformUI.getWorkbench().getActiveWorkbenchWindow().getActivePage().openEditor(
					new MigrationProgressEditorInput(getMigrationConfig(), migrationScript), id);
		} catch (PartInitException e) {
			MessageDialog.openError(PlatformUI.getWorkbench().getDisplay().getActiveShell(),
					Messages.msgError, Messages.msgStartMigrationFailed);
		}
	}

	/**
	 * Update configuration's source type and target type.
	 * 
	 * @param srcType index of source type
	 * @param tarType index of target type
	 * @return true if update successfully.
	 */
	public boolean updateSrcTarType(int srcType, int tarType) {
		//Warning message : type changing will cause settings reset
		MigrationConfiguration cfg = migrationConfig;
		if (isLoadMigrationScript()) {
			if (srcType != cfg.getSourceType()) {
				if (!MessageDialog.openConfirm(getShell(), Messages.msgConfirmation,
						Messages.msgConfirmationTypeChanged)) {
					return false;
				}
			} else if (tarType != cfg.getDestType()) {
				if (!MessageDialog.openConfirm(getShell(), Messages.msgConfirmation,
						Messages.msgConfirmationTypeChanged)) {
					return false;
				}
			}
		}
		cfg.setSourceType(srcType);
		cfg.setDestType(tarType);
		return true;
	}

	/**
	 * Refresh some status of migration wizard.
	 */
	public void refreshWizardStatus() {
		tarOfflineMode = migrationConfig.isTargetOfflineMode();
		srcOfflineMode = migrationConfig.isSourceOfflineMode();
	}

	/**
	 * @return Retrieves true If source is a JDBC connection and can't be
	 *         connected
	 */
	public boolean isSourceOfflineMode() {
		return srcOfflineMode;
	}

	/**
	 * @return Retrieves true If target is a JDBC connection and can't be
	 *         connected
	 */
	public boolean isTargetOfflineMode() {
		return tarOfflineMode;
	}
}
