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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.mysql.MysqlXmlDumpSource;
import com.cubrid.cubridmigration.ui.common.UIConstant;
import com.cubrid.cubridmigration.ui.database.DatabaseConnectionInfo;
import com.cubrid.cubridmigration.ui.database.IJDBCConnectionFilter;
import com.cubrid.cubridmigration.ui.database.JDBCConnectionMgrView;
import com.cubrid.cubridmigration.ui.database.MysqlXmlDumpSchemaProgressFetcher;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;
import com.cubrid.cubridmigration.ui.wizard.dialog.RenameSchemaDialog;

/**
 * 
 * Select online database or mysql dump file as source database.
 * 
 * @author fulei caoyilin
 * @version 1.0 - 2011-09-21
 */
public class SelectSourcePage extends
		MigrationWizardPage {

	/**
	 * AbstractSourceView
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-6-3 created by Kevin Cao
	 */
	private static interface AbstractSourceView {
		/**
		 * Create controls
		 * 
		 * @param parent of the controls
		 */
		void createControls(Composite parent);

		/**
		 * Retrieves the catalog
		 * 
		 * @return Catalog
		 */
		Catalog getCatalog();

		/**
		 * Hide view
		 */
		void hide();

		/**
		 * Initialize the view
		 * 
		 */
		void init();

		/**
		 * check whether the dialog changed
		 * 
		 * @return true if content changed
		 */
		boolean isInputChanged();

		/**
		 * Save to wizard
		 * 
		 * @return true if successfully
		 */
		boolean save();

		/**
		 * Show view
		 */
		void show();

	}

	/**
	 * Select MySQL Dump file as source.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-5-4 created by Kevin Cao
	 */
	private class SelectMySQLDumpSrcView implements
			AbstractSourceView {
		private Text txtXMLFile;
		private Button btnBrowse;
		private Button btnAnalyz;
		private Combo cboFileCharset;
		private Combo cobTimezone;
		private Group grpXML;
		private MysqlXmlDumpSource parsedSource;
		private Catalog xmlCatalog = null;

		/**
		 * Validate the char-set input.
		 * 
		 * @return true if all are valid.
		 */
		private boolean checkCharset() {
			if (StringUtils.isBlank(cboFileCharset.getItem(cboFileCharset.getSelectionIndex()))) {
				MessageDialog.openError(getShell(), Messages.msgError,
						Messages.sourceDBPageErrNoSetXMLFileCharset);
				return false;
			}
			return true;
		}

		/**
		 * Validate the input.
		 * 
		 * @return true if all are valid.
		 */
		private boolean checkInput() {
			if (StringUtils.isBlank(txtXMLFile.getText())) {
				MessageDialog.openError(getShell(), Messages.msgError,
						Messages.sourceDBPageErrNoSelectedXMLFile);
				return false;
			}
			return checkCharset();
		}

		/**
		 * Create controls
		 * 
		 * @param parent of the controls
		 */
		public void createControls(Composite parent) {
			if (grpXML != null) {
				return;
			}
			grpXML = new Group(parent, SWT.SHADOW_ETCHED_IN);
			grpXML.setLayout(new GridLayout(4, false));
			GridData groupGridData2 = new GridData(SWT.FILL, SWT.FILL, true, false);
			groupGridData2.heightHint = 98;
			grpXML.setLayoutData(groupGridData2);

			Label xmlFilePathLabel = new Label(grpXML, SWT.NONE);
			xmlFilePathLabel.setText(Messages.lblXMLFilePath);
			xmlFilePathLabel.setLayoutData(new GridData(SWT.END, SWT.CENTER, false, false));

			txtXMLFile = new Text(grpXML, SWT.BORDER | SWT.READ_ONLY);
			GridData gdXmlFilePathTxt = new GridData(SWT.FILL, SWT.CENTER, true, false);
			gdXmlFilePathTxt.grabExcessHorizontalSpace = true;
			txtXMLFile.setLayoutData(gdXmlFilePathTxt);

			btnBrowse = new Button(grpXML, SWT.NONE);
			GridData xmlButtonGd = new GridData(SWT.RIGHT, SWT.CENTER, false, false);
			xmlButtonGd.minimumWidth = 70;
			btnBrowse.setLayoutData(xmlButtonGd);
			btnBrowse.setText(Messages.btnBrowse);
			btnBrowse.setToolTipText(Messages.ttSelectXMLDumpFile);
			btnBrowse.addSelectionListener(new SelectionAdapter() {
				public void widgetSelected(final SelectionEvent event) {
					if (!checkCharset()) {
						return;
					}
					FileDialog dlg = new FileDialog(
							PlatformUI.getWorkbench().getDisplay().getActiveShell(), SWT.SINGLE
									| SWT.OPEN);
					dlg.setFilterPath(".");
					dlg.setFilterExtensions(new String[] {"*.xml"});
					dlg.setFilterNames(new String[] {"*.xml"});
					final String xmlFileTmp = dlg.open();
					if (xmlFileTmp == null) {
						return;
					}
					txtXMLFile.setText(xmlFileTmp);
					btnAnalyz.setEnabled(true);
					try {
						xmlCatalog = getXmlCatalog(true);
					} catch (Exception e) {
						LOG.error(e.getMessage());
					}
				}
			});

			btnAnalyz = new Button(grpXML, SWT.NONE);
			GridData gdAnalyzXMLFile = new GridData(SWT.RIGHT, SWT.CENTER, false, false);
			gdAnalyzXMLFile.minimumWidth = 70;
			btnAnalyz.setLayoutData(gdAnalyzXMLFile);
			btnAnalyz.setText(Messages.btnAnalyze);
			btnAnalyz.setToolTipText(Messages.ttAnalyzXMLFile);
			btnAnalyz.setEnabled(false);
			btnAnalyz.addSelectionListener(new SelectionAdapter() {
				public void widgetSelected(final SelectionEvent event) {
					if (!checkInput()) {
						return;
					}
					try {
						xmlCatalog = getXmlCatalog(false);
						if (null != xmlCatalog) {
							getMigrationWizard().resetBySourceDBChanged();
						}
					} catch (Exception e) {
						LOG.error(e.getMessage());
					}
				}
			});

			Label charsetLabel = new Label(grpXML, SWT.NONE);
			charsetLabel.setText(Messages.lblXMLFileCharset);
			charsetLabel.setLayoutData(new GridData(SWT.END, SWT.CENTER, false, false));
			cboFileCharset = new Combo(grpXML, SWT.READ_ONLY);
			final GridData gdCharsetCombo = new GridData(SWT.FILL, SWT.CENTER, true, false);
			cboFileCharset.setLayoutData(gdCharsetCombo);
			cboFileCharset.setItems(com.cubrid.cubridmigration.core.common.CharsetUtils.getCharsets());
			cboFileCharset.select(1);

			new Label(grpXML, SWT.NONE);
			new Label(grpXML, SWT.NONE);

			Label lblTZ = new Label(grpXML, SWT.NONE);
			lblTZ.setText(Messages.lblXMLFileTimezone);
			lblTZ.setLayoutData(new GridData(SWT.END, SWT.CENTER, false, false));

			cobTimezone = new Combo(grpXML, SWT.READ_ONLY);
			final GridData gdTimezoneCombo = new GridData(SWT.FILL, SWT.TOP, true, false);
			gdTimezoneCombo.widthHint = 600;
			cobTimezone.setLayoutData(gdTimezoneCombo);
			cobTimezone.setVisibleItemCount(20);
			List<String> allTimeZones = TimeZoneUtils.getTimeZonesList();
			cobTimezone.setItems(allTimeZones.toArray(new String[allTimeZones.size()]));
			cobTimezone.add(Messages.msgDefault, 0);
			cobTimezone.select(0);
		}

		/**
		 * Retrieves the catalog.
		 * 
		 * @return catalog
		 */
		public Catalog getCatalog() {
			if ((parsedSource != null
					&& parsedSource.getFileName().equals(txtXMLFile.getText().trim()) && parsedSource.getCharset().equalsIgnoreCase(
					cboFileCharset.getText()))
					&& xmlCatalog != null) {
				return xmlCatalog;
			} else {
				return getXmlCatalog(true);
			}
		}

		/**
		 * return xml catalog
		 * 
		 * @param hisFirst reading the parsing history firstly.
		 * @return Catalog
		 */
		private Catalog getXmlCatalog(boolean hisFirst) {
			//Reading parsing history firstly.
			String xmlFileCharset = cboFileCharset.getItem(cboFileCharset.getSelectionIndex());
			String xmlFile = txtXMLFile.getText();
			final File file = new File(xmlFile);
			if (!file.exists() || !file.isFile()) {
				MessageDialog.openError(getShell(), Messages.msgError,
						Messages.errInvalidMysqlDumpFile);
				return null;
			}
			parsedSource = new MysqlXmlDumpSource(xmlFile, xmlFileCharset);
			Catalog catalog = MysqlXmlDumpSchemaProgressFetcher.fetch(parsedSource, hisFirst);
			if (catalog == null) {
				parsedSource = null;
				return null;
			}
			//Update timezone.
			String newTimezoneTxt = cobTimezone.getText();
			String timezone;
			if (newTimezoneTxt.equalsIgnoreCase(UIConstant.DEFAULT_TIME_ZONE)) {
				timezone = TimeZoneUtils.getDefaultID2GMT();
			} else {
				timezone = TimeZoneUtils.getGMTByDisplay(newTimezoneTxt);
			}
			catalog.setTimezone(timezone);
			return catalog;
		}

		/**
		 * Hide
		 */
		public void hide() {
			if (grpXML == null) {
				return;
			}
			grpXML.setVisible(false);
			((GridData) grpXML.getLayoutData()).exclude = true;
		}

		/**
		 * Initialize
		 */
		public void init() {
			setTitle(getMigrationWizard().getStepNoMsg(SelectSourcePage.this)
					+ Messages.msgSrcSelectMySQLDump);
			setMessage(Messages.msgSrcSelectMySQLDumpDes);

			final MigrationWizard wizard = getMigrationWizard();
			final MigrationConfiguration config = wizard.getMigrationConfig();
			//if load script and use MYSQL dump, load the XML catalog
			if (wizard.isLoadMigrationScript()) {
				parsedSource = new MysqlXmlDumpSource(config.getSourceFileName(),
						config.getSourceFileEncoding());
				txtXMLFile.setText(config.getSourceFileName() == null ? ""
						: config.getSourceFileName());
				cboFileCharset.setText(config.getSourceFileEncoding() == null ? ""
						: config.getSourceFileEncoding());
				cobTimezone.setText(config.getSourceFileTimeZone() == null ? UIConstant.DEFAULT_TIME_ZONE
						: config.getSourceFileTimeZone());
			}

		}

		/**
		 * check whether the dialog changed
		 * 
		 * @return true if content changed
		 */
		public boolean isInputChanged() {
			boolean srcDBChanged = false;
			MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
			// if online is saved but not selected or dumpfile is saved but not selected
			String xmlFile = config.getSourceFileName();
			String xmlFileCharset = config.getSourceFileEncoding();
			if (!(txtXMLFile.getText().equals(xmlFile) && xmlFileCharset.equals(cboFileCharset.getItem(cboFileCharset.getSelectionIndex())))) {
				srcDBChanged = true;
			}
			// online saved but but some of the parameters changed
			return srcDBChanged;
		}

		/**
		 * Check and save
		 * 
		 * @return true if saved.
		 */
		public boolean save() {
			if (!checkInput()) {
				return false;
			}
			Catalog catalog = getCatalog();
			if (null == catalog) {
				return false;
			}
			final MigrationWizard wzd = getMigrationWizard();
			if (isInputChanged()) {
				//If it is a new migration, initialize the configuration
				wzd.resetBySourceDBChanged();
			}
			wzd.setSourceCatalog(catalog);
			MigrationConfiguration cfg = wzd.getMigrationConfig();
			cfg.setSourceType(MigrationConfiguration.XML);
			cfg.setSourceFileName(txtXMLFile.getText());
			cfg.setSourceFileEncoding(cboFileCharset.getItem(cboFileCharset.getSelectionIndex()));
			cfg.setSourceFileTimeZone(cobTimezone.getItem(cobTimezone.getSelectionIndex()));
			cfg.clearAllSQLTables();
			xmlCatalog = catalog;
			xmlCatalog.setCharset(cboFileCharset.getItem(cboFileCharset.getSelectionIndex()));
			xmlCatalog.setTimezone(cobTimezone.getItem(cobTimezone.getSelectionIndex()));
			return true;
		}

		/**
		 * Show
		 */
		public void show() {
			if (grpXML == null) {
				return;
			}
			grpXML.setVisible(true);
			((GridData) grpXML.getLayoutData()).exclude = false;
		}
	}

	/**
	 * SelectOnlineSrcView Description
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-4-23 created by Kevin Cao
	 */
	private class SelectOnlineSrcView implements
			AbstractSourceView {

		private final JDBCConnectionMgrView conMgrView;

		private SelectOnlineSrcView() {
			conMgrView = new JDBCConnectionMgrView(MigrationWizard.getSupportedSrcDBTypes(),
					new IJDBCConnectionFilter() {

						public boolean doFilter(ConnParameters cp) {
							return getMigrationWizard().getMigrationConfig().getSourceType() != cp.getDatabaseType().getID();
						}

					});
		}

		/**
		 * Create controls
		 * 
		 * @param parent of the controls
		 */
		public void createControls(Composite parent) {
			conMgrView.createControls(parent);
		}

		/**
		 * get Catalog
		 * 
		 * @return Catalog
		 */
		public Catalog getCatalog() {
			return conMgrView.getCatalog();
		}

		/**
		 * Hide
		 */
		public void hide() {
			conMgrView.hide();
		}

		/**
		 * Initialize with script's source connection
		 */
		public void init() {
			MigrationWizard wzd = getMigrationWizard();
			setTitle(wzd.getStepNoMsg(SelectSourcePage.this) + Messages.msgSrcSelectOnlineDB);
			setMessage(Messages.msgSrcSelectOnlineDBDes);
			List<Integer> dts = new ArrayList<Integer>();
			MigrationConfiguration cfg = wzd.getMigrationConfig();
			dts.add(cfg.getSourceType());
			conMgrView.setSupportedDBType(dts);
			//Add catalog to cache.
			Catalog offlineSrcCatalog = cfg.getOfflineSrcCatalog();
			ConnParameters srcConParams = cfg.getSourceConParams();
			conMgrView.init(srcConParams, offlineSrcCatalog);

		}

		/**
		 * check whether the dialog changed
		 * 
		 * @return true if content changed
		 */
		public boolean isInputChanged() {
			boolean srcDBChanged = false;
			MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
			// if online is saved but not selected or dumpfile is saved but not selected
			ConnParameters oldCP = config.getSourceConParams();
			// the first time set it changed
			DatabaseConnectionInfo dci = conMgrView.getSelectedDCI();
			if (oldCP == null && dci != null) {
				srcDBChanged = true;
			} else if (oldCP != null) {
				srcDBChanged = !oldCP.isSameDB(dci.getConnParameters());
			}
			return srcDBChanged;
		}

		/**
		 * Save to configurations
		 * 
		 * @return true if successfully
		 */
		public boolean save() {
			if (this.conMgrView.getSelectedDCI() == null) {
				MessageDialog.openError(getShell(), Messages.msgError,
						Messages.sourceDBPageErrNoSelectedItem);
				return false;
			}
			final MigrationWizard wzd = getMigrationWizard();
			Catalog catalog = getCatalog();
			if (catalog == null) {
				return false;
			}

			//
			List<String> errorSchemas = new ArrayList<String>();
			Map<String, String> old2NewSchemaMapping = new HashMap<String, String>();
			MigrationConfiguration cfg = wzd.getMigrationConfig();
			cfg.resetSchemaInfo();
			if (catalog.getDatabaseType().isSupportMultiSchema()
					&& !cfg.getExpEntryTableCfg().isEmpty()) {
				List<String> expSchemas = cfg.getExpSchemaNames();
				for (String schema : expSchemas) {
					if (catalog.getSchemaByName(schema) != null) {
						continue;
					}
					errorSchemas.add(schema);
				}
				if (!errorSchemas.isEmpty()) {
					List<String> newSchemas = new ArrayList<String>();
					for (Schema newSchema : catalog.getSchemas()) {
						newSchemas.add(newSchema.getName());
					}
					old2NewSchemaMapping = RenameSchemaDialog.renameSchemas(errorSchemas,
							newSchemas);
					//Dialog canceled, user maybe want to choose another source.  
					if (old2NewSchemaMapping == null) {
						return false;
					}
				}
			}

			if (isInputChanged() || wzd.getSourceCatalog() != catalog) {
				//If it is a new migration, initialize the configuration
				wzd.resetBySourceDBChanged();
				cfg = wzd.getMigrationConfig();
			}
			wzd.setSourceCatalog(catalog);
			cfg.setSourceConParams(catalog.getConnectionParameters());
			//Set the invalid schema to right schema or remove them.
			for (String es : errorSchemas) {
				String newSchema = old2NewSchemaMapping.get(es);
				if (StringUtils.isBlank(newSchema)) {
					cfg.removeExpSchema(es);
				} else {
					cfg.renameExpSchema(es, newSchema);
				}
			}
			return true;
		}

		/**
		 * Show
		 */
		public void show() {
			conMgrView.show();
		}
	}

	private static final Logger LOG = LogUtil.getLogger(SelectSourcePage.class);
	private AbstractSourceView onlineView = new SelectOnlineSrcView();
	private AbstractSourceView mysqlDumpView = new SelectMySQLDumpSrcView();

	private Composite container;

	public SelectSourcePage(String pageName) {
		super(pageName);
	}

	/**
	 * When migration wizard displayed current page.
	 * 
	 * @param event PageChangedEvent
	 */

	protected void afterShowCurrentPage(PageChangedEvent event) {
		try {
			final MigrationWizard wzd = getMigrationWizard();
			if (wzd.getMigrationConfig().sourceIsOnline()) {
				onlineView.createControls(container);
				mysqlDumpView.hide();
				onlineView.show();
			} else if (wzd.getMigrationConfig().sourceIsXMLDump()) {
				mysqlDumpView.createControls(container);
				onlineView.hide();
				mysqlDumpView.show();
			}
			container.layout(true);
			getCurrentView().init();
		} catch (Exception ex) {
			LOG.error("", ex);
			MessageDialog.openError(getShell(), Messages.msgError, ex.getMessage());
		}
	}

	/**
	 * Create contents of the wizard
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		container = new Composite(parent, SWT.NONE);
		final GridLayout gridLayoutRoot = new GridLayout();
		container.setLayout(gridLayoutRoot);
		setControl(container);
	}

	/**
	 * Retrieves the current view for selection data source.
	 * 
	 * @return AbstractView
	 */
	private AbstractSourceView getCurrentView() {
		final MigrationConfiguration cfg = getMigrationWizard().getMigrationConfig();
		if (cfg.sourceIsOnline()) {
			return onlineView;
		} else if (cfg.sourceIsXMLDump()) {
			return mysqlDumpView;
		}
		throw new RuntimeException("Can't support source type :" + cfg.getSourceType());
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
		if (!isGotoNextPage(event)) {
			return;
		}
		event.doit = updateMigrationConfig();
	}

	/**
	 * Save user input (source database connection information) to export
	 * options.
	 * 
	 * @return true if update success.
	 */
	protected boolean updateMigrationConfig() {
		return getCurrentView().save();
	}
}
