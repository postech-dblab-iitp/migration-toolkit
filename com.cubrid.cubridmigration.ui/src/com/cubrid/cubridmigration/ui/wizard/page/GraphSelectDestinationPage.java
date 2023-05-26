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
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.core.common.CharsetUtils;
import com.cubrid.cubridmigration.core.common.CommonUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.common.Status;
import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.database.IJDBCConnectionFilter;
import com.cubrid.cubridmigration.ui.database.JDBCConnectionMgrView;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;
import com.cubrid.cubridmigration.ui.wizard.dialog.CSVSettingsDialog;
import com.cubrid.cubridmigration.ui.wizard.page.view.AbstractDestinationView;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;

/**
 * new wizard step 3. Select target database connection or choose OFF Line
 * model. database
 * 
 * @author fulei caoyilin
 * @version 1.0 - 2011-09-28
 */
public class GraphSelectDestinationPage extends
		MigrationWizardPage {

	/**
	 * OnlineTargetDBView provides settings exporting to a online CUBRID DB.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-10-9 created by Kevin Cao
	 */
	private class OnlineTargetDBView extends
			AbstractDestinationView {
		private final JDBCConnectionMgrView conMgrView;

		private OnlineTargetDBView() {
			conMgrView = new JDBCConnectionMgrView(MigrationWizard.getSupportedTarDBTypes(),
					new IJDBCConnectionFilter() {

						//GDB filter source online. need rewrite
						public boolean doFilter(ConnParameters cp) {
							final MigrationConfiguration cfg = getMigrationWizard().getMigrationConfig();
							/* if (cfg.targetIsOnline()) {
								return cfg.getSourceConParams().isSameDB(cp);
							}
							return false; */
							return cfg.getDestType() != cp.getDatabaseType().getID();
						}
					});
		}

		/**
		 * Create Controls
		 * 
		 * @param parent Composite
		 */
		public void createControls(Composite parent) {
			conMgrView.createControls(parent);

		}

		/**
		 * Hide view
		 */
		public void hide() {
			conMgrView.hide();
		}

		/**
		 * initial the page set which option is visiable and updateDialogStatus
		 */
		public void init() {
			setTitle(getMigrationWizard().getStepNoMsg(GraphSelectDestinationPage.this)
					+ Messages.msgDestSelectOnlineGRAPHDB);
			setDescription(Messages.msgDestSelectOnlineGRAPHDBDes);
			final MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
			List<Integer> dts = new ArrayList<Integer>();
			dts.add(config.getDestType());
			conMgrView.setSupportedDBType(dts);
			conMgrView.init(config.getTargetConParams(), null);
		}

		/**
		 * Save UI
		 * 
		 * @return true if saving successfully
		 */
		public boolean save() {
			if (conMgrView.getSelectedDCI() == null) {
				MessageDialog.openError(getShell(), Messages.msgError,
						Messages.sourceDBPageErrNoSelectedItem);
				return false;
			}
			final MigrationWizard wzd = getMigrationWizard();
			final MigrationConfiguration config = wzd.getMigrationConfig();
			ConnParameters connParameters = conMgrView.getSelectedDCI().getConnParameters();
			config.setTargetConParams(connParameters);

			//check connection 
			try {
                Connection conn = config.getTargetConParams().createConnection();
                conn.close();
            } catch (SQLException e) {
                DetailMessageDialog.openInfo(getShell(), Messages.msgError, Messages.commonToolMysqlMsg5, e.toString());
                return false;
            }
 
			return true;

		}

		/**
		 * displayOnlineContainer
		 */
		public void show() {
			conMgrView.show();
		}

	}

	/**
	 * UnloadTargetDBView provides setting of export to unloadDB files
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-10-9 created by Kevin Cao
	 */
	private class UnloadTargetDBView extends
			AbstractDestinationView {
		private Composite fileRepositoryContainer;
		private Text txtFileRepository;
		private Text txtFilePrefix;

		private Button btnOneTableOneFile;
		private Combo targetFileTimezoneCombo;

		private Button btnCSVSetting;
		private String fileExt = ".txt";

		private Combo cboCharset;
		private Label lblCharset;
		private Label lblCharsetSP;
		private Label lblLobPath;
		private Text txtLobPath;

		/**
		 * checkFileRepositroy
		 * 
		 * @return boolean
		 */
		private boolean checkFileRepositroy() {
			File schema = new File(getSchemaFullName());
			File index = new File(getIndexFullName());
			File data = new File(getDataFullName());
			StringBuffer buffer = new StringBuffer();
			try {
				if (schema.exists()) {
					buffer.append(schema.getCanonicalPath()).append("\r\n");
				}
				if (index.exists()) {
					buffer.append(index.getCanonicalPath()).append("\r\n");
				}
				if (!btnOneTableOneFile.getSelection() && data.exists()) {
					buffer.append(data.getCanonicalPath()).append("\r\n");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (buffer.length() > 0) {
				return MessageDialog.openConfirm(
						PlatformUI.getWorkbench().getDisplay().getActiveShell(),
						Messages.msgConfirmation,
						Messages.fileWarningMessage + "\r\n" + buffer.toString() + "\r\n"
								+ Messages.confirmMessage);
			}
			return true;
		}

		/**
		 * Create Controls
		 * 
		 * @param parent Composite
		 */
		public void createControls(Composite parent) {
			if (fileRepositoryContainer != null) {
				return;
			}
			fileRepositoryContainer = new Composite(parent, SWT.BORDER);
			fileRepositoryContainer.setLayout(new GridLayout(3, false));
			fileRepositoryContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

			final Label fileLabel = new Label(fileRepositoryContainer, SWT.NONE);
			fileLabel.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			fileLabel.setText(Messages.targetDBPageFileRepositoryLabel);

			txtFileRepository = new Text(fileRepositoryContainer, SWT.BORDER);
			final GridData textLength = new GridData(SWT.FILL, SWT.CENTER, true, false);
			txtFileRepository.setLayoutData(textLength);
			txtFileRepository.setEditable(false);
			String mergePath = PathUtils.mergePath(PathUtils.getInstallPath(), "output"
					+ File.separator);
			mergePath = PathUtils.getLocalHostFilePath(mergePath);
			txtFileRepository.setText(mergePath);

			Button btnPath = new Button(fileRepositoryContainer, SWT.NONE);
			final GridData buttonGd = new GridData(SWT.LEFT, SWT.CENTER, false, false);
			buttonGd.minimumWidth = 70;
			btnPath.setLayoutData(buttonGd);
			btnPath.setText(Messages.btnBrowse);
			btnPath.addSelectionListener(new SelectionAdapter() {
				public void widgetSelected(final SelectionEvent event) {
					DirectoryDialog dialog = new DirectoryDialog(
							PlatformUI.getWorkbench().getDisplay().getActiveShell());
					dialog.setFilterPath(getFileRepository());

					String dir = dialog.open();
					if (dir != null) {
						if (!dir.endsWith(File.separator)) {
							dir += File.separator;
						}
						txtFileRepository.setText(dir);
						firePageStatusChanged(null);
					}
				}
			});

			final Label lblFilePrefix = new Label(fileRepositoryContainer, SWT.NONE);
			lblFilePrefix.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
//			lblFilePrefix.setText(Messages.lblOutputFilePrefix);

			txtFilePrefix = new Text(fileRepositoryContainer, SWT.BORDER);
			txtFilePrefix.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

			btnCSVSetting = new Button(fileRepositoryContainer, SWT.NONE);
			btnCSVSetting.setText(Messages.btnCSVSettings);
			btnCSVSetting.setToolTipText(Messages.ttCSVSettings);
			final MigrationConfiguration cfg = getMigrationWizard().getMigrationConfig();
			btnCSVSetting.addSelectionListener(new SelectionAdapter() {
				public void widgetSelected(SelectionEvent se) {
					CSVSettingsDialog dialog = new CSVSettingsDialog(getShell(), cfg);
					dialog.open();
				}
			});

			Label timezoneLabel = new Label(fileRepositoryContainer, SWT.NONE);
//			timezoneLabel.setText(Messages.lblXMLFileTimezone);
			timezoneLabel.setLayoutData(new GridData(SWT.END, SWT.CENTER, false, false));

			targetFileTimezoneCombo = new Combo(fileRepositoryContainer, SWT.READ_ONLY);
			targetFileTimezoneCombo.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			targetFileTimezoneCombo.setVisibleItemCount(20);
			List<String> allTimeZones = TimeZoneUtils.getTimeZonesList();
			targetFileTimezoneCombo.setItems(allTimeZones.toArray(new String[allTimeZones.size()]));
			targetFileTimezoneCombo.add(Messages.targetDBPageComboDefault, 0);
			targetFileTimezoneCombo.select(0);
			new Label(fileRepositoryContainer, SWT.NONE);

			lblCharset = new Label(fileRepositoryContainer, SWT.NONE);
//			lblCharset.setText(Messages.lblCharset);
			lblCharset.setLayoutData(new GridData(SWT.END, SWT.CENTER, false, false));

			cboCharset = new Combo(fileRepositoryContainer, SWT.READ_ONLY);
			cboCharset.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			cboCharset.setItems(CharsetUtils.getCharsets());
			cboCharset.remove(0);

			lblCharsetSP = new Label(fileRepositoryContainer, SWT.NONE);
			lblCharsetSP.setLayoutData(new GridData());
			lblCharsetSP.setVisible(false);

			new Label(fileRepositoryContainer, SWT.NONE);
			btnOneTableOneFile = new Button(fileRepositoryContainer, SWT.CHECK);
			btnOneTableOneFile.setLayoutData(new GridData(SWT.LEFT, SWT.TOP, false, false));
//			btnOneTableOneFile.setText(Messages.btnOneTableOneFile);
			new Label(fileRepositoryContainer, SWT.NONE);

			lblLobPath = new Label(fileRepositoryContainer, SWT.NONE);
			lblLobPath.setLayoutData(new GridData(SWT.END, SWT.CENTER, false, false));
//			lblLobPath.setText("LOB files' root path: ");
			txtLobPath = new Text(fileRepositoryContainer, SWT.BORDER);
			txtLobPath.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
//			txtLobPath.setText("");
			txtLobPath.addModifyListener(new ModifyListener() {

				public void modifyText(ModifyEvent ev) {
					if (StringUtils.isBlank(txtLobPath.getText())) {
						return;
					}
					String pathTtt = Messages.ttLobFileFullPathHeader + txtLobPath.getText();
					if (!(pathTtt.endsWith("\\") || pathTtt.endsWith("/"))) {
						pathTtt = pathTtt + "/";
					}
					pathTtt = pathTtt + "lob/{table}/{lob file}]";
					txtLobPath.setToolTipText(pathTtt);
				}
			});
		}

		/**
		 * Get output files prefix for output files name
		 * 
		 * @return prefix.
		 */
		private String getFilePrefix() {
			String result = txtFilePrefix.getText().trim();
			if (StringUtils.isEmpty(result)) {
				return "";
			}
			return result + "_";
		}

		private String getDataFullName() {
			return new StringBuffer(getFileRepository()).append(getFilePrefix()).append("data").append(
					fileExt).toString();
		}

		/**
		 * The file repository with File.separator ended.
		 * 
		 * @return String
		 */
		private String getFileRepository() {
			String text = txtFileRepository.getText();
			if (!text.endsWith(File.separator)) {
				text = text + File.separator;
			}
			return text;
		}

		private String getIndexFullName() {
			return new StringBuffer(getFileRepository()).append(getFilePrefix()).append("index").append(
					getMigrationWizard().getMigrationConfig().getDefaultTargetSchemaFileExtName()).toString();
		}

		private String getSchemaFullName() {
			return new StringBuffer(getFileRepository()).append(getFilePrefix()).append("schema").append(
					getMigrationWizard().getMigrationConfig().getDefaultTargetSchemaFileExtName()).toString();
		}

		/**
		 * Hide view
		 */
		public void hide() {
			if (fileRepositoryContainer == null) {
				return;
			}
			GridData fileRepositorygd = (GridData) fileRepositoryContainer.getLayoutData();
			fileRepositorygd.exclude = true;
			fileRepositoryContainer.setVisible(false);
		}

		/**
		 * Initialize
		 */
		public void init() {
			setTitle(getMigrationWizard().getStepNoMsg(GraphSelectDestinationPage.this)
					+ Messages.msgDestOutputFilesSetting);
			setDescription(Messages.msgDestOutputFilesSettingDes);

			MigrationWizard wizard = getMigrationWizard();
			final Schema schema = wizard.getSourceCatalog().getSchemas().get(0);
			String schemaName = "";
			if (schema != null) {
				schemaName = schema.getName();
			}
			MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
			fileExt = config.getDataFileExt();
			btnCSVSetting.setVisible(config.targetIsCSV());

			// final boolean isChar = config.targetIsCSV() ||
			// config.targetIsSQL();
			// lblCharset.setVisible(isChar);
			// GridData gd = (GridData) lblCharset.getLayoutData();
			// gd.exclude = !isChar;
			// cboCharset.setVisible(isChar);
			// gd = (GridData) cboCharset.getLayoutData();
			// gd.exclude = !isChar;
			// gd = (GridData) lblCharsetSP.getLayoutData();
			// gd.exclude = !isChar;

			final boolean oneToneF = config.targetIsDBDump() || config.targetIsSQL();
			btnOneTableOneFile.setVisible(oneToneF);
			btnOneTableOneFile.setSelection(oneToneF && config.isOneTableOneFile());
			if (config.getFileRepositroyPath() != null) {
				txtFileRepository.setText(config.getFileRepositroyPath());
			}
			txtFilePrefix.setText(config.getTargetFilePrefix() == null ? schemaName
					: config.getTargetFilePrefix());
			if (config.getTargetFileTimeZone() != null) {
				targetFileTimezoneCombo.setText(config.getTargetFileTimeZone());
			}
			cboCharset.setText(config.getTargetCharSet());
			lblLobPath.setVisible(config.targetIsDBDump());
			txtLobPath.setVisible(config.targetIsDBDump());
			if (config.getTargetLOBRootPath() != null) {
				txtLobPath.setText(config.getTargetLOBRootPath());
			}
			fileRepositoryContainer.layout();
			
			btnOneTableOneFile.setVisible(false);
			txtFilePrefix.setVisible(false);
			targetFileTimezoneCombo.setVisible(false);
			cboCharset.setVisible(false);
			lblLobPath.setVisible(false);
			txtLobPath.setVisible(false);
			
		}

		/**
		 * Save view to configuration
		 * 
		 * @return true if saving successfully
		 */
		public boolean save() {
			if (!checkFileRepositroy()) {
				return false;
			}
			MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
			if (config.targetIsDBDump()) {
				config.setOneTableOneFile(btnOneTableOneFile.getSelection());
				config.setTargetLOBRootPath(txtLobPath.getText());
			} else if (config.targetIsSQL()) {
				config.setOneTableOneFile(btnOneTableOneFile.getSelection());
			} else {
				config.setOneTableOneFile(true);
			}
			config.setFileRepositroyPath(getFileRepository());
			config.setTargetFilePrefix(txtFilePrefix.getText());
			config.setTargetIndexFileName(getIndexFullName());
			config.setTargetSchemaFileName(getSchemaFullName());
			config.setTargetDataFileName(getDataFullName());
			config.setTargetFileTimeZone(targetFileTimezoneCombo.getItem(targetFileTimezoneCombo.getSelectionIndex()));
			config.setTargetCharSet(cboCharset.getText());
			return true;
		}

		/**
		 * Show view
		 */
		public void show() {
			GridData fileRepositorygd = (GridData) fileRepositoryContainer.getLayoutData();
			fileRepositorygd.exclude = false;
			fileRepositoryContainer.setVisible(true);
		}

		/**
		 * Update parent UI status
		 * 
		 * @param statusList List<IStatus>
		 */
		public void updateStatus(List<IStatus> statusList) {
			if (StringUtils.isBlank(txtFileRepository.getText())) {
				IStatus offLineErrStatus = new Status(IStatus.ERROR,
						Messages.targetDBPageOfflineTargetFileRepositoryErrMsg);
				statusList.add(offLineErrStatus);
			} else if (StringUtils.isBlank(txtFilePrefix.getText())) {
				IStatus status = new Status(IStatus.ERROR, Messages.targetDBPageOfflineErrMsg2);
				statusList.add(status);
				txtFilePrefix.forceFocus();
			} else if (!verifyName(txtFilePrefix.getText())) {
				IStatus status = new Status(IStatus.ERROR, Messages.msgErrInvalidFilePrefix
						+ txtFilePrefix.getText());
				statusList.add(status);
				txtFilePrefix.forceFocus();
			}
		}

		/**
		 * verify
		 * 
		 * @param name String
		 * @return boolean
		 */
		private boolean verifyName(String name) {
			boolean isValid = true;
			if (StringUtils.isBlank(name)) {
				isValid = false;
			} else {
				String retstr = CommonUtils.validateCheckInIdentifier(name);

				if (retstr.length() > 0) {
					isValid = false;
				} else if (!CommonUtils.isASCII(name)) {
					isValid = false;
				}
			}

			return !isValid;
		}
	}

	// private static final Logger LOG =
	// LogUtil.getLogger(SelectDestinationPage.class);

	private OnlineTargetDBView onlineTargetDBView;
	// private OfflineTargetDBView offlineTargetDBView;
	private UnloadTargetDBView unloadTargetDBView;

	private Composite container;

	// private Listener autoSelectAll = new Listener() {
	// public void handleEvent(Event event) {
	// if (event.item instanceof Text) {
	// ((Text) event.item).selectAll();
	// }
	// }
	// };

	/**
	 * Create the wizard
	 */
	public GraphSelectDestinationPage(String pageName) {
		super(pageName);
	}

	/**
	 * When migration wizard displayed current page.
	 * 
	 * @param event PageChangedEvent
	 */
	protected void afterShowCurrentPage(PageChangedEvent event) {
		if (isFirstVisible) {
			isFirstVisible = false;
		}
		final AbstractDestinationView crtDBView = getCrtDBView();
		crtDBView.createControls(container);
		unloadTargetDBView.hide();
		// offlineTargetDBView.hide();
		onlineTargetDBView.hide();
		crtDBView.init();
		crtDBView.show();
		container.layout();
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
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		setControl(container);

		onlineTargetDBView = new OnlineTargetDBView();
		// offlineTargetDBView = new OfflineTargetDBView();
		unloadTargetDBView = new UnloadTargetDBView();
	}

	/**
	 * Retrieves current target DB view
	 * 
	 * @return TargetDBView
	 */
	private AbstractDestinationView getCrtDBView() {
		MigrationWizard wizard = getMigrationWizard();
		MigrationConfiguration config = wizard.getMigrationConfig();
		if (config.targetIsOnline()) {
			return onlineTargetDBView;
		} else if (config.targetIsFile()) {
			return unloadTargetDBView;
		}
		// else if (config.targetIsOffline()) {
		// return offlineTargetDBView;
		// }
		throw new RuntimeException("Error destination configuration.");
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
	 * Update migration configuration.
	 * 
	 * @return true if all updated
	 */
	protected boolean updateMigrationConfig() {
		return getCrtDBView().save();
	}
}
