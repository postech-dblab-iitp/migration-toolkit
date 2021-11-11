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
package com.cubrid.cubridmigration.ui.script.dialog;

import java.io.File;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.osgi.service.prefs.BackingStoreException;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.SSHConnectFailedException;
import com.cubrid.cubridmigration.core.common.SSHUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.MigrationTemplateParser;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.MigrationScript;
import com.jcraft.jsch.Session;

/**
 * Export migration script dialog, supports exporting to local file system and
 * remote file system by SSH.
 * 
 * @author Kevin Cao
 * 
 */
public class ExportScriptDialog extends
		TransFileBySSHDialog {
	protected static final String EXPORT_REMOTE = "com.cubrid.migration.script.export.remote";
	protected static final String EXPORT_LOCAL = "com.cubrid.migration.script.export.local";
	private static final Logger LOG = LogUtil.getLogger(ExportScriptDialog.class);

	/**
	 * exportScript
	 * 
	 * @param config to be exported
	 * @param isSaveSchema save schema option
	 */
	public static void exportScript(MigrationConfiguration config, boolean isSaveSchema) {
		ExportScriptDialog dialog = new ExportScriptDialog(Display.getDefault().getActiveShell(),
				config);
		dialog.isSaveSchema = isSaveSchema;
		dialog.open();
	}

	/**
	 * exportScript
	 * 
	 * @param script to be exported
	 */
	public static void exportScript(MigrationScript script) {
		if (script == null) {
			throw new IllegalArgumentException("Script can't be null.");
		}
		MigrationConfiguration config = MigrationTemplateParser.parse(script.getAbstractConfigFileName());
		//synchronized configuration name with script name
		config.setName(script.getName());
		exportScript(config, config.getOfflineSrcCatalog() != null);
	}

	private final MigrationConfiguration config;

	private String tmpFile;

	private boolean isSaveSchema;

	protected ExportScriptDialog(Shell parentShell, MigrationConfiguration config) {
		super(parentShell);
		setShellStyle(getShellStyle() | SWT.RESIZE);
		this.config = config;
	}

	/**
	 * Initialize shell
	 * 
	 * @param newShell to be set
	 */
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setText(Messages.titleExportScript);
	}

	/**
	 * @return type button style
	 */
	protected int getTypeButtonStyle() {
		return SWT.CHECK;
	}

	/**
	 * Init composites
	 */
	protected void initComposites() {
		btnEnableLocal.setText(Messages.btnExportScript2Local);
		btnEnableLocal.setSelection(prefers.getBoolean(EXPORT_LOCAL, true));
		String rf = getDefaultScriptFileName();
		txtLocal.setText(PathUtils.getUserHomeDir() + rf);
		btnBrowseLocal.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				FileDialog dialog = new FileDialog(getShell(), SWT.SAVE);
				dialog.setFilterPath(".");
				dialog.setFilterNames(new String[] {"*.xml"});
				dialog.setFilterExtensions(new String[] {"*.xml"});
				dialog.setOverwrite(true);
				String defaultScriptFileName = PathUtils.transStr2FileName(config.getName());
				defaultScriptFileName = StringUtils.lowerCase(defaultScriptFileName).endsWith(
						".xml") ? defaultScriptFileName : (defaultScriptFileName + ".xml");
				dialog.setFileName(defaultScriptFileName);
				String filePath = dialog.open();
				if (filePath == null) {
					return;
				}
				txtLocal.setText(filePath);
			}

		});
		btnEnableRemote.setText(Messages.btnExportScript2Remote);
		btnEnableRemote.setSelection(prefers.getBoolean(EXPORT_REMOTE, false));
		txtRemoteFile.setText(prefers.get(SSH_FILE, rf));
	}

	/**
	 * Retrieves the default migration script name
	 * 
	 * @return default script name
	 */
	private String getDefaultScriptFileName() {
		String rf = config.getName() == null ? "migrationscript" : config.getName().replaceAll(
				"[\\s|:|-]+", "_");
		rf = rf.toLowerCase(Locale.US).endsWith(".xml") ? rf : (rf + ".xml");
		return rf;
	}

	/**
	 * OK pressed
	 */
	protected void okPressed() {
		if (!(btnEnableLocal.getSelection() || btnEnableRemote.getSelection())) {
			MessageDialog.openError(getShell(), Messages.msgError, Messages.errMsgNoExportScript);
			return;
		}
		if (tmpFile == null) {
			tmpFile = PathUtils.getBaseTempDir() + UUID.randomUUID() + ".xml";
			MigrationTemplateParser.save(config, tmpFile, isSaveSchema);
		}
		if (!checkInput()) {
			return;
		}
		if (btnEnableLocal.getSelection()) {
			try {
				String fName = txtLocal.getText().trim();
				File dest = new File(fName);
				PathUtils.deleteFile(dest);
				CUBRIDIOUtils.copyFile(new File(tmpFile), dest);
			} catch (Exception ex) {
				LOG.error(ex);
				DetailMessageDialog.openError(getShell(), Messages.msgError,
						Messages.errMsgExportLocalFailed, ex.getMessage());
				return;
			}
		}
		if (btnEnableRemote.getSelection()) {
			try {
				saveHostAsDefault();
				updateCurrentHost(false);
				Session session = SSHUtils.newSSHSession(host);
				try {
					String fName = txtRemoteFile.getText().trim();
					String scpResult = SSHUtils.scpTo(session, tmpFile, fName);
					if (StringUtils.isNotBlank(scpResult)) {
						DetailMessageDialog.openError(getShell(), Messages.msgError,
								Messages.errMsgExportRemoteFailed, scpResult);
						return;
					}
				} finally {
					if (session.isConnected()) {
						session.disconnect();
					}
				}
			} catch (SSHConnectFailedException ex) {
				DetailMessageDialog.openError(getShell(), Messages.msgError,
						Messages.errMsgCannotConnectRemote, ex.getMessage());
				return;
			} catch (Exception ex) {
				LOG.error(ex);
				DetailMessageDialog.openError(getShell(), Messages.msgError,
						Messages.errMsgExportRemoteFailed, ex.getMessage());
				return;
			}
		}
		try {
			prefers.putBoolean(EXPORT_LOCAL, btnEnableLocal.getSelection());
			prefers.putBoolean(EXPORT_REMOTE, btnEnableRemote.getSelection());
			prefers.flush();
		} catch (BackingStoreException ex) {
			LOG.error(ex);
		}
		MessageDialog.openInformation(getShell(), Messages.msgInformation,
				Messages.msgExportScriptSuccess);
		super.okPressed();
	}

}