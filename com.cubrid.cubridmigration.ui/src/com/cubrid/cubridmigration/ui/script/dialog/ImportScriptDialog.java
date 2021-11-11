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

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.SSHConnectFailedException;
import com.cubrid.cubridmigration.core.common.SSHUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.MigrationScriptManager;
import com.jcraft.jsch.Session;

/**
 * Import migration script dialog, supports exporting to local file system and
 * remote file system by SSH.
 * 
 * @author Kevin Cao
 * 
 */
public class ImportScriptDialog extends
		TransFileBySSHDialog {
	protected static final String IMPORT_REMOTE = "com.cubrid.migration.script.import.remote";
	protected static final String IMPORT_LOCAL = "com.cubrid.migration.script.import.local";

	private static final Logger LOG = LogUtil.getLogger(ImportScriptDialog.class);

	/**
	 * Import script
	 */
	public static void importScript() {
		ImportScriptDialog dialog = new ImportScriptDialog(Display.getDefault().getActiveShell());
		dialog.open();
	}

	protected ImportScriptDialog(Shell parentShell) {
		super(parentShell);
		setShellStyle(getShellStyle() | SWT.RESIZE);
	}

	/**
	 * Initialize shell
	 * 
	 * @param newShell to be set
	 */
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setText(Messages.titleImportScript);
	}

	/**
	 * getTypeButtonStyle
	 * 
	 * @return style of type button
	 */
	protected int getTypeButtonStyle() {
		return SWT.RADIO;
	}

	/**
	 * initComposites
	 */
	protected void initComposites() {
		btnEnableLocal.setText(Messages.btnImportFromLocal);
		btnEnableLocal.setSelection(prefers.getBoolean(IMPORT_LOCAL, true));
		btnBrowseLocal.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				FileDialog dialog = new FileDialog(getShell(), SWT.OPEN);
				dialog.setFilterPath(".");
				dialog.setFilterNames(new String[] {"*.xml"});
				dialog.setFilterExtensions(new String[] {"*.xml"});
				dialog.setOverwrite(true);
				String filePath = dialog.open();
				if (filePath == null) {
					return;
				}
				txtLocal.setText(filePath);
			}

		});
		btnEnableRemote.setText(Messages.btnImportFromRemote);
		btnEnableRemote.setSelection(prefers.getBoolean(IMPORT_REMOTE, false));
		txtRemoteFile.setText(prefers.get(SSH_FILE, ""));
	}

	/**
	 * OK pressed
	 */
	protected void okPressed() {
		if (!checkInput()) {
			return;
		}
		if (btnEnableLocal.getSelection()) {
			try {
				String fName = txtLocal.getText().trim();
				File tmpFileInstance = new File(fName);
				if (!tmpFileInstance.exists()) {
					DetailMessageDialog.openError(getShell(), Messages.msgError,
							Messages.errMsgImportLocalScriptFailed,
							Messages.importScriptFileNotFound);
					return;
				}
				if (!MigrationScriptManager.getInstance().importScript(fName)) {
					return;
				}
			} catch (Exception ex) {
				LOG.error(ex);
				DetailMessageDialog.openError(getShell(), Messages.msgError,
						Messages.errMsgImportLocalScriptFailed, ex.getMessage());
				return;
			}
		} else if (btnEnableRemote.getSelection()) {
			try {
				saveHostAsDefault();
				updateCurrentHost(false);
				Session session = SSHUtils.newSSHSession(host);
				try {
					String remoteFileName = txtRemoteFile.getText().trim();
					File remoteFile = new File(remoteFileName);
					String tmpScriptFileName = PathUtils.getBaseTempDir() + remoteFile.getName();
					String scpResult = SSHUtils.scpFrom(session, remoteFileName, tmpScriptFileName);
					if (StringUtils.isNotBlank(scpResult)) {
						DetailMessageDialog.openError(getShell(), Messages.msgError,
								Messages.errMsgImportLocalScriptFailed, scpResult);
						return;
					}
					File tmpFileInstance = new File(tmpScriptFileName);
					if (!MigrationScriptManager.getInstance().importScript(tmpScriptFileName)) {
						PathUtils.deleteFile(tmpFileInstance);
						return;
					}
					PathUtils.deleteFile(tmpFileInstance);
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
						Messages.errMsgImportRemoteScriptFailed, ex.getMessage());
				return;
			}
		}
		try {
			prefers.putBoolean(IMPORT_LOCAL, btnEnableLocal.getSelection());
			prefers.putBoolean(IMPORT_REMOTE, btnEnableRemote.getSelection());
			prefers.flush();
		} catch (BackingStoreException ex) {
			LOG.error(ex);
		}
		MessageDialog.openInformation(getShell(), Messages.msgInformation,
				Messages.infoImportScriptSuccess);
		super.okPressed();
	}

}
