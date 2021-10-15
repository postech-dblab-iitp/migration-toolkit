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

import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;
import org.osgi.service.prefs.BackingStoreException;

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.SSHHost;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * Transmission file from/to remote server by SSH dialog
 * 
 * @author Kevin Cao
 * 
 */
public class TransFileBySSHDialog extends
		Dialog {

	protected static final String SSH_FILE = "com.cubrid.migration.script.file";
	protected static final String SSH_PUBLICKEY = "com.cubrid.migration.remote.ssh.publickey";
	protected static final String SSH_USER = "com.cubrid.migration.remote.ssh.user";
	protected static final String SSH_AUTHTYPE = "com.cubrid.migration.remote.ssh.authtype";
	protected static final String SSH_PORT = "com.cubrid.migration.remote.ssh.port";
	protected static final String SSH_HOST = "com.cubrid.migration.remote.ssh.host";
	protected static final String SSH_USE_PROXY = "com.cubrid.migration.remote.ssh.proxy";
	protected static final String SSH_KRB_CFG = "com.cubrid.migration.remote.ssh.krbconfig";
	protected static final String SSH_KRB_TGT = "com.cubrid.migration.remote.ssh.krbticket";

	protected Text txtLocal;
	protected Button btnBrowseLocal;
	protected Text txtRemoteFile;
	protected Text txtHost;
	protected Spinner txtPort;
	protected Combo cboAuthType;
	protected Text txtUser;
	protected Text txtPublicKey;
	protected Text txtPwd;
	protected Button btnBrowseKey;
	protected Button btnEnableLocal;
	protected Button btnEnableRemote;
	protected Button btnSaveHost;
	protected Button btnUseProxy;
	protected Button btnProxySetting;
	protected Button btnAuSettings;

	protected SSHHost host = new SSHHost();
	protected IEclipsePreferences prefers = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);

	protected TransFileBySSHDialog(Shell parentShell) {
		super(parentShell);
		setShellStyle(getShellStyle() | SWT.RESIZE);
	}

	/**
	 * Initialize shell
	 * 
	 * @param newShell to be set
	 */
	protected void configureShell(Shell newShell) {
		newShell.setMinimumSize(300, 500);
		newShell.setSize(500, 500);
		CompositeUtils.centerDialog(newShell);
		super.configureShell(newShell);
	}

	/**
	 * Create buttons
	 * 
	 * @param parent of the buttons
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, true);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
	}

	/**
	 * Should be overrided by subclasses
	 * 
	 * @return int
	 */
	protected int getTypeButtonStyle() {
		return SWT.CHECK;
	}

	/**
	 * Should be overrided by subclasses
	 */
	protected void initComposites() {

	}

	/**
	 * Dialog area
	 * 
	 * @param parent of he dialog area
	 * @return control
	 */
	protected Control createDialogArea(Composite parent) {
		updateCurrentHost(true);
		Composite result = (Composite) super.createDialogArea(parent);
		result.setLayout(new GridLayout());
		result.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		//----------------------------------------------------------
		btnEnableLocal = new Button(result, getTypeButtonStyle());
		btnEnableLocal.setSelection(true);
		btnEnableLocal.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				updateControlsStatus();
			}

		});

		Group grpLocal = new Group(result, SWT.NONE);
		grpLocal.setLayout(new GridLayout(3, false));
		grpLocal.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));

		Label lblLocal = new Label(grpLocal, SWT.NONE);
		lblLocal.setText(Messages.lblScriptFileName);
		lblLocal.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtLocal = new Text(grpLocal, SWT.BORDER);
		txtLocal.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));

		btnBrowseLocal = new Button(grpLocal, SWT.NONE);
		btnBrowseLocal.setText(Messages.btnBrowse);
		btnBrowseLocal.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));

		//----------------------------------------------------------
		btnEnableRemote = new Button(result, getTypeButtonStyle());
		btnEnableRemote.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				updateControlsStatus();
			}

		});

		Group grpRemote = new Group(result, SWT.NONE);
		grpRemote.setLayout(new GridLayout(3, false));
		grpRemote.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Label lblRemoteFile = new Label(grpRemote, SWT.NONE);
		lblRemoteFile.setText(Messages.lblScriptFileName);
		lblRemoteFile.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtRemoteFile = new Text(grpRemote, SWT.BORDER);
		txtRemoteFile.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));

		Label lblHost = new Label(grpRemote, SWT.NONE);
		lblHost.setText(Messages.lblHost);
		lblHost.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtHost = new Text(grpRemote, SWT.BORDER);
		txtHost.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
		txtHost.setText(prefers.get(SSH_HOST, ""));

		Label lblPort = new Label(grpRemote, SWT.NONE);
		lblPort.setText(Messages.lblPort);
		lblPort.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtPort = new Spinner(grpRemote, SWT.BORDER);
		txtPort.setValues(prefers.getInt(SSH_PORT, 22), 0, 65535, 0, 1, 10);
		txtPort.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));

		Label lblUser = new Label(grpRemote, SWT.NONE);
		lblUser.setText(Messages.lblUser);
		lblUser.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtUser = new Text(grpRemote, SWT.BORDER);
		txtUser.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
		txtUser.setText(prefers.get(SSH_USER, ""));

		Label lblPwd = new Label(grpRemote, SWT.NONE);
		lblPwd.setText(Messages.lblPassword);
		lblPwd.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtPwd = new Text(grpRemote, SWT.BORDER | SWT.PASSWORD);
		txtPwd.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));

		btnUseProxy = new Button(grpRemote, SWT.CHECK);
		btnUseProxy.setText(Messages.btnEnableSSHProxy);
		btnUseProxy.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				updateControlsStatus();
			}

		});
		btnUseProxy.setSelection(prefers.getBoolean(SSH_USE_PROXY, false));

		btnProxySetting = new Button(grpRemote, SWT.NONE);
		btnProxySetting.setText(Messages.btnSSHProxySettings);
		btnProxySetting.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				SSHProxyDialog.setSSHProxy();
			}

		});

		new Label(grpRemote, SWT.NONE);

		Label lblAuthType = new Label(grpRemote, SWT.NONE);
		lblAuthType.setText(Messages.lblAuthentication);
		lblAuthType.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		cboAuthType = new Combo(grpRemote, SWT.BORDER | SWT.READ_ONLY);
		cboAuthType.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
		cboAuthType.setItems(new String[] { "Password", "Public key", "Gss(Krb5)" });
		cboAuthType.select(prefers.getInt(SSH_AUTHTYPE, 0));
		cboAuthType.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent ev) {
				updateControlsStatus();
			}
		});

		btnAuSettings = new Button(grpRemote, SWT.NONE);
		btnAuSettings.setText(Messages.btnSettings);
		btnAuSettings.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent ev) {
				KerberosSettingsDialog.configKRB(getShell(), host);
			}
		});

		Label lblPublicKey = new Label(grpRemote, SWT.NONE);
		lblPublicKey.setText(Messages.lblPublicKeyFile);
		lblPublicKey.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtPublicKey = new Text(grpRemote, SWT.BORDER);
		txtPublicKey.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtPublicKey.setText(prefers.get(SSH_PUBLICKEY, ""));

		btnBrowseKey = new Button(grpRemote, SWT.NONE);
		btnBrowseKey.setText(Messages.btnBrowse);
		btnBrowseKey.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		btnBrowseKey.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				FileDialog dialog = new FileDialog(getShell(), SWT.OPEN | SWT.SINGLE);
				dialog.setFilterPath(System.getProperty("user.home"));
				dialog.setFilterNames(new String[] { "*.*" });
				dialog.setFilterExtensions(new String[] { "*.*" });
				String filePath = dialog.open();
				if (filePath == null) {
					return;
				}
				txtPublicKey.setText(filePath);
			}

		});

		new Label(grpRemote, SWT.NONE);
		btnSaveHost = new Button(grpRemote, SWT.CHECK);
		btnSaveHost.setText(Messages.btnSaveRemoteServer);
		btnSaveHost.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false, 2, 1));
		btnSaveHost.setSelection(true);
		//----------------------------------------------------------
		initComposites();
		updateControlsStatus();
		return result;
	}

	/**
	 * isPublicKey
	 * 
	 * @return true if isPublicKey auth
	 */
	protected boolean isPublicKey() {
		return btnEnableRemote.getSelection() && !btnUseProxy.getSelection()
				&& cboAuthType.getSelectionIndex() == 1;
	}

	/**
	 * Update controls status.
	 */
	protected void updateControlsStatus() {
		boolean selection = btnEnableLocal.getSelection();
		txtLocal.setEnabled(selection);
		btnBrowseLocal.setEnabled(selection);
		selection = btnEnableRemote.getSelection();
		txtRemoteFile.setEnabled(selection);
		txtHost.setEnabled(selection);
		txtPort.setEnabled(selection);
		txtUser.setEnabled(selection);
		txtPwd.setEnabled(selection);
		btnUseProxy.setEnabled(selection);
		btnProxySetting.setEnabled(selection && btnUseProxy.getSelection());

		cboAuthType.setEnabled(selection && !btnUseProxy.getSelection());
		txtPublicKey.setEnabled(isPublicKey());
		btnBrowseKey.setEnabled(isPublicKey());
		btnAuSettings.setEnabled(selection && !btnUseProxy.getSelection()
				&& cboAuthType.getSelectionIndex() == 2);
		btnSaveHost.setEnabled(selection);
	}

	/**
	 * Current host information
	 * 
	 * @param isInit boolean
	 */
	protected void updateCurrentHost(boolean isInit) {
		if (isInit) {
			host.setHost(prefers.get(TransFileBySSHDialog.SSH_HOST, ""));
			host.setPort(prefers.getInt(TransFileBySSHDialog.SSH_PORT, 22));
			host.setAuthType(prefers.getInt(TransFileBySSHDialog.SSH_AUTHTYPE, 0));
			host.setUser(prefers.get(TransFileBySSHDialog.SSH_USER, ""));
			host.setPrivateKeyAbsoluteFile(prefers.get(TransFileBySSHDialog.SSH_PUBLICKEY, ""));
			host.setPassword((txtPwd == null || txtPwd.isDisposed()) ? "" : txtPwd.getText());
			host.setUseProxy((btnUseProxy == null || btnUseProxy.isDisposed()) ? false
					: btnUseProxy.getSelection());
			host.setKrbConfig(prefers.get(TransFileBySSHDialog.SSH_KRB_CFG,
					PathUtils.getDefaultKrbConfigFile()));
			host.setKrbTicket(prefers.get(TransFileBySSHDialog.SSH_KRB_TGT,
					PathUtils.getDefaultTicketFile()));
			host.setProxy(SSHProxyDialog.getProxy());
		} else {
			host.setHost(txtHost.getText().trim());
			host.setPort(txtPort.getSelection());
			host.setAuthType(cboAuthType.getSelectionIndex());
			host.setUser(txtUser.getText().trim());
			host.setPrivateKeyAbsoluteFile(txtPublicKey.getText().trim());
			host.setPassword(txtPwd.getText());
			host.setUseProxy(btnUseProxy.getSelection());
			host.setProxy(SSHProxyDialog.getProxy());
		}
	}

	/**
	 * Check input's validation
	 * 
	 * @return true if pass the checking
	 */
	protected boolean checkInput() {
		if (btnEnableLocal.getSelection()) {
			String fName = txtLocal.getText().trim();
			if (StringUtils.isBlank(fName)) {
				MessageDialog.openError(getShell(), Messages.msgError, Messages.errMsgEmptyFileName);
				txtLocal.setFocus();
				return false;
			}
		}
		if (btnEnableRemote.getSelection()) {
			String fName = txtRemoteFile.getText().trim();
			if (StringUtils.isBlank(fName)) {
				MessageDialog.openError(getShell(), Messages.msgError, Messages.errMsgEmptyFileName);
				txtRemoteFile.setFocus();
				return false;
			}
			String shost = txtHost.getText().trim();
			if (StringUtils.isBlank(shost)) {
				MessageDialog.openError(getShell(), Messages.msgError, Messages.errMsgEmptyHost);
				txtHost.setFocus();
				return false;
			}
			String user = txtUser.getText().trim();
			if (StringUtils.isBlank(user)) {
				MessageDialog.openError(getShell(), Messages.msgError, Messages.errMsgEmptyUser);
				txtUser.setFocus();
				return false;
			}
			if (cboAuthType.getSelectionIndex() == 1) {
				//Authorize with public key and passphase
				String pubKey = txtPublicKey.getText().trim();
				if (StringUtils.isBlank(pubKey)) {
					MessageDialog.openError(getShell(), Messages.msgError,
							Messages.errMsgEmptyPublicKey);
					txtPublicKey.setFocus();
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Save host into preference as default.
	 * 
	 * @throws BackingStoreException ex
	 */
	protected void saveHostAsDefault() throws BackingStoreException {
		if (btnSaveHost.getSelection()) {
			prefers.put(SSH_HOST, txtHost.getText().trim());
			prefers.putInt(SSH_PORT, txtPort.getSelection());
			prefers.putInt(SSH_AUTHTYPE, cboAuthType.getSelectionIndex());
			prefers.put(SSH_USER, txtUser.getText().trim());
			prefers.put(SSH_PUBLICKEY, txtPublicKey.getText().trim());
			prefers.put(SSH_FILE, txtRemoteFile.getText().trim());
			prefers.putBoolean(SSH_USE_PROXY, btnUseProxy.getSelection());
			prefers.put(SSH_KRB_CFG, host.getKrbConfig());
			prefers.put(SSH_KRB_TGT, host.getKrbTicket());
			prefers.flush();
		}
	}
}