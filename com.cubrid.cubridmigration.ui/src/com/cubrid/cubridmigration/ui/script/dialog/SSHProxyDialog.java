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
import org.apache.log4j.Logger;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.common.CipherUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.SSHHostBaseInfo;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * Export migration script dialog, supports exporting to local file system and
 * remote file system by SSH.
 * 
 * @author Kevin Cao
 * 
 */
public class SSHProxyDialog extends
		Dialog {

	static final String PROXY_SSH_PUBLICKEY = "com.cubrid.migration.remote.ssh.proxy.publickey";
	static final String PROXY_SSH_PWD = "com.cubrid.migration.remote.ssh.proxy.password";
	static final String PROXY_SSH_USER = "com.cubrid.migration.remote.ssh.proxy.user";
	static final String PROXY_SSH_AUTHTYPE = "com.cubrid.migration.remote.ssh.proxy.authtype";
	static final String PROXY_SSH_PORT = "com.cubrid.migration.remote.ssh.proxy.port";
	static final String PROXY_SSH_HOST = "com.cubrid.migration.remote.ssh.proxy.host";
	private static final String PROXY_SSH_KRB_CFG = "com.cubrid.migration.remote.ssh.proxy.krbconfig";
	private static final String PROXY_SSH_KRB_TGT = "com.cubrid.migration.remote.ssh.proxy.krbticket";

	private static final Logger LOG = LogUtil.getLogger(SSHProxyDialog.class);

	/**
	 * Retrieves the proxy setting saved in local
	 * 
	 * @return SSHHostBaseInfo
	 */
	public static SSHHostBaseInfo getProxy() {
		SSHHostBaseInfo host = new SSHHostBaseInfo();
		IEclipsePreferences epf = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
		host.setPrivateKeyAbsoluteFile(epf.get(PROXY_SSH_PUBLICKEY, ""));
		host.setPassword(CipherUtils.decrypt(epf.get(PROXY_SSH_PWD, "")));
		host.setUser(epf.get(PROXY_SSH_USER, ""));
		host.setAuthType(epf.getInt(PROXY_SSH_AUTHTYPE, 0));
		host.setPort(epf.getInt(PROXY_SSH_PORT, 22));
		host.setHost(epf.get(PROXY_SSH_HOST, ""));
		host.setKrbConfig(epf.get(PROXY_SSH_KRB_CFG, PathUtils.getDefaultKrbConfigFile()));
		host.setKrbTicket(epf.get(PROXY_SSH_KRB_TGT, PathUtils.getDefaultTicketFile()));
		return host;
	}

	/**
	 * setSSHProxy
	 * 
	 */
	public static void setSSHProxy() {
		SSHProxyDialog dialog = new SSHProxyDialog(Display.getDefault().getActiveShell());
		dialog.open();
	}

	private Text txtHost;
	private Spinner txtPort;
	private Text txtUser;
	private Text txtPwd;
	private Combo cboAuthType;
	private Button btnAuSettings;
	private Text txtPublicKey;
	private Button btnBrowseKey;

	private SSHHostBaseInfo host = SSHProxyDialog.getProxy();
	private IEclipsePreferences epf = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);

	protected SSHProxyDialog(Shell parentShell) {
		super(parentShell);
		setShellStyle(getShellStyle() | SWT.RESIZE);
	}

	/**
	 * Initialize shell
	 * 
	 * @param newShell to be set
	 */
	protected void configureShell(Shell newShell) {
		newShell.setMinimumSize(300, 330);
		newShell.setSize(500, 330);
		CompositeUtils.centerDialog(newShell);
		newShell.setText("SSH Proxy Settings");
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
	 * Dialog area
	 * 
	 * @param parent of he dialog area
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite result = (Composite) super.createDialogArea(parent);
		result.setLayout(new GridLayout());
		result.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		//----------------------------------------------------------

		Group grpRemote = new Group(result, SWT.NONE);
		grpRemote.setLayout(new GridLayout(3, false));
		grpRemote.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Label lblHost = new Label(grpRemote, SWT.NONE);
		lblHost.setText(Messages.lblHost);
		lblHost.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtHost = new Text(grpRemote, SWT.BORDER);
		txtHost.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
		txtHost.setText(epf.get(PROXY_SSH_HOST, ""));

		Label lblPort = new Label(grpRemote, SWT.NONE);
		lblPort.setText(Messages.lblPort);
		lblPort.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtPort = new Spinner(grpRemote, SWT.BORDER);
		txtPort.setValues(epf.getInt(PROXY_SSH_PORT, 22), 0, 65535, 0, 1, 10);
		txtPort.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));

		Label lblUser = new Label(grpRemote, SWT.NONE);
		lblUser.setText(Messages.lblUser);
		lblUser.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtUser = new Text(grpRemote, SWT.BORDER);
		txtUser.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
		txtUser.setText(epf.get(PROXY_SSH_USER, ""));

		Label lblPwd = new Label(grpRemote, SWT.NONE);
		lblPwd.setText(Messages.lblPassword);
		lblPwd.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		txtPwd = new Text(grpRemote, SWT.BORDER | SWT.PASSWORD);
		txtPwd.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
		txtPwd.setText(CipherUtils.decrypt(epf.get(PROXY_SSH_PWD, "")));

		Label lblAuthType = new Label(grpRemote, SWT.NONE);
		lblAuthType.setText(Messages.lblAuthentication);
		lblAuthType.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		cboAuthType = new Combo(grpRemote, SWT.BORDER | SWT.READ_ONLY);
		cboAuthType.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
		cboAuthType.setItems(new String[] {"Password", "Public key", "Gss(Krb5)"});
		cboAuthType.select(epf.getInt(PROXY_SSH_AUTHTYPE, 0));
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
		txtPublicKey.setText(epf.get(PROXY_SSH_PUBLICKEY, ""));

		btnBrowseKey = new Button(grpRemote, SWT.NONE);
		btnBrowseKey.setText(Messages.btnBrowse);
		btnBrowseKey.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		btnBrowseKey.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				FileDialog dialog = new FileDialog(getShell(), SWT.OPEN | SWT.SINGLE);
				dialog.setFilterPath(System.getProperty("user.home"));
				dialog.setFilterNames(new String[] {"*.*"});
				dialog.setFilterExtensions(new String[] {"*.*"});
				String filePath = dialog.open();
				if (filePath == null) {
					return;
				}
				txtPublicKey.setText(filePath);
			}

		});

		//----------------------------------------------------------
		updateControlsStatus();
		return result;
	}

	/**
	 * isPublicKey
	 * 
	 * @return true if isPublicKey auth
	 */
	private boolean isPublicKey() {
		return cboAuthType.getSelectionIndex() == 1;
	}

	/**
	 * OK pressed
	 */
	protected void okPressed() {
		try {
			String shost = txtHost.getText().trim();
			if (StringUtils.isBlank(shost)) {
				MessageDialog.openError(getShell(), Messages.msgError, Messages.errMsgEmptyHost);
				txtHost.setFocus();
				return;
			}
			int port = txtPort.getSelection();

			String user = txtUser.getText().trim();
			if (StringUtils.isBlank(user)) {
				MessageDialog.openError(getShell(), Messages.msgError, Messages.errMsgEmptyUser);
				txtUser.setFocus();
				return;
			}
			String pwd = txtPwd.getText();
			if (cboAuthType.getSelectionIndex() == 1) {
				//Authorize with public key and passphase
				String pubKey = txtPublicKey.getText().trim();
				if (StringUtils.isBlank(pubKey)) {
					MessageDialog.openError(getShell(), Messages.msgError,
							Messages.errMsgEmptyPublicKey);
					txtPublicKey.setFocus();
					return;
				}
			}
			epf.put(PROXY_SSH_HOST, shost);
			epf.put(PROXY_SSH_PORT, String.valueOf(port));
			epf.put(PROXY_SSH_AUTHTYPE, String.valueOf(cboAuthType.getSelectionIndex()));
			epf.put(PROXY_SSH_USER, user);
			epf.put(PROXY_SSH_PUBLICKEY, txtPublicKey.getText().trim());
			epf.put(PROXY_SSH_PWD, CipherUtils.encrypt(pwd));
			epf.put(PROXY_SSH_KRB_CFG, host.getKrbConfig());
			epf.put(PROXY_SSH_KRB_TGT, host.getKrbTicket());
			epf.flush();

		} catch (Exception e) {
			LOG.error(e);
			DetailMessageDialog.openError(getShell(), Messages.msgError,
					Messages.errMsgProxySettingError, e.getMessage());
			return;
		}
		super.okPressed();
	}

	/**
	 * Update controls status.
	 */
	private void updateControlsStatus() {
		boolean selection = true;
		txtHost.setEnabled(selection);
		txtPort.setEnabled(selection);
		cboAuthType.setEnabled(selection);
		txtUser.setEnabled(selection);
		txtPublicKey.setEnabled(isPublicKey());
		btnBrowseKey.setEnabled(isPublicKey());
		btnAuSettings.setEnabled(selection && (cboAuthType.getSelectionIndex() == 2));
		txtPwd.setEnabled(selection);
	}
}
