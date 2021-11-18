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
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.common.SSHHostBaseInfo;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * Configure the host's KEB authentication
 * 
 * @author Kevin Cao
 * 
 */
public class KerberosSettingsDialog extends
		Dialog {

	private static final Logger LOG = LogUtil.getLogger(KerberosSettingsDialog.class);

	private Text txtKrbConfFile;
	private Text txtTicketFile;
	private SSHHostBaseInfo host;

	/**
	 * configKRB
	 * 
	 * @param parentShell Shell
	 * @param host SSHHostBaseInfo
	 */
	public static void configKRB(Shell parentShell, SSHHostBaseInfo host) {
		KerberosSettingsDialog dialog = new KerberosSettingsDialog(parentShell, host);
		dialog.open();
	}

	protected KerberosSettingsDialog(Shell parentShell, SSHHostBaseInfo host) {
		super(parentShell);
		this.host = host;
	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, true);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
	}

	/**
	 * createDialogArea
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite result = (Composite) super.createDialogArea(parent);
		result.setLayout(new GridLayout(3, false));
		result.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Label lblKrbConfFile = new Label(result, SWT.NONE);
		lblKrbConfFile.setText(Messages.lblKrbConfFile);

		txtKrbConfFile = new Text(result, SWT.BORDER);
		txtKrbConfFile.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtKrbConfFile.setText(host.getKrbConfig());
		//epf.get(KRB5_CONF, PathUtils.getDefaultKrbConfigFile())

		Button btnKrbConfFile = new Button(result, SWT.NONE);
		btnKrbConfFile.setText(Messages.btnBrowse);
		btnKrbConfFile.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				FileDialog dialog = new FileDialog(getShell(), SWT.OPEN | SWT.SINGLE);
				dialog.setFilterPath(System.getProperty("user.home"));
				dialog.setFilterNames(new String[] {"*.*"});
				dialog.setFilterExtensions(new String[] {"*.*"});
				String filePath = dialog.open();
				if (filePath == null) {
					return;
				}
				txtKrbConfFile.setText(filePath);
			}

		});

		Label lblTicketFile = new Label(result, SWT.NONE);
		lblTicketFile.setText(Messages.lblTichetCacheFile);

		txtTicketFile = new Text(result, SWT.BORDER);
		txtTicketFile.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtTicketFile.setText(host.getKrbTicket());
		//epf.get(TICKET_FILE, PathUtils.getDefaultTicketFile())

		Button btnTicketFile = new Button(result, SWT.NONE);
		btnTicketFile.setText(Messages.btnBrowse);
		btnTicketFile.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				FileDialog dialog = new FileDialog(getShell(), SWT.OPEN | SWT.SINGLE);
				dialog.setFilterPath(System.getProperty("user.home"));
				dialog.setFilterNames(new String[] {"*.*"});
				dialog.setFilterExtensions(new String[] {"*.*"});
				String filePath = dialog.open();
				if (filePath == null) {
					return;
				}
				txtTicketFile.setText(filePath);
			}

		});
		return result;
	}

	/**
	 * okPressed
	 */
	protected void okPressed() {
		if (StringUtils.isBlank(txtKrbConfFile.getText())) {
			return;
		}

		if (StringUtils.isBlank(txtTicketFile.getText())) {
			return;
		}

		try {
			host.setKrbConfig(txtKrbConfFile.getText());
			host.setKrbTicket(txtTicketFile.getText());
			super.okPressed();
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	/**
	 * @param newShell Shell
	 */
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setSize(500, 160);
		CompositeUtils.centerDialog(newShell);
		newShell.setText("Kerberos Environment Settings");
	}

}
