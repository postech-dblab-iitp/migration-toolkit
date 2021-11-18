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

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.TitleAreaDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * NewDBConnectionDialog
 * 
 * @author caoyilin
 * @version 1.0 - 2013-1-20
 */
public class JDBCPatternDialog extends
		TitleAreaDialog {

	private final static int DEFAULT_ID = -1001;
	private final ConnParameters connParameters;
	private Text txtURL;

	public JDBCPatternDialog(Shell parentShell, ConnParameters cp) {
		super(parentShell);
		this.connParameters = cp;
	}

	/**
	 * configureShell
	 * 
	 * @param newShell Shell
	 */
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setText(Messages.jdbcPatternTitle);
		newShell.setSize(480, 240);
		Composite parent = newShell.getParent();
		if (parent == null) {
			parent = newShell;
		}
		newShell.setBounds(parent.getBounds().x, parent.getBounds().y, 480, 240);
	}

	/**
	 * Create contents of the button bar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, DEFAULT_ID, Messages.btnDefaultURL, false);
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, true);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
	}

	/**
	 * Create contents of the dialog
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite area = (Composite) super.createDialogArea(parent);
		//PlatformUI.getWorkbench().getHelpSystem().setHelp(area, CubridMigrationHelpContextIDs.ADD_DB_CONN);
		Composite panel = new Composite(area, SWT.NONE);
		panel.setLayout(new GridLayout(2, false));
		panel.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Label lblURL = new Label(panel, SWT.NONE);
		lblURL.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblURL.setText("JDBC URL:");

		txtURL = new Text(panel, SWT.BORDER);
		txtURL.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		if (StringUtils.isBlank(connParameters.getUserJDBCURL())) {
			txtURL.setText(connParameters.getDefaultURL());
		} else {
			txtURL.setText(connParameters.getUserJDBCURL());
		}

		setTitle(Messages.jdbcPatternTitle);
		setMessage(Messages.msgInputJDBCURL);
		return area;
	}

	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE | SWT.MAX;
	}

	public boolean isHelpAvailable() {
		return false;
	}

	/**
	 * Handle button pressed
	 * 
	 * @param buttonId button's id.
	 */
	protected void buttonPressed(int buttonId) {
		if (buttonId == DEFAULT_ID) {
			txtURL.setText(connParameters.getDefaultURL());
		} else if (buttonId == IDialogConstants.OK_ID) {
			if (StringUtils.isBlank(txtURL.getText())
					|| txtURL.getText().trim().equals(connParameters.getDefaultURL())) {
				connParameters.setUserJDBCURL(null);
			} else {
				connParameters.setUserJDBCURL(txtURL.getText().trim());
				//Don't check the URL any more.
				//				try {
				//					connParameters.setUserJDBCURL(txtURL.getText().trim());
				//
				//					Connection con = connParameters.createConnection();
				//					if (con == null) {
				//						MessageDialog.openError(getShell(), Messages.msgError,
				//								Messages.msgInvalidJDBCURL);
				//						return;
				//					}
				//					con.close();
				//				} catch (Exception ex) {
				//					MessageDialog.openError(getShell(), Messages.msgError,
				//							Messages.msgInvalidJDBCURL);
				//					return;
				//				}
			}
		}
		super.buttonPressed(buttonId);
	}
}
