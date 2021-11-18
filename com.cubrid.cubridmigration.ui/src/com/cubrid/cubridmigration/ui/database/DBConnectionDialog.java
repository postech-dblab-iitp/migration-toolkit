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

import java.sql.Connection;

import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.TitleAreaDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.ui.common.Status;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * Add or edit a JDBC connection dialog.
 * 
 * @author moulinwang JessieHuang fulei caoyilin
 * @version 1.0 - 2009-10-12
 */
public class DBConnectionDialog extends
		TitleAreaDialog {

	/**
	 * Get catalog from dialog
	 * 
	 * @param parentShell Shell
	 * @param databaseTypes DatabaseType[]
	 * @param oldParam ConnParameters
	 * @return catalog or null
	 */
	public static ConnParameters getCatalog(Shell parentShell, DatabaseType[] databaseTypes,
			ConnParameters oldParam) {
		if (databaseTypes == null || databaseTypes.length == 0) {
			throw new IllegalArgumentException("Database type can't be empty");
		}
		DBConnectionDialog dialog = new DBConnectionDialog(parentShell, databaseTypes, oldParam);
		if (dialog.open() != IDialogConstants.OK_ID) {
			return null;
		}
		return dialog.resultParam;
	}

	private final ConnParameters oldParam;
	private ConnParameters resultParam;
	private final JDBCConnectEditView dbConnectView;

	public DBConnectionDialog(Shell parentShell, DatabaseType[] databaseTypes,
			ConnParameters oldParam) {
		super(parentShell);
		this.oldParam = oldParam;
		dbConnectView = new JDBCConnectEditView(databaseTypes);
		setHelpAvailable(false);
	}

	/**
	 * configureShell
	 * 
	 * @param newShell Shell
	 */
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setText(Messages.newDBConnDialogWinTitle);
	}

	/**
	 * Create contents of the button bar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.DETAILS_ID, Messages.btnTestConnection, false);
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
		dbConnectView.createConstrols(parent);
		dbConnectView.setConParameters(oldParam);
		if (oldParam == null) {
			setTitle(Messages.newDBConnDialogTitle);
			setMessage(Messages.newDBConnDialogMessage);
		} else {
			setTitle(Messages.msgEditJDBC);
			setMessage(Messages.msgEditJDBCDesc);
		}
		return area;
	}

	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE | SWT.MAX;
	}

	/**
	 * If ok pressed, create catalog.
	 */
	protected void okPressed() {
		if (!checkInput()) {
			return;
		}

		final ConnParameters newCP = dbConnectView.getConnParameters();
		String userJDBCURL = newCP.getUserJDBCURL();
		if (StringUtils.isNotBlank(userJDBCURL)) {
			if (!MessageDialog.openConfirm(getShell(), Messages.titleConfirm,
					Messages.bind(Messages.msgSetJDBCURL, userJDBCURL))) {
				return;
			}
		}
		resultParam = newCP;
		super.okPressed();

	}

	/**
	 * Check the input
	 * 
	 * @return true if can be OK
	 */
	private boolean checkInput() {
		IStatus status = dbConnectView.checkStatus();
		if (status == null) {
			status = new Status(IStatus.INFO, Messages.newDBConnDialogMessage);
		}
		if (status.getSeverity() == IStatus.INFO) {
			setErrorMessage(null);
			setMessage(status.getMessage());
		} else {
			setErrorMessage(status.getMessage());
		}
		return (status.getSeverity() == IStatus.INFO);
	}

	/**
	 * 
	 * If IDialogConstants.DETAILS_ID button pressed, save information and do
	 * not connect server.
	 * 
	 * @param buttonId button id pressed
	 * 
	 */
	protected void buttonPressed(int buttonId) {
		//Test the connection
		if (buttonId == IDialogConstants.DETAILS_ID) {
			final ConnParameters newCP = dbConnectView.getConnParameters();

			try {
				Connection conn = newCP.createConnection();
				conn.close();
				MessageDialog.openInformation(getShell(), Messages.msgInformation,
						Messages.msgConnectSuccess);
			} catch (Exception ex) {
				DetailMessageDialog.openError(getShell(), Messages.msgInformation,
						Messages.msgConnectFailed, ex.getMessage());
			}
		}
		super.buttonPressed(buttonId);
	}
}
