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
package com.cubrid.cubridmigration.ui.history.dialog;

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * Performance settings dialog
 * 
 * @author cn13425
 * @version 1.0 - 2013-01-22 created by cn13425
 */
public class OpenWizardWithHistoryDialog extends
		Dialog {

	private Button btnAll;
	private Button btnAllError;
	private Button btnErrorData;

	private int handlingMode = 0;
	private Button btnErrorFiles;
	private final boolean targetIsOnline;

	public OpenWizardWithHistoryDialog(Shell parentShell, boolean targetIsOnline) {
		super(parentShell);
		this.targetIsOnline = targetIsOnline;
	}

	public boolean isHelpAvailable() {
		return false;
	}

	/**
	 * createDialogArea
	 * 
	 * @param parent Composite
	 * @return Control
	 */

	protected Control createDialogArea(Composite parent) {
		final Composite composite = new Composite(parent, SWT.BORDER);
		composite.setLayout(new GridLayout());
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));

		Label txtTitleMsg = new Label(composite, SWT.WRAP);
		txtTitleMsg.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		txtTitleMsg.setText(Messages.msgErrorMigrationHint);

		final Composite comButtons = new Composite(composite, SWT.BORDER);
		comButtons.setLayout(new GridLayout());
		comButtons.setLayoutData(new GridData(GridData.FILL_BOTH));

		btnAll = new Button(comButtons, SWT.RADIO);
		btnAll.setText(Messages.btnAllSchemaAndData);
		btnAllError = new Button(comButtons, SWT.RADIO);
		btnAllError.setText(Messages.btnErrorSchemaAndData);
		btnErrorData = new Button(comButtons, SWT.RADIO);
		btnErrorData.setText(Messages.btnErrorData);
		btnErrorData.setSelection(true);

		//If target is not online database, this option will not be displayed.
		btnErrorFiles = new Button(comButtons, SWT.RADIO);
		btnErrorFiles.setText(Messages.btnErrorFiles);
		btnErrorFiles.setSelection(false);
		btnErrorFiles.setVisible(targetIsOnline);
		return parent;
	}

	/**
	 * constrainShellSize
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setSize(700, 250);
		getShell().setText(Messages.msgErrorMigrationTitle);
	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		Button btnOK = createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, true);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
		btnOK.setEnabled(true);
	}

	/**
	 * If OK button pressed
	 */
	protected void okPressed() {
		if (btnAll.getSelection()) {
			handlingMode = 0;
		} else if (btnAllError.getSelection()) {
			handlingMode = 1;
		} else if (btnErrorData.getSelection()) {
			handlingMode = 2;
		} else if (btnErrorFiles.getSelection()) {
			handlingMode = 3;
		}
		super.okPressed();
	}

	public int getHandlingMode() {
		return handlingMode;
	}
}
