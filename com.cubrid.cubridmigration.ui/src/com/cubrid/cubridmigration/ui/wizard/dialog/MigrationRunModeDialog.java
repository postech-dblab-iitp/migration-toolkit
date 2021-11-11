/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
 * Solution.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */
package com.cubrid.cubridmigration.ui.wizard.dialog;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.MigrationScript;
import com.cubrid.cubridmigration.ui.script.MigrationScriptManager;

/**
 * 
 * Users select migration's running mode: run now/reservation/cancel
 * 
 * @author caoyilin
 * @version 1.0 - 2013-3-1 created by caoyilin
 */
public class MigrationRunModeDialog extends
		Dialog {

	private Text txtName;
	private String migrationName;
	private MigrationScript script = null;

	public MigrationRunModeDialog(Shell parentShell) {
		super(parentShell);
	}

	/**
	 * constrainShellSize
	 * 
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setSize(480, 258);
		UICommonTool.centerShell(getShell());
	}

	/**
	 * Press button event.
	 * 
	 * @param buttonId the ID of button being pressed
	 */
	protected void buttonPressed(int buttonId) {
		migrationName = txtName.getText().trim();
		if (buttonId != IDialogConstants.CANCEL_ID) {
			String error = MigrationScriptManager.getInstance().checkScriptName(migrationName,
					script);
			if (StringUtils.isNotBlank(error)) {
				MessageDialog.openError(getShell(), Messages.msgError, error);
				return;
			}
		}
		if (buttonId == IDialogConstants.NEXT_ID) {
			setReturnCode(IDialogConstants.NEXT_ID);
			close();
		}
		super.buttonPressed(buttonId);
	}

	/**
	 * Create buttons in button bar
	 * 
	 * @param parent the parent composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, Messages.btnStartNow, true);
		createButton(parent, IDialogConstants.NEXT_ID, Messages.btnReservation, false);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
	}

	/**
	 * Creates the page content
	 * 
	 * @param parent the parent composite to contain the dialog area
	 * @return the dialog area control
	 */
	protected Control createDialogArea(Composite parent) {
		this.getShell().setText(Messages.titleRunMigration);
		Composite rootCom = new Composite(parent, SWT.NONE);
		rootCom.setLayout(new GridLayout());
		rootCom.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Composite workArea1 = new Composite(rootCom, SWT.BORDER);
		workArea1.setLayout(new GridLayout(2, false));
		workArea1.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));

		Label lblName = new Label(workArea1, SWT.NONE);
		lblName.setText("Migration Name:");
		txtName = new Text(workArea1, SWT.BORDER);
		txtName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtName.setText(this.migrationName);
		txtName.selectAll();

		Composite workArea = new Composite(rootCom, SWT.BORDER);
		workArea.setLayout(new GridLayout(2, false));
		workArea.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Label btnImageStartNow = new Label(workArea, SWT.NONE);
		GridData layoutData = new GridData(SWT.RIGHT, SWT.CENTER, false, false);
		layoutData.horizontalIndent = 15;
		layoutData.verticalIndent = 15;

		btnImageStartNow.setLayoutData(layoutData);
		btnImageStartNow.setImage(MigrationUIPlugin.getImage("icon/tb/tb_new_wizard.png"));

		Label lblStartNow = new Label(workArea, SWT.NONE);
		layoutData = new GridData(SWT.FILL, SWT.CENTER, true, false);
		layoutData.horizontalIndent = 5;
		layoutData.verticalIndent = 15;
		lblStartNow.setLayoutData(layoutData);
		lblStartNow.setText(Messages.lblStartNow);

		Label btnImageReservation = new Label(workArea, SWT.NONE);
		layoutData = new GridData(SWT.RIGHT, SWT.CENTER, false, false);
		layoutData.horizontalIndent = 15;
		layoutData.verticalIndent = 5;
		btnImageReservation.setLayoutData(layoutData);
		btnImageReservation.setImage(MigrationUIPlugin.getImage("icon/tb/tb_reservation.png"));
		Label lblReservation = new Label(workArea, SWT.NONE);
		layoutData = new GridData(SWT.FILL, SWT.CENTER, true, false);
		layoutData.horizontalIndent = 5;
		layoutData.verticalIndent = 5;
		lblReservation.setLayoutData(layoutData);
		lblReservation.setText(Messages.lblReservation);
		return workArea;
	}

	public String getMigrationName() {
		return migrationName;
	}

	public void setMigrationName(String migrationName) {
		this.migrationName = migrationName == null ? "" : migrationName;
	}

	public void setScript(MigrationScript script) {
		this.script = script;
	}
}