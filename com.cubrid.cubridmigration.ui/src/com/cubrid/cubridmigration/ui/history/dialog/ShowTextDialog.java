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

import java.io.File;
import java.io.IOException;

import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.TrayDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * NewDBConnectionDialog
 * 
 * @author moulinwang JessieHuang Kevin Cao
 * @version 1.0 - 2009-10-12
 */
public class ShowTextDialog extends
		TrayDialog {

	private static final int SAVE_ID = 10;
	private final String failedLog;

	/**
	 * Create the dialog
	 * 
	 * @param parentShell
	 */
	public ShowTextDialog(Shell parentShell, String failedLog) {
		super(parentShell);
		this.failedLog = failedLog;
	}

	/**
	 * The dialog's shell style
	 * 
	 * @return shell style
	 */
	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE | SWT.MAX;
	}

	/**
	 * Create contents of the dialog
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite area = (Composite) super.createDialogArea(parent);
		//PlatformUI.getWorkbench().getHelpSystem().setHelp(area,CubridMigrationHelpContextIDs.ERROR_LOG);

		final GridData gdArea = new GridData(SWT.FILL, SWT.FILL, true, true);
		gdArea.verticalIndent = 5;
		gdArea.horizontalIndent = 5;
		area.setLayoutData(gdArea);
		Text text = new Text(area, SWT.WRAP | SWT.V_SCROLL | SWT.READ_ONLY | SWT.MULTI
				| SWT.H_SCROLL | SWT.BORDER);
		text.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		text.setText(failedLog);

		return area;
	}

	/**
	 * Create contents of the button bar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, SAVE_ID, Messages.dialogBtnSave, false);
		createButton(parent, IDialogConstants.OK_ID, IDialogConstants.CLOSE_LABEL, true);
	}

	/**
	 * configureShell
	 * 
	 * @param newShell Shell
	 */
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setText(Messages.ddlDialogShellText);
	}

	/**
	 * click OK button
	 * 
	 * @param buttonId int
	 */
	protected void buttonPressed(int buttonId) {

		if (buttonId == SAVE_ID) {
			FileDialog dialog = new FileDialog(getShell(), SWT.SAVE);
			dialog.setOverwrite(true);
			dialog.setFilterPath(".");
			dialog.setFilterNames(new String[] {"*.txt"});
			dialog.setFilterExtensions(new String[] {"*.txt"});

			String fileName = dialog.open();

			if (fileName != null) {
				try {
					CUBRIDIOUtils.writeLines(new File(fileName), new String[] {failedLog});
					MessageDialog.openInformation(
							PlatformUI.getWorkbench().getDisplay().getActiveShell(),
							Messages.msgInformation, "OK");
				} catch (IOException e) {
					MessageDialog.openError(
							PlatformUI.getWorkbench().getDisplay().getActiveShell(),
							Messages.msgError,
							Messages.failedLogDialogSaveErrMsg + "\n" + e.getMessage());
					return;
				}
			}
		}

		super.buttonPressed(buttonId);
	}

	/**
	 * The initial size of dialog
	 * 
	 * @return Point
	 */
	protected Point getInitialSize() {
		return new Point(650, 500);
	}

	/**
	 * Remove help button
	 * 
	 * @return false
	 */
	public boolean isHelpAvailable() {
		return false;
	}

}
