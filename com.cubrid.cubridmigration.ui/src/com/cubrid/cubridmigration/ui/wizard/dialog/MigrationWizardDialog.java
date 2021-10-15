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
package com.cubrid.cubridmigration.ui.wizard.dialog;

import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.IPageChangedListener;
import org.eclipse.jface.dialogs.IPageChangingListener;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.wizard.IWizardPage;
import org.eclipse.jface.wizard.ProgressMonitorPart;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.jface.wizard.WizardDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.page.SelectSrcTarTypesPage;

/**
 * Migration Wizard Dialog
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-12-28 created by Kevin Cao
 */
public class MigrationWizardDialog extends
		WizardDialog {

	public MigrationWizardDialog(Shell parentShell, final Wizard newWizard) {
		super(parentShell, newWizard);
		setHelpAvailable(false);
		setShellStyle(SWT.CLOSE | SWT.MAX | SWT.TITLE | SWT.BORDER | SWT.APPLICATION_MODAL
				| SWT.RESIZE | getDefaultOrientation());

	}

	/**
	 * Overwrite the method. disable the ProgressMonitorPart which take up place
	 * on bottom of page
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite composite = (Composite) super.createDialogArea(parent);
		for (Control control : composite.getChildren()) {
			if (control instanceof ProgressMonitorPart) {
				GridData gd = (GridData) control.getLayoutData();
				gd.exclude = true;
			}
		}

		return composite;
	}

	/**
	 * Overwrite the method. Auto add IPageChangingListener(s);
	 * 
	 * @param parent of the control.
	 * @return Control
	 */
	protected Control createContents(Composite parent) {
		Control result = super.createContents(parent);
		IWizardPage[] pages = this.getWizard().getPages();
		for (IWizardPage page : pages) {
			if (page instanceof IPageChangingListener) {
				this.addPageChangingListener((IPageChangingListener) page);
			}
			if (page instanceof IPageChangedListener) {
				this.addPageChangedListener((IPageChangedListener) page);
			}
		}
		return result;
	}

	/**
	 * constrainShellSize
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setMinimumSize(750, 450);
		UICommonTool.centerShell(getShell());

	}

	/**
	 * rename finish button text
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		super.createButtonsForButtonBar(parent);
		Button finishButton = super.getButton(IDialogConstants.FINISH_ID);
		finishButton.setText(Messages.startButtonText);
		Button btnBack = super.getButton(IDialogConstants.BACK_ID);
		btnBack.setText(Messages.btnBack);
		Button btnNext = super.getButton(IDialogConstants.NEXT_ID);
		btnNext.setText(Messages.btnNext);
		Button btnCancel = super.getButton(IDialogConstants.CANCEL_ID);
		btnCancel.setText(Messages.btnCancel);
	}

	/**
	 * Back button pressed
	 */
	protected void backPressed() {
		IWizardPage prePage = getCurrentPage().getPreviousPage();
		if (prePage instanceof SelectSrcTarTypesPage) {
			if (!MessageDialog.openConfirm(getShell(), Messages.msgConfirmation,
					Messages.msgConfirmationChangedType)) {
				return;
			}
		}
		super.backPressed();
	}

}
