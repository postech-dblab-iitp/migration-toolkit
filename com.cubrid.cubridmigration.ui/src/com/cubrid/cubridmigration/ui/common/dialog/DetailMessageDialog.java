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
package com.cubrid.cubridmigration.ui.common.dialog;

import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.IconAndMessageDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * 
 * This dialog shows message with details.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-03-17 created by Kevin Cao
 */
public class DetailMessageDialog extends
		IconAndMessageDialog {

	private int dlgStyle;
	private String detailMsg;
	private Button detailsButton;
	private Text txtDetail;
	private boolean listCreated;
	private String title;

	public DetailMessageDialog(Shell parentShell, int dlgStyle, String title, String message,
			String detailMsg) {
		super(parentShell);
		this.title = title;
		this.dlgStyle = dlgStyle;
		this.message = message;
		this.detailMsg = detailMsg;
	}

	/**
	 * openError with details
	 * 
	 * @param parentShell Shell
	 * @param title String
	 * @param message String
	 * @param detailMsg String
	 */
	public static void openError(Shell parentShell, String title, String message, String detailMsg) {
		DetailMessageDialog dlg = new DetailMessageDialog(parentShell, MessageDialog.ERROR, title,
				message, detailMsg);
		dlg.open();
	}

	/**
	 * openInfo with details
	 * 
	 * @param parentShell Shell
	 * @param title String
	 * @param message String
	 * @param detailMsg String
	 */
	public static void openInfo(Shell parentShell, String title, String message, String detailMsg) {
		DetailMessageDialog dlg = new DetailMessageDialog(parentShell, MessageDialog.INFORMATION,
				title, message, detailMsg);
		dlg.open();
	}

	/**
	 * openWarning with details
	 * 
	 * @param parentShell Shell
	 * @param title String
	 * @param message String
	 * @param detailMsg String
	 */
	public static void openWarning(Shell parentShell, String title, String message, String detailMsg) {
		DetailMessageDialog dlg = new DetailMessageDialog(parentShell, MessageDialog.WARNING,
				title, message, detailMsg);
		dlg.open();
	}

	/**
	 * openConfirm with details
	 * 
	 * @param parentShell Shell
	 * @param title String
	 * @param message String
	 * @param detailMsg String
	 * @return true if confirmed
	 */
	public static boolean openConfirm(Shell parentShell, String title, String message,
			String detailMsg) {
		DetailMessageDialog dlg = new DetailMessageDialog(parentShell, MessageDialog.CONFIRM,
				title, message, detailMsg);
		int sel = dlg.open();
		return (sel == IDialogConstants.OK_ID);
	}

	/**
	 * (non-Javadoc) Method declared in Window.
	 * 
	 * @param shell Shell
	 */
	protected void configureShell(Shell shell) {
		super.configureShell(shell);
		shell.setText(title);
	}

	/**
	 * createDialogArea
	 * 
	 * @param parent Composite
	 * @return composite
	 */
	protected Control createDialogArea(Composite parent) {
		Composite composite = new Composite(parent, SWT.NONE);
		GridLayout layout = new GridLayout();
		layout.marginHeight = convertVerticalDLUsToPixels(IDialogConstants.VERTICAL_MARGIN);
		layout.marginWidth = convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_MARGIN);
		layout.verticalSpacing = convertVerticalDLUsToPixels(IDialogConstants.VERTICAL_SPACING);
		layout.horizontalSpacing = convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_SPACING);
		layout.numColumns = 2;
		composite.setLayout(layout);
		GridData childData = new GridData(GridData.FILL_BOTH);
		childData.horizontalSpan = 2;
		childData.grabExcessVerticalSpace = false;
		composite.setLayoutData(childData);
		composite.setFont(parent.getFont());
		createMessageArea(composite);
		return composite;
	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 * 
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		// create OK and Details buttons
		createButton(parent, IDialogConstants.OK_ID, IDialogConstants.OK_LABEL, true);
		if (dlgStyle == MessageDialog.CONFIRM) {
			createButton(parent, IDialogConstants.CANCEL_ID, IDialogConstants.CANCEL_LABEL, false);
		}
		detailsButton = createButton(parent, IDialogConstants.DETAILS_ID,
				IDialogConstants.SHOW_DETAILS_LABEL, false);
	}

	/**
	 * @return image on the left.
	 */
	protected Image getImage() {
		switch (dlgStyle) {
		case MessageDialog.CONFIRM:
			return getQuestionImage();
		case MessageDialog.ERROR:
			return getErrorImage();
		case MessageDialog.INFORMATION:
			return getInfoImage();
		case MessageDialog.WARNING:
			return getWarningImage();
		default:
			return null;
		}
	}

	/**
	 * buttonPressed
	 * 
	 * @param id button id
	 */
	protected void buttonPressed(int id) {
		if (id == IDialogConstants.DETAILS_ID) {
			// was the details button pressed?
			toggleDetailsArea();
		} else {
			super.buttonPressed(id);
		}
	}

	/**
	 * Show or hid detail area
	 * 
	 */
	private void toggleDetailsArea() {
		Point windowSize = getShell().getSize();
		Point oldSize = getShell().computeSize(SWT.DEFAULT, SWT.DEFAULT);
		if (listCreated) {
			txtDetail.dispose();
			listCreated = false;
			detailsButton.setText(IDialogConstants.SHOW_DETAILS_LABEL);
		} else {
			createDropDownText();
			detailsButton.setText(IDialogConstants.HIDE_DETAILS_LABEL);
			getContents().getShell().layout();
		}
		Point newSize = getShell().computeSize(SWT.DEFAULT, SWT.DEFAULT);
		getShell().setSize(new Point(windowSize.x, windowSize.y + (newSize.y - oldSize.y)));
	}

	/**
	 * createDropDownText
	 */
	private void createDropDownText() {
		Composite parent = (Composite) getContents();
		// create the list
		txtDetail = new Text(parent, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL | SWT.MULTI);
		// fill the txtDetail
		GridData data = new GridData(GridData.HORIZONTAL_ALIGN_FILL | GridData.GRAB_HORIZONTAL
				| GridData.VERTICAL_ALIGN_FILL | GridData.GRAB_VERTICAL);
		data.heightHint = 250;
		data.horizontalSpan = 2;
		txtDetail.setLayoutData(data);
		txtDetail.setFont(parent.getFont());
		txtDetail.setText(detailMsg == null ? "" : detailMsg);
		listCreated = true;
	}

	/**
	 * Dialog can be resized.
	 * 
	 * @return shell style
	 */
	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE;
	}

}