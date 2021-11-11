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
package com.cubrid.cubridmigration.ui.product;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.TrayDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.osgi.service.prefs.Preferences;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * CopyrightDialog
 * 
 * @author moulinwang
 * @version 1.0 - 2011-02-18
 */
public class CopyrightDialog extends
		TrayDialog {

	private static final Logger LOGGER = LogUtil.getLogger(ProductInfoDialog.class);
	String copyRightTxt;
	private Button btnShowWindowAgain;
	private Button btnAgreed;

	/**
	 * Create the dialog
	 * 
	 * @param parentShell
	 */
	public CopyrightDialog(Shell parentShell, String copyRightTxt) {
		super(parentShell);
		this.copyRightTxt = copyRightTxt;
	}

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

		final GridData gdArea = new GridData(SWT.FILL, SWT.FILL, true, true);
		area.setLayoutData(gdArea);
		Text text = new Text(area, SWT.WRAP | SWT.V_SCROLL | SWT.READ_ONLY | SWT.MULTI
				| SWT.H_SCROLL | SWT.BORDER);
		text.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		text.setText(copyRightTxt);

		btnAgreed = new Button(area, SWT.CHECK);
		btnAgreed.setText(Messages.btnAgreed);
		btnAgreed.setSelection(false);
		btnAgreed.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent se) {
				getButton(IDialogConstants.OK_ID).setEnabled(btnAgreed.getSelection());
			}
		});

		btnShowWindowAgain = new Button(area, SWT.CHECK);
		btnShowWindowAgain.setText(Messages.btnShowWindowAgain);
		btnShowWindowAgain.setSelection(false);

		return area;
	}

	/**
	 * Create contents of the button bar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, IDialogConstants.OK_LABEL, false);
		createButton(parent, IDialogConstants.CANCEL_ID, IDialogConstants.CANCEL_LABEL, true);
		getButton(IDialogConstants.OK_ID).setEnabled(false);
	}

	/**
	 * configureShell
	 * 
	 * @param newShell Shell
	 */
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setText("Copyright of CUBRID Migration Toolkit");
	}

	/**
	 * click OK button
	 * 
	 * @param buttonId int
	 */
	protected void buttonPressed(int buttonId) {
		try {
			if (IDialogConstants.OK_ID == buttonId) {
				Preferences preference = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
				preference.putBoolean("btnShowWindowAgain", !btnShowWindowAgain.getSelection());
				preference.flush();
			}
		} catch (Exception ex) {
			LOGGER.error(LogUtil.getExceptionString(ex));
		}
		super.buttonPressed(buttonId);
	}

	protected Point getInitialSize() {
		return new Point(650, 500);
	}

}
