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

import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * CSVSettingsDialog provides CSV file format settings
 * 
 * @author caoyilin
 * @version 1.0 - 2012-10-12 created by caoyilin
 */
public class CSVSettingsDialog extends
		Dialog {

	private Text txtSeparator;
	private Text txtQuoteChar;
	private Text txtEscapeChar;

	private final MigrationConfiguration config;

	public CSVSettingsDialog(Shell parentShell, MigrationConfiguration config) {
		super(parentShell);
		this.config = config;
	}

	/**
	 * Validation and save
	 * 
	 * @param buttonId int
	 */
	protected void buttonPressed(int buttonId) {
		if (buttonId == IDialogConstants.OK_ID) {
			if (StringUtils.isEmpty(txtSeparator.getText())) {
				MessageDialog.openError(getShell(), Messages.msgError,
						Messages.msgErrEmptySeparator);
				txtSeparator.setFocus();
				return;
			} else {
				config.getCsvSettings().setSeparateChar(
						txtSeparator.getText().charAt(0));
			}
			if (StringUtils.isEmpty(txtQuoteChar.getText())) {
				config.getCsvSettings().setQuoteChar(
						MigrationConfiguration.CSV_NO_CHAR);
			} else {
				config.getCsvSettings().setQuoteChar(
						txtQuoteChar.getText().charAt(0));
			}
			if (StringUtils.isEmpty(txtEscapeChar.getText())) {
				config.getCsvSettings().setEscapeChar(
						MigrationConfiguration.CSV_NO_CHAR);
			} else {
				config.getCsvSettings().setEscapeChar(
						txtEscapeChar.getText().charAt(0));
			}
		} else if (buttonId == IDialogConstants.BACK_ID) {
			txtSeparator.setText(",");
			txtQuoteChar.setText("\"");
			txtEscapeChar.setText("");
		}
		super.buttonPressed(buttonId);
	}

	/**
	 * constrainShellSize
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setSize(400, 210);
		getShell().setText(Messages.titleCSVSettings);
	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.BACK_ID, Messages.btnCSVDefault,
				false);
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, false);
		getButton(IDialogConstants.OK_ID).setEnabled(true);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel,
				false);
	}

	/**
	 * Initialize text values
	 * 
	 */
	private void initTexts() {
		if (config.getCsvSettings().getSeparateChar() == MigrationConfiguration.CSV_NO_CHAR) {
			txtSeparator.setText("");
		} else {
			txtSeparator.setText(String.valueOf(config.getCsvSettings().getSeparateChar()));
		}
		if (config.getCsvSettings().getQuoteChar() == MigrationConfiguration.CSV_NO_CHAR) {
			txtQuoteChar.setText("");
		} else {
			txtQuoteChar.setText(String.valueOf(config.getCsvSettings().getQuoteChar()));
		}
		if (config.getCsvSettings().getEscapeChar() == MigrationConfiguration.CSV_NO_CHAR) {
			txtEscapeChar.setText("");
		} else {
			txtEscapeChar.setText(String.valueOf(config.getCsvSettings().getEscapeChar()));
		}
		txtSeparator.setFocus();
		txtSeparator.selectAll();
	}

	/**
	 * createDialogArea
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite parentComp = (Composite) super.createDialogArea(parent);
		final Composite composite = new Composite(parentComp, SWT.BORDER);
		composite.setLayout(new GridLayout(2, false));
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));

		Label lblSeparator = new Label(composite, SWT.NONE);
		lblSeparator.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false,
				false));
		lblSeparator.setText(Messages.btnCSVSeparator);
		txtSeparator = new Text(composite, SWT.BORDER);
		txtSeparator.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true,
				false));
		txtSeparator.setTextLimit(1);

		Label lblQuote = new Label(composite, SWT.NONE);
		lblQuote.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblQuote.setText(Messages.btnCSVQuoteChar);
		txtQuoteChar = new Text(composite, SWT.BORDER);
		txtQuoteChar.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true,
				false));
		txtQuoteChar.setTextLimit(1);

		Label lblEscapeChar = new Label(composite, SWT.NONE);
		lblEscapeChar.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false,
				false));
		lblEscapeChar.setText(Messages.btnCSVEscapeChar);
		txtEscapeChar = new Text(composite, SWT.BORDER);
		txtEscapeChar.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true,
				false));
		txtEscapeChar.setTextLimit(1);

		initTexts();

		return parentComp;
	}

}
