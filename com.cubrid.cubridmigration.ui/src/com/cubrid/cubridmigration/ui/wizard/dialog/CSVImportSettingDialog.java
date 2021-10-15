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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.common.CharsetUtils;
import com.cubrid.cubridmigration.core.engine.config.CSVSettings;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * CSVImportSettingDialog Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-3-15 created by Kevin Cao
 */
public class CSVImportSettingDialog extends
		Dialog {

	private Text txtSeparator;
	private Text txtQuoteChar;
	private Text txtEscapeChar;

	private Button btnNULL1;
	private Button btnNULL2;
	private Button btnNULL3;
	private Button btnNULL4;

	private Text txtNULL;

	private Combo cboCharset;

	private final CSVSettings settings = new CSVSettings();

	public CSVImportSettingDialog(Shell parentShell, MigrationConfiguration config) {
		super(parentShell);
		this.settings.copyFrom(config.getCsvSettings());
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
				settings.setSeparateChar(txtSeparator.getText().charAt(0));
			}
			if (StringUtils.isEmpty(txtQuoteChar.getText())) {
				settings.setQuoteChar(MigrationConfiguration.CSV_NO_CHAR);
			} else {
				settings.setQuoteChar(txtQuoteChar.getText().charAt(0));
			}
			if (StringUtils.isEmpty(txtEscapeChar.getText())) {
				settings.setEscapeChar(MigrationConfiguration.CSV_NO_CHAR);
			} else {
				settings.setEscapeChar(txtEscapeChar.getText().charAt(0));
			}

			List<String> nullStrings = new ArrayList<String>();
			if (btnNULL1.getSelection()) {
				nullStrings.add("\\N");
			}
			if (btnNULL2.getSelection()) {
				nullStrings.add("NULL");
			}
			if (btnNULL3.getSelection()) {
				nullStrings.add("(NULL)");
			}
			if (btnNULL4.getSelection() && StringUtils.isNotEmpty(txtNULL.getText())) {

				final String[] split = txtNULL.getText().split(";");
				for (String ss : split) {
					if (StringUtils.isNotEmpty(ss)) {
						nullStrings.add(ss);
					}
				}
			}
			settings.setNullStrings(nullStrings);
			settings.setCharset(cboCharset.getText());

		} else if (buttonId == IDialogConstants.BACK_ID) {
			txtSeparator.setText(",");
			txtQuoteChar.setText("\"");
			txtEscapeChar.setText("");
			btnNULL1.setSelection(true);
			btnNULL2.setSelection(true);
			btnNULL3.setSelection(true);
			btnNULL4.setSelection(false);
			txtNULL.setEnabled(btnNULL4.getSelection());
			txtNULL.setText("");
			cboCharset.setText("UTF-8");
		}
		super.buttonPressed(buttonId);
	}

	/**
	 * constrainShellSize
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setSize(400, 360);
		getShell().setText(Messages.titleCSVSettings);
	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.BACK_ID, Messages.btnCSVDefault, false);
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, false);
		getButton(IDialogConstants.OK_ID).setEnabled(true);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
	}

	/**
	 * createDialogArea
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite parentComp = (Composite) super.createDialogArea(parent);
		final Group grp1 = new Group(parentComp, SWT.NONE);
		grp1.setLayout(new GridLayout(2, false));
		grp1.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));

		Label lblSeparator = new Label(grp1, SWT.NONE);
		lblSeparator.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblSeparator.setText(Messages.btnCSVSeparator);
		txtSeparator = new Text(grp1, SWT.BORDER);
		txtSeparator.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtSeparator.setTextLimit(1);

		Label lblQuote = new Label(grp1, SWT.NONE);
		lblQuote.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblQuote.setText(Messages.btnCSVQuoteChar);
		txtQuoteChar = new Text(grp1, SWT.BORDER);
		txtQuoteChar.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtQuoteChar.setTextLimit(1);

		Label lblEscapeChar = new Label(grp1, SWT.NONE);
		lblEscapeChar.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblEscapeChar.setText(Messages.btnCSVEscapeChar);
		txtEscapeChar = new Text(grp1, SWT.BORDER);
		txtEscapeChar.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtEscapeChar.setTextLimit(1);

		final Group grp2 = new Group(parentComp, SWT.NONE);
		grp2.setLayout(new GridLayout(5, false));
		grp2.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));
		grp2.setText(Messages.lblNULLString);

		btnNULL1 = new Button(grp2, SWT.CHECK);
		btnNULL1.setText("\\N");
		btnNULL2 = new Button(grp2, SWT.CHECK);
		btnNULL2.setText("NULL");
		btnNULL3 = new Button(grp2, SWT.CHECK);
		btnNULL3.setText("(NULL)");
		btnNULL4 = new Button(grp2, SWT.CHECK);
		btnNULL4.setText((Messages.lblOtherNULLString));
		btnNULL4.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ex) {
				txtNULL.setEnabled(btnNULL4.getSelection());
			}
		});

		txtNULL = new Text(grp2, SWT.BORDER);
		txtNULL.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtNULL.setEnabled(btnNULL4.getSelection());

		final Group grp3 = new Group(parentComp, SWT.NONE);
		grp3.setLayout(new GridLayout(2, false));
		grp3.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));

		Label lblCharset = new Label(grp3, SWT.NONE);
		lblCharset.setText(Messages.lblCharset);

		cboCharset = new Combo(grp3, SWT.READ_ONLY);
		cboCharset.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		cboCharset.setItems(CharsetUtils.getCharsets());

		initTexts();
		return parentComp;
	}

	/**
	 * Retrieves the settings of this dialog.
	 * 
	 * @return CSVSettings
	 */
	public CSVSettings getSettings() {
		return settings;
	}

	/**
	 * Initialize text values
	 * 
	 */
	private void initTexts() {
		if (settings.getSeparateChar() == MigrationConfiguration.CSV_NO_CHAR) {
			txtSeparator.setText("");
		} else {
			txtSeparator.setText(String.valueOf(settings.getSeparateChar()));
		}
		if (settings.getQuoteChar() == MigrationConfiguration.CSV_NO_CHAR) {
			txtQuoteChar.setText("");
		} else {
			txtQuoteChar.setText(String.valueOf(settings.getQuoteChar()));
		}
		if (settings.getEscapeChar() == MigrationConfiguration.CSV_NO_CHAR) {
			txtEscapeChar.setText("");
		} else {
			txtEscapeChar.setText(String.valueOf(settings.getEscapeChar()));
		}
		txtSeparator.setFocus();
		txtSeparator.selectAll();

		List<String> nullStrings = settings.getNullStrings();
		btnNULL1.setSelection(nullStrings.indexOf("\\N") >= 0);
		btnNULL2.setSelection(nullStrings.indexOf("NULL") >= 0);
		btnNULL3.setSelection(nullStrings.indexOf("(NULL)") >= 0);

		nullStrings.remove("\\N");
		nullStrings.remove("NULL");
		nullStrings.remove("(NULL)");
		btnNULL4.setSelection(nullStrings.size() > 0);
		txtNULL.setEnabled(btnNULL4.getSelection());
		if (btnNULL4.getSelection()) {
			StringBuffer sb = new StringBuffer();
			for (String ss : nullStrings) {
				if (sb.length() > 0) {
					sb.append(";");
				}
				sb.append(ss);
			}
			txtNULL.setText(sb.toString());
		}

		cboCharset.setText(settings.getCharset());
	}
}
