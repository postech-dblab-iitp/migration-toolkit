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

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Spinner;

import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.preference.MigrationConfigPage;

/**
 * Performance settings dialog
 * 
 * @author cn13425
 * @version 1.0 - 2013-01-22 created by cn13425
 */
public class PerformanceSettingsDialog extends
		Dialog {

	private final static int DEFAULT_ID = -1001;

	private final MigrationConfiguration config;

	private Spinner txtThreadCount;

	private Spinner txtMaxImportThreadCountPerTable;

	private Spinner txtPageCount;

	private Spinner txtCommitCount;

	private Spinner txtFileMaxSize;

	private Button btnImplicitEstimate;

	public PerformanceSettingsDialog(Shell parentShell, MigrationConfiguration config) {
		super(parentShell);
		this.config = config;
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
		final Composite composite = new Composite(parent, SWT.NONE);
		composite.setLayout(new GridLayout());
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));

		final Composite container = new Composite(composite, SWT.BORDER);
		container.setLayout(new GridLayout(2, false));
		container.setLayoutData(new GridData(GridData.FILL_BOTH));

		Label lblThreadCount = new Label(container, SWT.NONE);
		lblThreadCount.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblThreadCount.setText(Messages.btnThreadCount);

		txtThreadCount = new Spinner(container, SWT.BORDER);
		txtThreadCount.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtThreadCount.setValues(config.getExportThreadCount(), 1, 50, 0, 1, 5);

		Label lblImportThreadCount = new Label(container, SWT.NONE);
		lblImportThreadCount.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblImportThreadCount.setText(Messages.maxImportThreadCountPerTable);

		txtMaxImportThreadCountPerTable = new Spinner(container, SWT.BORDER);
		txtMaxImportThreadCountPerTable.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true,
				false));
		txtMaxImportThreadCountPerTable.setValues(config.getImportThreadCount(), 1, 50, 0, 1, 5);

		if (isSupportPageQuery()) {
			Label lblPageCount = new Label(container, SWT.NONE);
			lblPageCount.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblPageCount.setText(Messages.btnPageFetchingCount);

			txtPageCount = new Spinner(container, SWT.BORDER);
			txtPageCount.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtPageCount.setValues(config.getPageFetchCount(), 100, Integer.MAX_VALUE, 0, 100, 1000);
		}

		Label lblCommitCount = new Label(container, SWT.NONE);
		lblCommitCount.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblCommitCount.setText(Messages.btnCommitCount);

		txtCommitCount = new Spinner(container, SWT.BORDER);
		txtCommitCount.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtCommitCount.setValues(config.getCommitCount(), MigrationConfigPage.MIN_CC,
				MigrationConfigPage.MAX_CC, 0, 1, 1000);

		if (config.targetIsFile() && config.isOneTableOneFile()) {
			Label lblMaxSize = new Label(container, SWT.NONE);
			lblMaxSize.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblMaxSize.setText(Messages.lblFileMaxSize);

			txtFileMaxSize = new Spinner(container, SWT.BORDER);
			txtFileMaxSize.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtFileMaxSize.setToolTipText(Messages.ttFileMaxSize);
			int maxValue = Integer.MAX_VALUE;
			if (config.getDestType() == MigrationConfiguration.DEST_XLS) {
				maxValue = MigrationConfiguration.XLS_MAX_COUNT;
			}
			txtFileMaxSize.setValues(config.getMaxCountPerFile(), 0, maxValue, 0, 1, 1000);
		}
		Label lblImplicit = new Label(container, SWT.NONE);
		lblImplicit.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		//lblImplicit.setText(Messages.lblImplicitEstimate);

		btnImplicitEstimate = new Button(container, SWT.CHECK);
		btnImplicitEstimate.setSelection(config.isImplicitEstimate());
		btnImplicitEstimate.setText(Messages.lblImplicitEstimate);
		//btnImplicitEstimate.setText(Messages.btnImplicitEstimate);
		btnImplicitEstimate.addSelectionListener(new SelectionListener() {
			public void widgetSelected(SelectionEvent ex) {
				if (btnImplicitEstimate.getSelection()) {
					UICommonTool.openInformationBox(getShell(), Messages.msgWarning,
							Messages.msgImplicitEstimate);
				}
			}

			public void widgetDefaultSelected(SelectionEvent ex) {
			}
		});

		//		if (config.getSourceDBType() == DatabaseType.CUBRID) {
		//			btnImplicitEstimate.setSelection(false);
		//			btnImplicitEstimate.setEnabled(false);
		//		}
		return parent;
	}

	private boolean isSupportPageQuery() {
		return config.sourceIsOnline()
				&& (config.getSourceDBType().getID() == DatabaseType.CUBRID.getID() || config.getSourceDBType().getID() == DatabaseType.MSSQL.getID());
	}

	/**
	 * constrainShellSize
	 */

	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setMinimumSize(550, 220);
		getShell().setText(Messages.titlePerformanceSettings);
	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, DEFAULT_ID, Messages.btnDefaultURL, false);
		Button btnOK = createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, true);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
		btnOK.setEnabled(true);
	}

	/**
	 * @return shell style
	 */
	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE | SWT.MAX;
	}

	/**
	 * buttonPressed
	 * 
	 * @param buttonId int
	 */
	protected void buttonPressed(int buttonId) {
		if (buttonId == DEFAULT_ID) {
			txtThreadCount.setValues(MigrationConfigPage.getDefaultExportThreadCount(), 1, 50, 0,
					1, 1);
			txtMaxImportThreadCountPerTable.setValues(
					MigrationConfigPage.getDefaultImpportThreadCountEachTable(), 1, 50, 0, 1, 1);
			if (txtPageCount != null) {
				txtPageCount.setValues(MigrationConfigPage.getPageFetchingCount(), 1,
						Integer.MAX_VALUE, 0, 100, 1000);
			}
			if (txtFileMaxSize != null) {
				config.setMaxCountPerFile(0);
			}
			txtCommitCount.setValues(MigrationConfigPage.getCommitCount(),
					MigrationConfigPage.MIN_CC, MigrationConfigPage.MAX_CC, 0, 100, 1000);
			btnImplicitEstimate.setSelection(false);
		}
		super.buttonPressed(buttonId);
	}

	/**
	 * If OK button pressed
	 */
	protected void okPressed() {
		config.setExportThreadCount(txtThreadCount.getSelection());
		config.setImportThreadCount(txtMaxImportThreadCountPerTable.getSelection());
		config.setCommitCount(txtCommitCount.getSelection());
		if (txtPageCount != null) {
			config.setPageFetchCount(txtPageCount.getSelection());
		}
		if (txtFileMaxSize != null) {
			config.setMaxCountPerFile(txtFileMaxSize.getSelection());
		}
		config.setImplicitEstimate(btnImplicitEstimate.getSelection());
		super.okPressed();
	}

}
