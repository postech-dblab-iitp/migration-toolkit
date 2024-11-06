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
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.TitleAreaDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * AddSQLDialog
 * 
 * @author moulinwang fulei caoyilin
 */
public class SQLEditorDialog extends
		TitleAreaDialog {

	/**
	 * Add a new SQL table into configuration
	 * 
	 * @param config MigrationConfiguration
	 * @return new SourceSQLTableConfig
	 */
	public static List<SourceSQLTableConfig> addSQL(MigrationConfiguration config) {
		SQLEditorDialog dialog = new SQLEditorDialog(Display.getDefault().getActiveShell(), config,
				null, true);
		if (SQLEditorDialog.OK == dialog.open()) {
			return dialog.newsstcs;
		}
		return null;
	}

	/**
	 * Edit a exists SourceSQLTableConfig
	 * 
	 * @param config MigrationConfiguration
	 * @param sstc SourceSQLTableConfig
	 * @return boolean
	 */
	public static boolean editSQL(MigrationConfiguration config, SourceSQLTableConfig sstc) {
		SQLEditorDialog dialog = new SQLEditorDialog(Display.getDefault().getActiveShell(), config,
				sstc, false);
		return SQLEditorDialog.OK == dialog.open();
	}

	private Text txtSQL = null;
	private final boolean newFlag;

	private SourceSQLTableConfig sstc;
	private List<SourceSQLTableConfig> newsstcs = new ArrayList<SourceSQLTableConfig>();

	private final MigrationConfiguration config;

	public SQLEditorDialog(Shell parentShell, MigrationConfiguration config,
			SourceSQLTableConfig sourceSQLTableConfig, boolean newFlag) {
		super(parentShell);
		this.config = config;
		this.sstc = sourceSQLTableConfig;
		this.newFlag = newFlag;
		this.setHelpAvailable(false);
	}

	/**
	 * Validation and save
	 * 
	 * @param buttonId int
	 */
	protected void buttonPressed(int buttonId) {
		if (buttonId == IDialogConstants.OK_ID) {
			String sql = txtSQL.getText().trim();
			if (StringUtils.isBlank(sql)) {
				setErrorMessage(Messages.addSQLDialogErrorMess + sql);
				return;
			}

			if (newFlag) {
				if (!saveNewSQL()) {
					return;
				}
			} else {
				if (!saveEditSQL()) {
					return;
				}
			}
		}
		super.buttonPressed(buttonId);
	}

	/**
	 * constrainShellSize
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setSize(700, 480);
		getShell().setText(Messages.addSQLDialogShellTitle);
		//TODO: Show supporting pagination query message.
		if (config.sourceIsOnline()) {
			setMessage(Messages.msgSQLParameterSupport);
		}

	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, false);
		getButton(IDialogConstants.OK_ID).setEnabled(!config.isSourceOfflineMode());
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
	}

	/**
	 * Create Database Name Group
	 * 
	 * @param composite Composite
	 */
	private void createdbNameGroup(Composite composite) {

		final Composite dbnameGroup = new Composite(composite, SWT.NONE);
		GridLayout layout = new GridLayout();
		layout.marginWidth = 0;
		layout.marginHeight = 0;

		GridData gdDbnameGroup = new GridData(GridData.FILL_BOTH);
		dbnameGroup.setLayoutData(gdDbnameGroup);
		dbnameGroup.setLayout(layout);
		layout = new GridLayout();
		layout.marginWidth = 10;
		layout.marginHeight = 10;
		layout.numColumns = 2;
		gdDbnameGroup = new GridData(GridData.FILL_BOTH);
		final Group group = new Group(dbnameGroup, SWT.NONE);
		group.setLayoutData(gdDbnameGroup);
		group.setLayout(layout);
		group.setText(Messages.addGraphSQLDialogGroupTitle);

		txtSQL = new Text(group, SWT.BORDER | SWT.WRAP | SWT.MULTI | SWT.V_SCROLL);

		final GridData parameterName = new GridData(SWT.FILL, SWT.FILL, true, true, 2, 1);
		txtSQL.setLayoutData(parameterName);

	}

	/**
	 * createDialogArea
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite parentComp = (Composite) super.createDialogArea(parent);

		final Composite composite = new Composite(parentComp, SWT.NONE);
		GridLayout layout = new GridLayout();
		layout.marginHeight = convertVerticalDLUsToPixels(IDialogConstants.VERTICAL_MARGIN);
		layout.marginWidth = convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_MARGIN);
		layout.verticalSpacing = convertVerticalDLUsToPixels(IDialogConstants.VERTICAL_SPACING);
		layout.horizontalSpacing = convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_SPACING);
		layout.numColumns = 2;
		composite.setLayout(layout);
		final GridData gdComposit = new GridData(GridData.FILL_BOTH);
		composite.setLayoutData(gdComposit);
		createdbNameGroup(composite);

		if (newFlag) {
			setTitle(Messages.addGraphSQLDialogTitle1);
			setMessage(Messages.addGraphSQLDialogMessage1);
		} else {
			setTitle(Messages.addGraphSQLDialogTitle2);
			setMessage(Messages.addGraphSQLDialogMessage2);
		}

		initial();
		return parentComp;
	}

	/**
	 * Dialog shell style
	 * 
	 * @return SWT.RESIZE | SWT.MAX
	 */
	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE | SWT.MAX;
	}

	/**
	 * initialization
	 * 
	 */
	private void initial() {
		if (newFlag) {
			txtSQL.setText("");
		} else {
			txtSQL.setText(sstc.getSql());
		}
	}

	/**
	 * Save edit SQL to configuration
	 * 
	 * @return true if successfully
	 */
	private boolean saveEditSQL() {
		String sql = txtSQL.getText().trim();
		final boolean sqlnochanged = sql.equals(sstc.getSql());
		if (sqlnochanged) {
			return true;
		}
		//Check duplicated SQL
		if (config.getExpSQLCfgBySql(sql) != null) {
			setErrorMessage(Messages.addSQLRepeatErrorMessage);
			return false;
		}
		try {
			//Validate SQL and replace
			config.replaceSQL(sstc, sstc.getName(), sql);
		} catch (Exception ex) {
			setErrorMessage(Messages.addSQLDialogErrorMess + sql);
			MessageDialog.openError(getShell(), Messages.msgWarning, ex.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Save new SQL to configuration
	 * 
	 * @return true if successfully
	 */
	private boolean saveNewSQL() {
		String[] sqls = txtSQL.getText().trim().split(";");
		int i = 0;
		newsstcs.clear();
		for (String sql : sqls) {
			sql = sql.trim();
			if (StringUtils.isEmpty(sql)) {
				continue;
			}
			SourceSQLTableConfig sqlCfg = config.getExpSQLCfgBySql(sql);
			if (sqlCfg != null) {
				setErrorMessage(Messages.addSQLRepeatErrorMessage);
				return false;
			}

			try {
				config.validateExpSQLConfig(sql);
				i++;
				String newName = "SQL" + i;
				String tableName = "table" + i;
				while (config.getExpSQLCfgByName(newName) != null) {
					i++;
					newName = "SQL" + i;
				}
				while (config.getTargetTableSchema(tableName) != null) {
					i++;
					tableName = "table" + i;
				}
				SourceSQLTableConfig newSTC = new SourceSQLTableConfig();
				newSTC.setName(newName);
				newSTC.setTarget(tableName);
				newSTC.setSql(sql);
				newSTC.setCreateNewTable(false);
				newSTC.setReplace(false);
				newSTC.setMigrateData(true);
				newsstcs.add(newSTC);
			} catch (Exception ex) {
				setErrorMessage(Messages.addSQLDialogErrorMess + sql);
				DetailMessageDialog.openError(getShell(), Messages.msgError,
						Messages.msgInvalidSQL, ex.getMessage());
				return false;
			}
		}
		//Add new SQL to configuration 
		for (SourceSQLTableConfig sstc : newsstcs) {
			config.addExpSQLTableCfgWithST(sstc);
		}
		return true;
	}
}
