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
package com.cubrid.cubridmigration.ui.wizard.page;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.cubrid.CUBRIDSQLHelper;
import com.cubrid.cubridmigration.ui.SWTResourceConstents;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.dialog.EditScriptDialog;
import com.cubrid.cubridmigration.ui.script.dialog.ExportScriptDialog;
import com.cubrid.cubridmigration.ui.wizard.dialog.PerformanceSettingsDialog;

/**
 * new wizard step 5. Base Confirm Migration Settings
 * 
 * @author caoyilin
 * @version 2.0 - 2013-8-23
 */
public class BaseConfirmationPage extends
		MigrationWizardPage {

	private static final String NEWLINE = System.getProperty("line.separator");
	protected StyledText txtSummary;
	protected StyledText txtDDL;
	private ToolItem btnPreviewDDL;
	protected Composite comRoot;
	protected ToolBar tbTools;

	public BaseConfirmationPage(String pageName) {
		super(pageName);
	}

	/**
	 * Create buttons in this page
	 * 
	 * @param parent of the buttons
	 */
	protected void createButtons(Composite parent) {
		comRoot = new Composite(parent, SWT.BORDER);
		comRoot.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, true, false));
		comRoot.setLayout(new GridLayout(2, false));

		tbTools = new ToolBar(comRoot, SWT.WRAP | SWT.RIGHT | SWT.FLAT);
		tbTools.setLayout(new GridLayout());
		tbTools.setLayoutData(new GridData(SWT.RIGHT, SWT.BOTTOM, true, false));

		final ToolItem btnSettings = new ToolItem(tbTools, SWT.NONE);
		btnSettings.setText(Messages.btnAdvancedSettings);
		btnSettings.setToolTipText(Messages.tipAdvancedSettings);
		btnSettings.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				PerformanceSettingsDialog dialog = new PerformanceSettingsDialog(getShell(),
						getMigrationWizard().getMigrationConfig());
				dialog.open();
			}
		});

		new ToolItem(tbTools, SWT.SEPARATOR);
		btnPreviewDDL = new ToolItem(tbTools, SWT.CHECK);
		btnPreviewDDL.setSelection(false);
		btnPreviewDDL.setText(Messages.btnPreviewDDL);
		btnPreviewDDL.setToolTipText(Messages.tipPreviewDDL);
		btnPreviewDDL.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				boolean flag = btnPreviewDDL.getSelection();
				switchText(flag);
			}
		});
		new ToolItem(tbTools, SWT.SEPARATOR);

		final ToolItem btnExportScript = new ToolItem(tbTools, SWT.NONE);
		btnExportScript.setText(Messages.btnExportScript);
		btnExportScript.setToolTipText(Messages.tipSaveScript);
		btnExportScript.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				exportScriptToFile();
			}
		});
		new ToolItem(tbTools, SWT.SEPARATOR);
		final ToolItem btnUpdateScript = new ToolItem(tbTools, SWT.NONE);
		btnUpdateScript.setText(Messages.btnUpdateScript);
		btnUpdateScript.setToolTipText(Messages.tipUpdateScript);
		btnUpdateScript.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				prepare4SaveScript();
				getMigrationWizard().saveMigrationScript(false, isSaveSchema());
				MessageDialog.openInformation(
						PlatformUI.getWorkbench().getDisplay().getActiveShell(),
						Messages.msgInformation, Messages.setOptionPageOKMsg);
			}
		});
		btnUpdateScript.setEnabled(getMigrationWizard().getMigrationScript() != null);
		new ToolItem(tbTools, SWT.SEPARATOR);
		final ToolItem btnNewScript = new ToolItem(tbTools, SWT.NONE);
		btnNewScript.setText(Messages.btnCreateNewScript);
		btnNewScript.setToolTipText(Messages.tipCreateNewScript);
		btnNewScript.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				prepare4SaveScript();
				String name = EditScriptDialog.getMigrationScriptName(getShell(),
						getMigrationWizard().getMigrationConfig().getName());
				if (StringUtils.isBlank(name)) {
					return;
				}
				getMigrationWizard().getMigrationConfig().setName(name);
				getMigrationWizard().saveMigrationScript(true, isSaveSchema());
				btnUpdateScript.setEnabled(getMigrationWizard().getMigrationScript() != null);
				MessageDialog.openInformation(
						PlatformUI.getWorkbench().getDisplay().getActiveShell(),
						Messages.msgInformation, Messages.setOptionPageOKMsg);
			}
		});
	}

	/**
	 * Create contents of the wizard
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		Composite container = new Composite(parent, SWT.NONE);
		container.setLayout(new GridLayout());
		setControl(container);

		Composite container2 = new Composite(container, SWT.BORDER);
		container2.setLayout(new GridLayout());
		container2.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		txtSummary = new StyledText(container2, SWT.LEFT | SWT.BORDER | SWT.READ_ONLY | SWT.WRAP
				| SWT.V_SCROLL);
		txtSummary.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		txtSummary.setBackground(SWTResourceConstents.COLOR_WHITE);

		txtDDL = new StyledText(container2, SWT.LEFT | SWT.BORDER | SWT.READ_ONLY | SWT.WRAP
				| SWT.V_SCROLL);
		GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
		gd.exclude = true;
		txtDDL.setLayoutData(gd);
		txtDDL.setVisible(false);
		txtDDL.setBackground(SWTResourceConstents.COLOR_WHITE);
		createButtons(container);
	}

	/**
	 * Export script to a external XML file
	 */
	protected void exportScriptToFile() {
		try {
			final MigrationConfiguration cfg = getMigrationWizard().getMigrationConfig();
			prepare4SaveScript();
			ExportScriptDialog.exportScript(cfg, isSaveSchema());
		} catch (Exception e) {
			//			MessageDialog.openError(PlatformUI.getWorkbench().getDisplay().getActiveShell(),
			//					Messages.msgWarning, Messages.setOptionPageErrMsg);
		}
	}

	/**
	 * postMigrationData
	 */
	protected void postMigrationData() {

	}

	/**
	 * Prepare for saving migration script.
	 * 
	 */
	protected void prepare4SaveScript() {
		MigrationConfiguration cfg = getMigrationWizard().getMigrationConfig();
		cfg.cleanNoUsedConfigForStart();
	}

	/**
	 * Set DDL to text area.
	 * 
	 */
	protected void setDDLText() {
		MigrationConfiguration cfg = getMigrationWizard().getMigrationConfig();
		if (cfg.sourceIsSQL() || cfg.targetIsGraph()) {
			btnPreviewDDL.setSelection(false);
			btnPreviewDDL.setEnabled(false);
			switchText(false);
			return;
		}
		txtDDL.setText("");
		prepare4SaveScript();
		List<Table> tables = new ArrayList<Table>();
		final CUBRIDSQLHelper ddlUtils = CUBRIDSQLHelper.getInstance(null);
		StringBuffer sbConstrains = new StringBuffer();
		for (SourceEntryTableConfig setc : cfg.getExpEntryTableCfg()) {
			if (!setc.isCreateNewTable()) {
				continue;
			}
			Table tarTbl = cfg.getTargetTableSchema(setc.getTarget());
			if (tarTbl == null || tables.contains(tarTbl)) {
				continue;
			}
			tables.add(tarTbl);
			String sql = ddlUtils.getTableDDL(tarTbl);
			txtDDL.append(sql);
			txtDDL.append(NEWLINE);

			final PK pk = tarTbl.getPk();
			if (setc.isCreatePK() && pk != null) {
				String ddl = ddlUtils.getPKDDL(tarTbl.getName(), pk.getName(), pk.getPkColumns());
				sbConstrains.append(ddl);
				sbConstrains.append(";");
				sbConstrains.append(NEWLINE);
			}
			for (FK fk : tarTbl.getFks()) {
				String ddl = ddlUtils.getFKDDL(tarTbl.getName(), fk);
				sbConstrains.append(ddl);
				sbConstrains.append(";");
				sbConstrains.append(NEWLINE);
			}
			for (Index idx : tarTbl.getIndexes()) {
				String ddl = ddlUtils.getIndexDDL(tarTbl.getName(), idx, "");
				sbConstrains.append(ddl);
				sbConstrains.append(";");
				sbConstrains.append(NEWLINE);
			}
		}
		for (SourceSQLTableConfig sstc : cfg.getExpSQLCfg()) {
			if (!sstc.isCreateNewTable()) {
				continue;
			}
			Table tarTbl = cfg.getTargetTableSchema(sstc.getTarget());
			if (tarTbl == null || tables.contains(tarTbl)) {
				continue;
			}
			tables.add(tarTbl);
			String sql = ddlUtils.getTableDDL(tarTbl);
			txtDDL.append(sql);
			txtDDL.append(NEWLINE);
		}
		for (SourceConfig sc : cfg.getExpViewCfg()) {
			if (!sc.isCreate()) {
				continue;
			}
			View vw = cfg.getTargetViewSchema(sc.getTarget());
			String ddl = ddlUtils.getViewDDL(vw);
			txtDDL.append(ddl);
			txtDDL.append(NEWLINE);
		}
		for (SourceConfig sc : cfg.getExpSerialCfg()) {
			if (!sc.isCreate()) {
				continue;
			}
			Sequence sq = cfg.getTargetSerialSchema(sc.getTarget());
			String ddl = ddlUtils.getSequenceDDL(sq);
			txtDDL.append(ddl);
			txtDDL.append(NEWLINE);
		}
		for (SourceCSVConfig sstc : cfg.getCSVConfigs()) {
			if (!sstc.isCreate()) {
				continue;
			}
			Table tarTbl = cfg.getTargetTableSchema(sstc.getTarget());
			if (tarTbl == null || tables.contains(tarTbl)) {
				continue;
			}
			tables.add(tarTbl);
			String sql = ddlUtils.getTableDDL(tarTbl);
			txtDDL.append(sql);
			txtDDL.append(NEWLINE);
		}
		txtDDL.append(sbConstrains.toString());
	}

	/**
	 * Show or hide the DDL text.
	 * 
	 * @param flag boolean
	 */
	protected void switchText(boolean flag) {
		txtDDL.setVisible(flag);
		GridData gd = (GridData) txtDDL.getLayoutData();
		gd.exclude = !flag;
		txtSummary.setVisible(!flag);
		gd = (GridData) txtSummary.getLayoutData();
		gd.exclude = flag;
		txtDDL.getParent().layout();
		btnPreviewDDL.setSelection(flag);
	}

	/**
	 * If saving source schema into migration script file
	 * 
	 * @return default is false
	 */
	protected boolean isSaveSchema() {
		return false;
	}
}
