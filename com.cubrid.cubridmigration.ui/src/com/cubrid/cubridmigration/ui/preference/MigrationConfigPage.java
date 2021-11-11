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
package com.cubrid.cubridmigration.ui.preference;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.osgi.service.prefs.BackingStoreException;

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * Migration Configuration Page
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-15 created by Kevin Cao
 */
public class MigrationConfigPage extends
		PreferencePage implements
		IWorkbenchPreferencePage {
	private static final String DEFAULT_FETCHING_COUNT = "defaultFetchingCount";
	private static final int DEFAULT_MAX_RECORD_COUNT_EACH_DATA_FILE = 0;
	private static final int DEFAULT_IMPORT_THREAD_COUNT_OF_EACHE_TARGET_TABLE = 3;
	private static final int DEFAULT_EXPORT_THREAD_COUNT = 4;
	public static final int MIN_CC = 1;
	public static final int MAX_CC = 50000;
	public static final int DEFAULT_PAGE_COUNT = 1000;
	private static final int ONLINE_DEFAULT_CC = 1000;

	private static final Logger LOG = LogUtil.getLogger(MigrationConfigPage.class);
	private static final IEclipsePreferences NODE = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);

	/**
	 * Retrieves the default Online commit count
	 * 
	 * @return count
	 */
	public static int getCommitCount() {
		return NODE.getInt("onlineDefaultCommitCount", ONLINE_DEFAULT_CC);
	}

	/**
	 * Retrieves the default Online Export Thread count
	 * 
	 * @return count
	 */
	public static int getDefaultExportThreadCount() {
		return NODE.getInt("edtOnlineExportThread", DEFAULT_EXPORT_THREAD_COUNT);
	}

	/**
	 * Retrieves the default Import Thread count of each table
	 * 
	 * @return 4 is default value
	 */
	public static int getDefaultImpportThreadCountEachTable() {
		return NODE.getInt("importThreadCountSpinner",
				DEFAULT_IMPORT_THREAD_COUNT_OF_EACHE_TARGET_TABLE);
	}

	/**
	 * Retrieves the default template file path
	 * 
	 * @return templateFilePath
	 */
	public static int getFileMaxSize() {
		return NODE.getInt("fileMaxSize", DEFAULT_MAX_RECORD_COUNT_EACH_DATA_FILE);
	}

	/**
	 * getDefaultPageFetchingCount
	 * 
	 * @return INTEGER
	 */
	public static int getPageFetchingCount() {
		return NODE.getInt(DEFAULT_FETCHING_COUNT, DEFAULT_PAGE_COUNT);
	}

	/**
	 * Retrieves the default template file path
	 * 
	 * @return templateFilePath
	 */
	public static String getTemplateFilePath() {
		return NODE.get("templateFilePath", PathUtils.getDefaultBaseTempDir());
	}

	private Spinner exportThreadCountSpinner;

	private Spinner onlineDefaultCommitCount;

	private Spinner txtMaxFileSize;

	private Text edtTempFilePath;

	private Spinner txtFetchCount;

	private Spinner importThreadCountSpinner;

	public MigrationConfigPage() {
		super("Migration Configurations", null);
	}

	/**
	 * createContents
	 * 
	 * @param parent Composite
	 * @return container Control
	 */
	protected Control createContents(Composite parent) {
		Composite container = new Composite(parent, SWT.NONE);
		container.setLayout(new GridLayout(1, false));
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

		Group performGrp = new Group(container, SWT.NONE);
		performGrp.setLayout(new GridLayout(1, false));
		performGrp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		performGrp.setText(Messages.grpPerformanceSettings);

		Group tcGrp = new Group(performGrp, SWT.NONE);
		tcGrp.setLayout(new GridLayout(2, false));
		tcGrp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

		Label lblOnlineExportThread = new Label(tcGrp, SWT.RIGHT);
		lblOnlineExportThread.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblOnlineExportThread.setText(Messages.defaultExportThreadCountLabel);
		exportThreadCountSpinner = new Spinner(tcGrp, SWT.BORDER);
		exportThreadCountSpinner.setValues(getDefaultExportThreadCount(), 1, 50, 0, 1, 1);
		exportThreadCountSpinner.setLayoutData(new GridData(50, 16));

		Label lblOnlineImportThread = new Label(tcGrp, SWT.RIGHT);
		lblOnlineImportThread.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblOnlineImportThread.setText(Messages.defaultImportThreadCountEachTableLabel);
		importThreadCountSpinner = new Spinner(tcGrp, SWT.BORDER);
		importThreadCountSpinner.setValues(getDefaultImpportThreadCountEachTable(), 1, 10, 0, 1, 1);
		importThreadCountSpinner.setLayoutData(new GridData(50, 16));

		Group fcGrp = new Group(performGrp, SWT.NONE);
		fcGrp.setLayout(new GridLayout(2, false));
		fcGrp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

		Label lblFetchCount = new Label(fcGrp, SWT.RIGHT);
		lblFetchCount.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblFetchCount.setText(Messages.txtPageFetchCount);
		txtFetchCount = new Spinner(fcGrp, SWT.BORDER);
		txtFetchCount.setValues(getPageFetchingCount(), MIN_CC, Integer.MAX_VALUE, 0, 1, 1);
		txtFetchCount.setLayoutData(new GridData(50, 16));

		Group cmGrp = new Group(performGrp, SWT.NONE);
		cmGrp.setLayout(new GridLayout(2, false));
		cmGrp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

		Label lblOnlineCM = new Label(cmGrp, SWT.RIGHT);
		lblOnlineCM.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblOnlineCM.setText(Messages.onlineCUBRIDCommitCount);
		onlineDefaultCommitCount = new Spinner(cmGrp, SWT.BORDER);
		onlineDefaultCommitCount.setValues(getCommitCount(), MIN_CC, MAX_CC, 0, 1, 1);
		onlineDefaultCommitCount.setLayoutData(new GridData(50, 16));

		Group grpFileMaxSize = new Group(performGrp, SWT.NONE);
		grpFileMaxSize.setLayout(new GridLayout(2, false));
		grpFileMaxSize.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

		Label lblFileMaxSize = new Label(grpFileMaxSize, SWT.RIGHT);
		lblFileMaxSize.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblFileMaxSize.setText(Messages.lblFileMaxSize);
		txtMaxFileSize = new Spinner(grpFileMaxSize, SWT.BORDER);
		txtMaxFileSize.setValues(getFileMaxSize(), 0, Integer.MAX_VALUE, 0, 1, 1000);
		txtMaxFileSize.setLayoutData(new GridData(50, 16));
		txtMaxFileSize.setToolTipText(Messages.ttFileMaxSize);

		Group grp3 = new Group(container, SWT.NONE);
		grp3.setLayout(new GridLayout(3, false));
		grp3.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		grp3.setText(Messages.otherSettings);

		Label lblTempFile = new Label(grp3, SWT.LEFT);
		lblTempFile.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, false, false));
		lblTempFile.setText(Messages.tempPath);
		edtTempFilePath = new Text(grp3, SWT.BORDER | SWT.READ_ONLY);
		edtTempFilePath.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		edtTempFilePath.setText(getTemplateFilePath());
		Button btnBrowse = new Button(grp3, SWT.NONE);
		btnBrowse.setText(Messages.btnBrowse);
		btnBrowse.setLayoutData(new GridData(70, 25));
		btnBrowse.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				DirectoryDialog dd = new DirectoryDialog(getShell());
				dd.setFilterPath(edtTempFilePath.getText());
				String dir = dd.open();
				if (dir == null) {
					return;
				}
				edtTempFilePath.setText(dir);
			}
		});
		return container;
	}

	/**
	 * Create controls
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		super.createControl(parent);
		Button btnDB = this.getDefaultsButton();
		btnDB.setText(Messages.btnDefault);
		Button btnAB = this.getApplyButton();
		btnAB.setText(Messages.btnApply);
	}

	/**
	 * init
	 * 
	 * @param workbench IWorkbench
	 */
	public void init(IWorkbench workbench) {
		//empty
	}

	/**
	 * performDefaults
	 */
	protected void performDefaults() {
		exportThreadCountSpinner.setSelection(DEFAULT_EXPORT_THREAD_COUNT);
		importThreadCountSpinner.setSelection(DEFAULT_IMPORT_THREAD_COUNT_OF_EACHE_TARGET_TABLE);
		txtFetchCount.setSelection(DEFAULT_PAGE_COUNT);
		onlineDefaultCommitCount.setSelection(ONLINE_DEFAULT_CC);
		edtTempFilePath.setText(PathUtils.getDefaultBaseTempDir());
		txtMaxFileSize.setSelection(DEFAULT_MAX_RECORD_COUNT_EACH_DATA_FILE);
	}

	/**
	 * performOk
	 * 
	 * @return boolean
	 */
	public boolean performOk() {
		try {
			NODE.put("edtOnlineExportThread",
					Integer.toString(exportThreadCountSpinner.getSelection()));
			NODE.put("importThreadCountSpinner",
					Integer.toString(importThreadCountSpinner.getSelection()));

			NODE.put(DEFAULT_FETCHING_COUNT, Integer.toString(txtFetchCount.getSelection()));
			NODE.put("onlineDefaultCommitCount",
					Integer.toString(onlineDefaultCommitCount.getSelection()));
			NODE.put("templateFilePath", edtTempFilePath.getText());
			NODE.put("fileMaxSize", Integer.toString(txtMaxFileSize.getSelection()));
			NODE.flush();
			PathUtils.setBaseTempDir(edtTempFilePath.getText());
		} catch (BackingStoreException e) {
			LOG.error(e);
		}
		return true;
	}
}
