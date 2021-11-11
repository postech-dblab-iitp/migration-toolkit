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
package com.cubrid.cubridmigration.ui.history;

import java.util.Date;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorSite;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.part.EditorPart;

import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.engine.report.MigrationReport;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.SWTResourceConstents;
import com.cubrid.cubridmigration.ui.common.TextAppender;
import com.cubrid.cubridmigration.ui.history.controller.FileSourceMigrationReportUIController;
import com.cubrid.cubridmigration.ui.history.tableviewer.FileSourceMigrationResultOverviewLabelProvider;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * SQLImportReportEditorPart responses to monitor the migration progress.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-11-30 created by Kevin Cao
 */
public class CSVImportReportEditorPart extends
		EditorPart {
	public static final String ID = CSVImportReportEditorPart.class.getName();

	private static final String[] TABLE_HEADER_CSV_OVERVIEW = new String[] {Messages.colCSVFile,
			Messages.colExpCount, Messages.colImpCount, Messages.colProgress};

	protected static final int[] COLUMN_WIDTHS_OVERVIEW = new int[] {300, 150, 150, 200};

	protected static final int[] COLUMN_STYLES_OVERVIEW = new int[] {SWT.LEFT, SWT.RIGHT,
			SWT.RIGHT, SWT.LEFT};

	protected TableViewer tvOverview;

	protected Text txtLog;

	protected Text txtConfigSummary;

	protected TabFolder tfReport;

	protected final FileSourceMigrationReportUIController controller = new FileSourceMigrationReportUIController();

	/**
	 * Do no thing
	 * 
	 * @param monitor IProgressMonitor
	 */
	public void doSave(IProgressMonitor monitor) {
		//Do no thing

	}

	/**
	 * Do nothing
	 */
	public void doSaveAs() {
		//Do no thing

	}

	/**
	 * Retrieves the migration report object
	 * 
	 * @return MigrationReport
	 */
	private MigrationReport getReport() {
		return (MigrationReport) this.getEditorInput().getAdapter(MigrationReport.class);
	}

	/**
	 * Initializes this editor with the given editor site and input.
	 * 
	 * @param site the editor site
	 * @param input the editor input
	 * @exception PartInitException if this editor was not initialized
	 *            successfully
	 */
	public void init(IEditorSite site, IEditorInput input) throws PartInitException {
		setSite(site);
		setInput(input);
		setTitleToolTip(input.getToolTipText());
		setTitleImage(SWTResourceConstents.IMAGE_EXPORT_REPORT);
		controller.setOverviewTableViewerHeader(TABLE_HEADER_CSV_OVERVIEW);
	}

	/**
	 * No changes
	 * 
	 * @return false
	 */
	public boolean isDirty() {
		return false;
	}

	/**
	 * Don't save
	 * 
	 * @return false
	 */
	public boolean isSaveAsAllowed() {
		return false;
	}

	/**
	 * Creates the SWT controls for this workbench part.
	 * 
	 * @param parent the parent control
	 */
	public void createPartControl(Composite parent) {
		Composite backGroundCom = new Composite(parent, SWT.NONE);
		backGroundCom.setLayout(new GridLayout());
		backGroundCom.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		ToolBar tbReport = new ToolBar(backGroundCom, SWT.WRAP | SWT.FLAT | SWT.RIGHT);
		ToolItem btnSaveAll = new ToolItem(tbReport, SWT.PUSH);
		btnSaveAll.setImage(MigrationUIPlugin.getImage("icon/saveall.gif"));
		btnSaveAll.setToolTipText(Messages.btnSaveAllTab);
		btnSaveAll.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				saveReportToDir();
			}
		});

		new ToolItem(tbReport, SWT.SEPARATOR);

		final ToolItem btnOpenWizard = new ToolItem(tbReport, SWT.PUSH);
		btnOpenWizard.setToolTipText(Messages.btnOpenWizardWithReport);
		btnOpenWizard.setText(Messages.btnStartMigrationByHistory);
		btnOpenWizard.setImage(MigrationUIPlugin.getImage("icon/tb/mnu_script_wizard.png"));
		btnOpenWizard.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				controller.openWizardByReport(getMigrationReporter());
			}
		});

		tfReport = new TabFolder(backGroundCom, SWT.NONE);
		tfReport.setLayout(new GridLayout());
		tfReport.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		createOverviewPage(tfReport);
		createLogPage(tfReport);
		createConfigSummaryPage();
		setContent2Tables();
	}

	/**
	 * Create configuration summary page
	 * 
	 */
	private void createConfigSummaryPage() {
		TabItem tiCs = new TabItem(tfReport, SWT.NONE);
		tiCs.setText(Messages.lblConfigSummary);
		txtConfigSummary = new Text(tfReport, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
		txtConfigSummary.setEditable(false);
		txtConfigSummary.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		tiCs.setControl(txtConfigSummary);
	}

	/**
	 * Create Log Page
	 * 
	 * @param tfReport parent
	 */
	private void createLogPage(TabFolder tfReport) {
		TabItem tiLog = new TabItem(tfReport, SWT.NONE);
		tiLog.setText(Messages.lblLog);
		txtLog = new Text(tfReport, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
		txtLog.setEditable(false);
		txtLog.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		tiLog.setControl(txtLog);
	}

	/**
	 * Create overview page
	 * 
	 * @param tfReport parent
	 */
	private void createOverviewPage(TabFolder tfReport) {
		TabItem tiOverview = new TabItem(tfReport, SWT.NONE);
		tiOverview.setText(Messages.lblOverview);

		Composite comOverview = new Composite(tfReport, SWT.NONE);
		tiOverview.setControl(comOverview);
		comOverview.setLayout(new GridLayout());
		comOverview.setLayoutData(new GridData(SWT.FILL, SWT.FILL, false, false));

		Composite comTime = new Composite(comOverview, SWT.NONE);
		comTime.setLayout(new GridLayout(6, false));
		comTime.setLayoutData(new GridData(SWT.FILL, SWT.TOP, false, false));

		Label lblStartTime = new Label(comTime, SWT.NONE);
		lblStartTime.setLayoutData(new GridData(SWT.CENTER));
		lblStartTime.setText(Messages.lblStartTime);
		Label txtStartTime = new Label(comTime, SWT.NONE);
		txtStartTime.setLayoutData(new GridData(SWT.CENTER));
		txtStartTime.setText(CUBRIDTimeUtil.defaultFormatMilin(new Date(
				getReport().getTotalStartTime())));
		txtStartTime.setForeground(Display.getDefault().getSystemColor(SWT.COLOR_BLUE));

		Label lblEndTime = new Label(comTime, SWT.NONE);
		lblEndTime.setLayoutData(new GridData(SWT.CENTER));
		lblEndTime.setText(Messages.lblEndTime);
		Label txtEndTime = new Label(comTime, SWT.NONE);
		txtEndTime.setLayoutData(new GridData(SWT.CENTER));
		txtEndTime.setText(CUBRIDTimeUtil.defaultFormatMilin(new Date(getReport().getTotalEndTime())));
		txtEndTime.setForeground(Display.getDefault().getSystemColor(SWT.COLOR_BLUE));

		Label lblTotalTime = new Label(comTime, SWT.NONE);
		lblTotalTime.setLayoutData(new GridData(SWT.CENTER));
		lblTotalTime.setText(Messages.lblTotalTimeSpend);
		Label txtTotalTime = new Label(comTime, SWT.NONE);
		txtTotalTime.setLayoutData(new GridData(SWT.CENTER));
		txtTotalTime.setText(TimeZoneUtils.format(getReport().getTotalEndTime()
				- getReport().getTotalStartTime()));
		txtTotalTime.setForeground(Display.getDefault().getSystemColor(SWT.COLOR_BLUE));
		createOverviewTableViewer(comOverview);
	}

	/**
	 * @param comOverview Composite
	 */
	protected void createOverviewTableViewer(Composite comOverview) {
		//Create overview table viewer 
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(TABLE_HEADER_CSV_OVERVIEW);
		tvBuilder.setColumnStyles(COLUMN_STYLES_OVERVIEW);
		tvBuilder.setColumnWidths(COLUMN_WIDTHS_OVERVIEW);
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new FileSourceMigrationResultOverviewLabelProvider());
		tvOverview = tvBuilder.buildTableViewer(comOverview, SWT.BORDER | SWT.FULL_SELECTION);
	}

	/**
	 * Fill the data to tables.
	 * 
	 */
	private void setContent2Tables() {
		tvOverview.setInput(getReport().getDataFileResults());
		txtConfigSummary.setText(getReport().getConfigSummary());
		controller.loadLogText(getMigrationReporter(), new TextAppender() {

			public void append(String text) {
				txtLog.append(text);
			}
		});
	}

	/**
	 * Set focus event
	 */
	public void setFocus() {
		//Do nothing here
	}

	/**
	 * saveReportToDir
	 */
	protected void saveReportToDir() {
		DirectoryDialog fd = new DirectoryDialog(tfReport.getShell(), SWT.NONE);
		String file = fd.open();
		if (file == null) {
			return;
		}
		String bindings = controller.saveReportToDirectory(getMigrationReporter(), file);
		MessageDialog.openInformation(getSite().getShell(), Messages.msgInformation,
				Messages.bind(Messages.msgReportSaved, bindings));
	}

	/**
	 * @return MigrationReporter
	 */
	protected MigrationReporter getMigrationReporter() {
		return (MigrationReporter) getEditorInput();
	}
}
