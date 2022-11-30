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
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorSite;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.part.EditorPart;

import com.cubrid.common.ui.swt.Resources;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;
import com.cubrid.cubridmigration.core.engine.report.MigrationReport;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.TextAppender;
import com.cubrid.cubridmigration.ui.history.controller.MigrationReportUIController;
import com.cubrid.cubridmigration.ui.history.dialog.ShowTextDialog;
import com.cubrid.cubridmigration.ui.history.tableviewer.MigrationOverviewTableLabelProvider;
import com.cubrid.cubridmigration.ui.history.tableviewer.ObjectMigrationResultTableLabelProvider;
import com.cubrid.cubridmigration.ui.history.tableviewer.RecordMigrationResultTableLabelProvider;
import com.cubrid.cubridmigration.ui.message.Messages;

public class R2GMigrationReportEditorPart extends
		EditorPart {

	public static final String ID = R2GMigrationReportEditorPart.class.getName();
	public static final String EMPTY_CELL_VALUE = "-";

	private TableViewer tvOverview;
	private TableViewer tvObjDetails;
	private TableViewer tvTableRecords;

	private Text txtNonsupport;
	private Text txtLog;
	private Text txtConfigSummary;

	private TabFolder tfReport;
	private TabFolder tfDetail;

	private Label txtOuputDir;

	private MigrationReportUIController controller = new MigrationReportUIController();

	private TextAppender noSupportedAppender = new TextAppender() {

		public void append(String text) {
			txtNonsupport.append(text);
		}

	};

	private TextAppender logAppender = new TextAppender() {

		public void append(String text) {
			txtLog.append(text);
		}

	};

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
	 * Create the detail page
	 * 
	 * @param tfReport parent
	 */
	private void createDetailPage(TabFolder tfReport) {
		TabItem tiDetail = new TabItem(tfReport, SWT.NONE);
		tiDetail.setText(Messages.lblDetail);
		Composite comDetail = new Composite(tfReport, SWT.NONE);
		tiDetail.setControl(comDetail);
		comDetail.setLayout(new GridLayout());
		comDetail.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		tfDetail = new TabFolder(comDetail, SWT.NONE);
		tfDetail.setLayout(new GridLayout());
		tfDetail.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		TabItem tiDBObjects = new TabItem(tfDetail, SWT.NONE);
		tiDBObjects.setText(Messages.lblDBObjects);

		TabItem tiRecords = new TabItem(tfDetail, SWT.NONE);
		tiRecords.setText(Messages.lblDBRecords);

		createObjectDetailTableViewer(tiDBObjects);
		createRecordDetailTableViewer(tiRecords);
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
	 * Create non-supported objects page
	 * 
	 * @param tfReport parent
	 */
	private void createNonsupportPage(TabFolder tfReport) {
		TabItem tiNonsupport = new TabItem(tfReport, SWT.NONE);
		tiNonsupport.setText(Messages.lblNonsupport);
		txtNonsupport = new Text(tfReport, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
		txtNonsupport.setEditable(false);
		txtNonsupport.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		tiNonsupport.setControl(txtNonsupport);
	}

	/**
	 * @param tiViews TabItem
	 */
	private void createObjectDetailTableViewer(TabItem tiViews) {
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(MigrationReportUIController.TABLE_HEADER_OBJ);
		tvBuilder.setColumnWidths(new int[] {50, 110, 150, 300, 150});
		tvBuilder.setColumnStyles(new int[] {SWT.LEFT, SWT.LEFT, SWT.LEFT, SWT.LEFT, SWT.LEFT});
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new ObjectMigrationResultTableLabelProvider());
		tvObjDetails = tvBuilder.buildTableViewer(tfDetail, SWT.BORDER | SWT.FULL_SELECTION
				| SWT.H_SCROLL | SWT.V_SCROLL);
		tvObjDetails.addDoubleClickListener(new IDoubleClickListener() {

			public void doubleClick(DoubleClickEvent event) {
				TableItem[] selection = tvObjDetails.getTable().getSelection();
				if (selection == null || selection.length == 0) {
					return;
				}
				String message = selection[0].getText(3);
				if (!EMPTY_CELL_VALUE.equals(selection[0].getText(4))) {
					message = message + "\nError:\n" + selection[0].getText(4);
				}
				ShowTextDialog dialog = new ShowTextDialog(
						R2GMigrationReportEditorPart.this.getSite().getShell(), message);
				dialog.open();
			}
		});
		tiViews.setControl(tvObjDetails.getTable());
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
		comTime.setLayout(new GridLayout(8, false));
		comTime.setLayoutData(new GridData(SWT.FILL, SWT.TOP, false, false));

		Label lblStartTime = new Label(comTime, SWT.NONE);
		lblStartTime.setLayoutData(new GridData(SWT.CENTER));
		lblStartTime.setText(Messages.lblStartTime);
		Label txtStartTime = new Label(comTime, SWT.NONE);
		txtStartTime.setLayoutData(new GridData(SWT.CENTER));
		MigrationReport report = getReporter().getReport();
		txtStartTime.setText(CUBRIDTimeUtil.defaultFormatMilin(new Date(report.getTotalStartTime())));
		final Color clrBlue = Display.getDefault().getSystemColor(SWT.COLOR_BLUE);
		txtStartTime.setForeground(clrBlue);

		Label lblEndTime = new Label(comTime, SWT.NONE);
		lblEndTime.setLayoutData(new GridData(SWT.CENTER));
		lblEndTime.setText(Messages.lblEndTime);
		Label txtEndTime = new Label(comTime, SWT.NONE);
		txtEndTime.setLayoutData(new GridData(SWT.CENTER));
		txtEndTime.setText(CUBRIDTimeUtil.defaultFormatMilin(new Date(report.getTotalEndTime())));
		txtEndTime.setForeground(clrBlue);

		Label lblTotalTime = new Label(comTime, SWT.NONE);
		lblTotalTime.setLayoutData(new GridData(SWT.CENTER));
		lblTotalTime.setText(Messages.lblTotalTimeSpend);
		Label txtTotalTime = new Label(comTime, SWT.NONE);
		txtTotalTime.setLayoutData(new GridData(SWT.CENTER));
		txtTotalTime.setText(TimeZoneUtils.format(report.getTotalEndTime()
				- report.getTotalStartTime()));
		txtTotalTime.setForeground(clrBlue);

		if (controller.isFileOutputMigration(report)) {
			Label lblOuputDir = new Label(comTime, SWT.NONE);
			lblOuputDir.setLayoutData(new GridData(SWT.CENTER));
			lblOuputDir.setText(Messages.lblOutputDir);
			txtOuputDir = new Label(comTime, SWT.NONE);
			txtOuputDir.setLayoutData(new GridData(SWT.CENTER));
			final MigrationBriefReport brief = this.getReporter().getReport().getBrief();
			txtOuputDir.setText(brief.getOutputDir());
			txtOuputDir.setForeground(clrBlue);

			txtOuputDir.setCursor(Resources.getInstance().getCursor(SWT.CURSOR_HAND));
			txtOuputDir.addMouseListener(new MouseAdapter() {

				public void mouseDown(MouseEvent event) {
					Program.launch(txtOuputDir.getText());
				}

			});
		}

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(MigrationReportUIController.TABLE_HEADER_OVERVIEW);
		tvBuilder.setColumnWidths(new int[] {150, 150, 150, 150, 150});
		tvBuilder.setColumnStyles(new int[] {SWT.LEFT, SWT.RIGHT, SWT.RIGHT, SWT.RIGHT, SWT.LEFT});
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new MigrationOverviewTableLabelProvider());
		tvOverview = tvBuilder.buildTableViewer(comOverview, SWT.BORDER | SWT.FULL_SELECTION
				| SWT.H_SCROLL | SWT.V_SCROLL);

		tvOverview.addDoubleClickListener(new IDoubleClickListener() {

			public void doubleClick(DoubleClickEvent event) {
				if (event.getSelection().isEmpty()) {
					return;
				}
				R2GMigrationReportEditorPart.this.tfReport.setSelection(1);
				if (tvOverview.getTable().getSelectionIndex() == tvOverview.getTable().getItemCount() - 1) {
					tfDetail.setSelection(1);
				} else {
					tfDetail.setSelection(0);
				}
			}
		});

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

		createToolbar(backGroundCom);

		tfReport = new TabFolder(backGroundCom, SWT.NONE);
		tfReport.setLayout(new GridLayout());
		tfReport.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		createOverviewPage(tfReport);
		createDetailPage(tfReport);
		createNonsupportPage(tfReport);
		createLogPage(tfReport);
		createConfigSummaryPage();
		setContent2Tables();
		tfReport.setSelection(0);
		tfReport.layout();
	}


	/**
	 * @param tiTables TabItem
	 */
	private void createRecordDetailTableViewer(TabItem tiTables) {
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(MigrationReportUIController.TABLE_HEADER_DATA);
		tvBuilder.setColumnWidths(new int[] {150, 100, 140, 130, 140, 130, 90, 130, 130});
		tvBuilder.setColumnStyles(new int[] {SWT.LEFT, SWT.RIGHT, SWT.RIGHT, SWT.LEFT, SWT.RIGHT,
				SWT.LEFT, SWT.RIGHT, SWT.LEFT, SWT.CENTER});
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new RecordMigrationResultTableLabelProvider());
		tvTableRecords = tvBuilder.buildTableViewer(tfDetail, SWT.BORDER | SWT.FULL_SELECTION
				| SWT.H_SCROLL | SWT.V_SCROLL);
		tiTables.setControl(tvTableRecords.getTable());
	}
	
	/**
	 * @param backGroundCom Composite
	 */
	private void createToolbar(Composite backGroundCom) {
		ToolBar tbReport = new ToolBar(backGroundCom, SWT.WRAP | SWT.FLAT | SWT.RIGHT);
		ToolItem btnSaveAll = new ToolItem(tbReport, SWT.PUSH);
		btnSaveAll.setImage(MigrationUIPlugin.getImage("icon/saveall.gif"));
		btnSaveAll.setToolTipText(Messages.btnSaveAllTab);
		btnSaveAll.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				saveAllTableContent();
			}
		});

		new ToolItem(tbReport, SWT.SEPARATOR);

		final ToolItem btnOpenWizard = new ToolItem(tbReport, SWT.PUSH);
		btnOpenWizard.setToolTipText(Messages.btnOpenWizardWithReport);
		btnOpenWizard.setText(Messages.btnStartMigrationByHistory);
		btnOpenWizard.setImage(MigrationUIPlugin.getImage("icon/tb/mnu_script_wizard.png"));
		btnOpenWizard.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				controller.openMigrationWizardByHistory(getReporter());
			}
		});

		if (controller.isFileOutputMigration(getReporter().getReport())) {
			new ToolItem(tbReport, SWT.SEPARATOR);

			final ToolItem btnGotoOutputDir = new ToolItem(tbReport, SWT.PUSH);
			btnGotoOutputDir.setText(Messages.btnGotoOutputDirectory); //Messages.btnStartMigrationByHistory
			btnGotoOutputDir.setImage(MigrationUIPlugin.getImage("icon/file_open.png"));
			btnGotoOutputDir.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(final SelectionEvent event) {
					Program.launch(txtOuputDir.getText());
				}
			});
		}
	}

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
	 * @return MigrationReporter
	 */
	private MigrationReporter getReporter() {
		return (MigrationReporter) getEditorInput();
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
		setTitleImage(MigrationUIPlugin.getImage("icon/exportReport.gif"));
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
	 * Save all report information to a directory.
	 */
	private void saveAllTableContent() {
		DirectoryDialog fd = new DirectoryDialog(tfReport.getShell(), SWT.NONE);
		String file = fd.open();
		if (file == null) {
			return;
		}
		String savedFiles = controller.saveReportToDirectory(getReporter(), file);
		MessageDialog.openInformation(getSite().getShell(), Messages.msgInformation,
				Messages.bind(Messages.msgReportSaved, savedFiles));
	}

	/**
	 * Fill the data to tables.
	 * 
	 */
	private void setContent2Tables() {
		MigrationReporter reporter = getReporter();
		MigrationReport report = reporter.getReport();
		tvOverview.setInput(report.getOverviewResults());
		tvObjDetails.setInput(report.getDbObjectsResult());
		tvTableRecords.setInput(report.getRecMigResults());
		txtConfigSummary.setText(report.getConfigSummary());
		controller.loadNonSupportedObjectText(reporter, noSupportedAppender);
		controller.loadLogText(reporter, logAppender);
	}

	/**
	 * Set focus event
	 */
	public void setFocus() {
		//Do nothing here
	}
}