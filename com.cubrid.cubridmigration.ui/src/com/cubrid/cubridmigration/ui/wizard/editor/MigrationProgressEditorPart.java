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
package com.cubrid.cubridmigration.ui.wizard.editor;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorSite;
import org.eclipse.ui.ISaveablePart2;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.part.EditorPart;

import com.cubrid.common.ui.swt.table.ObjectArrayRowTableLabelProvider;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.IMigrationMonitor;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.event.ExportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.SWTResourceConstents;
import com.cubrid.cubridmigration.ui.history.MigrationReportEditorPart;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.MigrationScriptManager;
import com.cubrid.cubridmigration.ui.wizard.editor.controller.MigrationProgressUIController;

/**
 * MigrationProgressEditorPart responses to monitor the migration progress.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-1 created by Kevin Cao
 */
public class MigrationProgressEditorPart extends
		EditorPart implements
		ISaveablePart2 {
	/**
	 * MigrationMonitor responses to monitor the progress of migration
	 * 
	 * @author Kevin Cao
	 */
	protected class MigrationMonitor implements
			IMigrationMonitor {
		private long startTime = 0;

		private final Timer timer = new Timer();

		/**
		 * Event found
		 * 
		 * @param event MigrationEvent
		 */
		public void addEvent(final MigrationEvent event) {
			Display.getDefault().asyncExec(new Runnable() {

				public void run() {
					updateViewWithMigrationEvent(event);
				}
			});
		}

		/**
		 * When migration finished.
		 */
		public void finished() {
			timer.cancel();
			MigrationScriptManager.getInstance().save();
			Display.getDefault().asyncExec(new Runnable() {

				public void run() {
					updateUIAfterMigrationFinished();
				}

			});
		}

		/**
		 * 
		 */
		protected void initTotalTimeCounter() {
			startTime = System.currentTimeMillis();
			timer.schedule(new TimerTask() {

				public void run() {
					updateTotalTime();
				}
			}, 0, 1000);
		}

		/**
		 * When migration start
		 */
		public void start() {
			final int value = controller.getTotalProgress();
			Display.getDefault().asyncExec(new Runnable() {

				public void run() {
					pbTotal.setMaximum(value);
				}
			});
			initTotalTimeCounter();
		}

		/**
		 * 
		 */
		protected void updateTotalTime() {
			Display.getDefault().asyncExec(new Runnable() {

				public void run() {
					final long ms = System.currentTimeMillis() - startTime;
					final String format = TimeZoneUtils.format(ms);
					lblTotalTime.setText(format);

				}
			});
		}

	}

	//private static final String NA_STRING = "--";

	public static final String ID = MigrationProgressEditorPart.class.getName();

	//private final static Logger LOG = LogUtil.getLogger(MigrationProgressEditorPart.class);

	protected ProgressBar pbTotal;
	protected Button btnStop;
	protected Button btnReport;
	protected StyledText txtProgress;
	protected TableViewer tvProgress;

	protected Label lblTotalTime;
	protected Label lblTotalRecord;

	protected MigrationProgressUIController controller;

	private MigrationConfiguration cf;
	
	/**
	 * Create part of the editor
	 * 
	 * @param sf SashForm
	 */
	protected void createPart1(final SashForm sf) {
		final Composite pnlBackTop = new Composite(sf, SWT.NONE);
		pnlBackTop.setLayout(new GridLayout());
		pnlBackTop.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

		createProgressTableViewer(pnlBackTop);

		Composite textControlPanel = new Composite(pnlBackTop, SWT.NONE);
		GridData gd = new GridData(SWT.FILL, SWT.BOTTOM, false, false);
		gd.heightHint = 36;
		textControlPanel.setLayoutData(gd);
		textControlPanel.setLayout(new GridLayout(6, false));

		Label lblTotalTimeTitle = new Label(textControlPanel, SWT.RIGHT);
		lblTotalTimeTitle.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		lblTotalTimeTitle.setText(Messages.lblTotalTimeElapsed);

		lblTotalTime = new Label(textControlPanel, SWT.LEFT);
		lblTotalTime.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		lblTotalTime.setForeground(Display.getDefault().getSystemColor(SWT.COLOR_BLUE));
		lblTotalTime.setText("00 00:00:00");

		Label lblTotalRecordTitle = new Label(textControlPanel, SWT.RIGHT);
		lblTotalRecordTitle.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		lblTotalRecordTitle.setText(Messages.lblTotalRecordsImported);

		lblTotalRecord = new Label(textControlPanel, SWT.LEFT);
		lblTotalRecord.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		lblTotalRecord.setText("0");
		lblTotalRecord.setForeground(Display.getDefault().getSystemColor(SWT.COLOR_BLUE));

		final Button btnHideOrShowMsg = new Button(textControlPanel, SWT.FLAT);
		btnHideOrShowMsg.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		btnHideOrShowMsg.setImage(MigrationUIPlugin.getImage("icon/hide_msg.gif"));
		btnHideOrShowMsg.setToolTipText(Messages.btnHideMessages);
		btnHideOrShowMsg.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				txtProgress.setVisible(!txtProgress.getVisible());
				if (txtProgress.getVisible()) {
					sf.setMaximizedControl(null);
					btnHideOrShowMsg.setImage(MigrationUIPlugin.getImage("icon/hide_msg.gif"));
					btnHideOrShowMsg.setToolTipText(Messages.btnHideMessages);
				} else {
					sf.setMaximizedControl(pnlBackTop);
					btnHideOrShowMsg.setToolTipText(Messages.btnShowMessages);
					btnHideOrShowMsg.setImage(MigrationUIPlugin.getImage("icon/show_msg.gif"));
				}
				sf.redraw();
			}
		});

		Button btnClearMsg = new Button(textControlPanel, SWT.FLAT);
		btnClearMsg.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		btnClearMsg.setToolTipText(Messages.btnClearMessages);
		btnClearMsg.setImage(MigrationUIPlugin.getImage("icon/clear_msg.gif"));
		btnClearMsg.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				txtProgress.setText("");
			}
		});
	}

	/**
	 * @param pnlBackTop Composite
	 */
	protected void createProgressTableViewer(final Composite pnlBackTop) {
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.colTable, Messages.colRecordCount,
				Messages.colExportedCount, Messages.colImportedCount, Messages.colProgressPercent, Messages.colOwnerName});
		int[] columnWidths = new int[] { 200, 120, 120, 120, 100, 120 };
		DatabaseType dbType = cf.getSourceDBType();
		if (dbType != null && !dbType.isSupportMultiSchema()) {
			columnWidths[5] = 0;
		}
		tvBuilder.setColumnWidths(columnWidths);
		tvBuilder.setColumnStyles(new int[] {SWT.LEFT, SWT.RIGHT, SWT.RIGHT, SWT.RIGHT, SWT.CENTER, SWT.CENTER});
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new ObjectArrayRowTableLabelProvider());
		tvProgress = tvBuilder.buildTableViewer(pnlBackTop, SWT.BORDER | SWT.FULL_SELECTION);
	}

	/**
	 * Create part controls
	 * 
	 * @param parent of the controls
	 * 
	 */
	public void createPartControl(Composite parent) {
		Composite backPanel = new Composite(parent, SWT.NONE);
		backPanel.setLayout(new GridLayout(1, false));
		backPanel.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, true));

		final Composite progressPanel = new Composite(backPanel, SWT.NONE);
		progressPanel.setLayout(new GridLayout(3, false));
		progressPanel.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));

		pbTotal = new ProgressBar(progressPanel, controller.getProgressBarStyle());
		pbTotal.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, true));

		btnStop = new Button(progressPanel, SWT.NONE);
		btnStop.setText(Messages.btnStop);
		btnStop.setImage(MigrationUIPlugin.getImage("icon/stop.gif"));

		btnStop.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				controller.stopMigrationNow();
			}
		});

		btnReport = new Button(progressPanel, SWT.NONE);
		btnReport.setText(Messages.btnViewReport);
		btnReport.setEnabled(false);
		btnReport.setImage(MigrationUIPlugin.getImage("icon/exportReport.gif"));
		btnReport.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent event) {
				openMigrationReport();
			}
		});

		final SashForm sf = new SashForm(backPanel, SWT.BORDER | SWT.VERTICAL);
		sf.setLayout(new GridLayout());
		sf.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		createPart1(sf);

		txtProgress = new StyledText(sf, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
		txtProgress.setEditable(false);
		txtProgress.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		txtProgress.setBackground(txtProgress.getDisplay().getSystemColor(SWT.COLOR_BLACK));
		//initialize the controls in progress dialog.
		controller.updateTableRowCount();
		intiTableView();
		startMigration();
	}

	/**
	 * Do nothing
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
	 * Init the editor part
	 * 
	 * @param site IEditorSite
	 * @param input MigrationCfgEditorInput
	 * @throws PartInitException when error
	 */
	public void init(IEditorSite site, IEditorInput input) throws PartInitException {
		cf = (MigrationConfiguration) input.getAdapter(MigrationConfiguration.class);
		if (cf == null) {
			throw new RuntimeException("Migration configuration can not be null.");
		}
		initUIController(cf);
		setSite(site);
		setInput(input);
		setTitleToolTip(input.getToolTipText());
		setTitleImage(SWTResourceConstents.IMAGE_PROGRESS);
	}

	/**
	 * @param cf MigrationConfiguration
	 */
	protected void initUIController(MigrationConfiguration cf) {
		controller = new MigrationProgressUIController();
		controller.setConfig(cf);
		controller.setReportEditorPartId(MigrationReportEditorPart.ID);
	}

	/**
	 * Initialize content of the table view.
	 * 
	 */
	protected void intiTableView() {
		tvProgress.setInput(controller.getProgressTableInput());
	}

	/**
	 * Default return false
	 * 
	 * @return true if is running
	 */
	public boolean isDirty() {
		return controller.isMigrationRunning();
	}

	/**
	 * Default return false
	 * 
	 * @return false
	 */
	public boolean isSaveAsAllowed() {
		return false;
	}

	/**
	 * @param reporter
	 */
	protected void openMigrationReport() {
		controller.openMigrationReport(this);
	}

	/**
	 * If migration is running, can't be closed.
	 * 
	 * @return cann't be close in migrating
	 */
	public int promptToSaveOnClose() {
		return controller.canBeClosed();
	}

	/**
	 * Open a dialog to query user if showing report now.
	 * 
	 * @return true if open the dialog
	 */
	protected boolean queryIfOpenDisplayReportDialogAfterMigration() {
		return ((MigrationProgressEditorInput) getEditorInput()).isStartedByUser()
				&& MessageDialog.openConfirm(getSite().getShell(), Messages.msgConfirmation,
						Messages.msgOpenReportNow);
	}

	/**
	 * Set focus
	 */
	public void setFocus() {
		txtProgress.setFocus();
	}

	/**
	 * Start migration process
	 */
	protected void startMigration() {
		try {
			MigrationProgressEditorInput migrationProgressEditorInput = (MigrationProgressEditorInput) getEditorInput();
			controller.startMigration(new MigrationMonitor(),
					migrationProgressEditorInput.getStartMode());
		} catch (RuntimeException ex) {
			updateUIWhenMigrationStartFailed();
		}
	}

	/**
	 * Update the export count
	 * 
	 * @param event MigrationEvent
	 */
	protected void updateExportedCountInTableViewer(MigrationEvent event) {
		ExportRecordsEvent ere = (ExportRecordsEvent) event;
		String[] item = controller.updateTableExpData(ere.getSourceTable().getOwner(), ere.getSourceTable().getName(),
				ere.getRecordCount());
		tvProgress.refresh(item);
	}

	/**
	 * Update import count of table
	 * 
	 * @param event MigrationEvent
	 */
	protected void updateImportedCountInTableViewer(MigrationEvent event) {
		ImportRecordsEvent ire = (ImportRecordsEvent) event;
		String[] item = controller.updateTableImpData(ire.getSourceTable().getOwner(), ire.getSourceTable().getName(),
				ire.getRecordCount());
		tvProgress.refresh(item);
	}

	/**
	 * @param event MigrationEvent
	 */
	protected void updateTotalImportedCount(MigrationEvent event) {
		ImportRecordsEvent ire = (ImportRecordsEvent) event;
		long imp = ire.getRecordCount();
		if (imp > 0) {
			lblTotalRecord.setText(Long.toString(Long.valueOf(lblTotalRecord.getText()) + imp));
		}
	}

	/**
	 * updateUIWhileMigrationFinished
	 */
	protected void updateUIAfterMigrationFinished() {
		controller.migrationFinished();
		setTitleImage(SWTResourceConstents.IMAGE_PROGRESS_OK);
		btnStop.setEnabled(false);
		btnReport.setEnabled(true);
		pbTotal.setSelection(pbTotal.getMaximum());
		pbTotal.setEnabled(false);
		if (queryIfOpenDisplayReportDialogAfterMigration()) {
			openMigrationReport();
		}
	}

	/**
	 * Handle errors when errors occurred at the migration starting.
	 * 
	 */
	protected void updateUIWhenMigrationStartFailed() {
		btnReport.setEnabled(false);
		btnStop.setEnabled(false);
		controller.addMessage2Text(txtProgress, new Date(), Messages.errOtherMigrationRunning, true);
	}

	/**
	 * @param event MigrationEvent
	 */
	protected void updateViewWithMigrationEvent(final MigrationEvent event) {
		if (controller.ifShouldUpdateExportStatus(event)) {
			updateExportedCountInTableViewer(event);
		}
		if (controller.ifShouldUpdateImportStatus(event)) {
			updateImportedCountInTableViewer(event);
			updateTotalImportedCount(event);
		}
		boolean errFlag = controller.ifEventHasError(event);
		if (errFlag && pbTotal.getState() != SWT.ERROR) {
			pbTotal.setState(SWT.ERROR);
			//Sleep a moment or the progress bar will not change the status.
			ThreadUtils.threadSleep(300, null);
		}
		controller.addMessage2Text(txtProgress, event.getEventTime(), event.toString(), errFlag);
		int pbvalue = controller.getProgressBarProgressValue(event);
		if (pbvalue > 0) {
			pbTotal.setSelection(pbTotal.getSelection() + pbvalue);
		}
	}
}
