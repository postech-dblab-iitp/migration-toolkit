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
package com.cubrid.cubridmigration.ui.history.controller;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IEditorInput;

import com.cubrid.common.ui.swt.EditorPartProvider;
import com.cubrid.common.ui.swt.IEditorInputInitializer;
import com.cubrid.common.ui.swt.ProgressMonitorDialogRunner;
import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;
import com.cubrid.cubridmigration.core.engine.report.MigrationReport;
import com.cubrid.cubridmigration.ui.history.MigrationReporter;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizardFactory;

/**
 * MigrationHistoryUIController responses to store all logic of the migration
 * history management.
 * 
 * @see ProgressMonitorDialogRunner
 * @see EditorPartProvider
 * @author Kevin Cao
 */
public class MigrationHistoryUIController {

	private final static Logger LOGGER = LogUtil.getLogger(MigrationHistoryUIController.class);

	private ProgressMonitorDialogRunner progressMonitorDialogRunner;

	private EditorPartProvider editorPartProvider;

	/**
	 * Retrieves the selected reporter with full information.
	 * 
	 * @param itemSelection from view.
	 * @return MigrationReporter may be null.
	 * 
	 */
	private MigrationReporter getSelectedReporter(IStructuredSelection itemSelection) {
		if (itemSelection.isEmpty()) {
			return null;
		}
		MigrationBriefReport brief = (MigrationBriefReport) itemSelection.getFirstElement();
		if (brief == null) {
			return null;
		}
		final MigrationReporter reporter = getMigrationReporterByBrief(brief);
		//Load with progress
		loadReporterInProgress(reporter);
		final MigrationReport rpt = reporter.getReport();
		if (rpt == null) {
			throw new RuntimeException("Error migration history file.");
		}
		if (rpt.getBrief() == null) {
			rpt.setBrief(brief);
		}
		return reporter;
	}

	/**
	 * 
	 * Reopen migration wizard with the migration history
	 * 
	 * @param itemSelection from view to be opened
	 * 
	 */
	public void reopenWizard(IStructuredSelection itemSelection) {
		Shell shell = Display.getDefault().getActiveShell();
		try {
			MigrationWizardFactory.openWizardWithReport(getSelectedReporter(itemSelection));
		} catch (Exception e) {
			MessageDialog.openError(shell, Messages.msgError,
					Messages.msgErrCanntOpenWizardWithReport + e.getMessage());
			LOGGER.error("", e);
		}
	}

	/**
	 * Load report in progress dialog
	 * 
	 * @param reporter MigrationReporter
	 * 
	 */
	public void loadReporterInProgress(final MigrationReporter reporter) {
		progressMonitorDialogRunner.run(true, false, new IRunnableWithProgress() {
			public void run(IProgressMonitor monitor) throws InvocationTargetException,
					InterruptedException {
				monitor.beginTask("", 100);
				monitor.worked(50);
				reporter.loadMigrationHistory();
				monitor.worked(90);
				monitor.done();
			}
		});
	}

	/**
	 * Show the migration report of migration history.
	 * 
	 * @param selection the selected migration history.
	 */
	public void showMigrationReport(IStructuredSelection selection) {
		if (selection.isEmpty()) {
			return;
		}
		try {
			final MigrationBriefReport mbr = (MigrationBriefReport) selection.getFirstElement();
			final MigrationReporter reporter = getMigrationReporterByBrief(mbr);
			String editorId = getReportEditorPartID(mbr);
			IEditorInputInitializer editorInputInitializer = new IEditorInputInitializer() {

				public void initializeInput(IEditorInput input) {
					//Load with progress
					try {
						loadReporterInProgress(reporter);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					final MigrationReport rpt = reporter.getReport();
					if (rpt == null) {
						throw new RuntimeException("Invalid migration report file.");
					}
					if (rpt.getBrief() == null) {
						rpt.setBrief(mbr);
					}
				}

			};
			editorPartProvider.openOrActiveEditorPart(reporter, editorId, editorInputInitializer);
		} catch (Exception e1) {
			throw new RuntimeException(e1);
		}
	}

	/**
	 * getReportEditorPartID by source type
	 * 
	 * @param mbr MigrationBriefReport
	 * @return String
	 */
	protected String getReportEditorPartID(final MigrationBriefReport mbr) {
		return MigrationWizardFactory.getReportEditorPartID(mbr.getSourceType());
	}

	/**
	 * 
	 * getMigrationReporterByBrief
	 * 
	 * @param mbr MigrationBriefReport
	 * @return MigrationReporter
	 */
	protected MigrationReporter getMigrationReporterByBrief(final MigrationBriefReport mbr) {
		return new MigrationReporter(new File(mbr.getHistoryFile()));
	}

	/**
	 * Import external history file into migration history management
	 */
	public void importHistory() {
		FileDialog dialog = new FileDialog(getShell(), SWT.OPEN);
		dialog.setFilterExtensions(new String[] {"*.mh"});
		String historyFileName = dialog.open();
		if (historyFileName == null) {
			return;
		}
		importHistory(historyFileName);
	}

	/**
	 * @param historyFileName to be imported
	 */
	protected void importHistory(String historyFileName) {
		try {
			String userDir = PathUtils.getReportDir();
			final File sf = new File(historyFileName);
			String targetFile = userDir + File.separator + sf.getName();
			File tf = new File(targetFile);
			if (tf.getCanonicalPath().equals(sf.getCanonicalPath())) {
				return;
			}
			if (tf.exists()) {
				targetFile = userDir + File.separator + System.currentTimeMillis() + ".mh";
				tf = new File(targetFile);
				ThreadUtils.threadSleep(2, null);
			}
			PathUtils.createFile(tf);
			PathUtils.deleteFile(tf);
			CUBRIDIOUtils.copyFile(sf, tf);
		} catch (Exception ex) {
			LOGGER.error("", ex);
		}
	}

	/**
	 * 
	 * Refresh the history table view.
	 * 
	 * @return local history list
	 */
	public List<MigrationBriefReport> getAllLocalHistory() {
		List<MigrationBriefReport> briefs = new ArrayList<MigrationBriefReport>();
		File reportDir = new File(PathUtils.getReportDir());
		if (!reportDir.exists()) {
			return briefs;
		}
		File[] hisFiles = reportDir.listFiles(new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return name.toLowerCase(Locale.US).endsWith(".mh");
			}
		});
		if (hisFiles == null) {
			return briefs;
		}

		for (File hf : hisFiles) {
			MigrationBriefReport mbr = new MigrationBriefReport();
			try {
				mbr.loadFromHistoryFile(hf.getName());
				briefs.add(mbr);
			} catch (Exception ex) {
				LOGGER.error("", ex);
			}
		}
		return briefs;
	}

	/**
	 * Delete the selected history.
	 * 
	 * @param itemSelection from view
	 * 
	 */
	public void deleteHistory(IStructuredSelection itemSelection) {
		if (!MessageDialog.openConfirm(getShell(), Messages.dlgDeleteHistory,
				Messages.dlgDeleteHistoryComfirm)) {
			return;
		}
		deleteSelectedHistory(itemSelection);
	}

	/**
	 * deleteSelectedHistory
	 * 
	 * @param itemSelection from view
	 */
	protected void deleteSelectedHistory(IStructuredSelection itemSelection) {
		@SuppressWarnings("unchecked")
		Iterator<MigrationBriefReport> iterator = itemSelection.iterator();
		while (iterator.hasNext()) {
			try {
				MigrationBriefReport file = iterator.next();
				PathUtils.deleteFile(new File(PathUtils.getReportDir() + file.getHistoryFile()));
			} catch (Exception ex) {
				LOGGER.error("", ex);
			}
		}
	}

	/**
	 * Get the active shell
	 * 
	 * @return Shell
	 */
	private Shell getShell() {
		return Display.getDefault().getActiveShell();
	}

	public void setProgressMonitorDialogRunner(
			ProgressMonitorDialogRunner progressMonitorDialogRunner) {
		this.progressMonitorDialogRunner = progressMonitorDialogRunner;
	}

	public void setEditorPartProvider(EditorPartProvider editorPartProvider) {
		this.editorPartProvider = editorPartProvider;
	}

}
