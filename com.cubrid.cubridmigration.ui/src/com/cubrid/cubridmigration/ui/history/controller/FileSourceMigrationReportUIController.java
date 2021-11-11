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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.report.DataFileImportResult;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.ui.common.TextAppender;
import com.cubrid.cubridmigration.ui.history.MigrationReporter;
import com.cubrid.cubridmigration.ui.history.tableviewer.FileSourceMigrationResultOverviewLabelProvider;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizardFactory;

/**
 * @author Kevin Cao
 * 
 */
public class FileSourceMigrationReportUIController {

	private static final Logger LOG = LogUtil.getLogger(FileSourceMigrationReportUIController.class);

	private String[] overviewTableViewerHeader;

	/**
	 * 
	 * @param reporter MigrationReporter
	 * @param textAppender TextAppender
	 */
	public void loadLogText(MigrationReporter reporter, TextAppender textAppender) {
		MigrationReportUIController controller = new MigrationReportUIController();
		controller.loadLogText(reporter, textAppender);
	}

	/**
	 * 
	 * @param reporter MigrationReporter
	 * 
	 */
	public void openWizardByReport(MigrationReporter reporter) {
		Shell shell = Display.getDefault().getActiveShell();
		try {
			MigrationWizardFactory.openWizardWithReport(reporter);
		} catch (Exception e) {
			MessageDialog.openError(shell, Messages.msgError,
					Messages.msgErrCanntOpenWizardWithReport + e.getMessage());
			LOG.error("", e);
		}
	}

	/**
	 * @param reporter MigrationReporter
	 * @param outputDir to save report
	 * 
	 * @return the files saved successfully
	 */
	public String saveReportToDirectory(MigrationReporter reporter, String outputDir) {
		String filePartName = outputDir
				+ File.separator
				+ CUBRIDTimeUtil.getDateFormat("yyyy_MM_dd_HH_mm_ss_SSS", Locale.US,
						TimeZone.getDefault()).format(
						new Date(reporter.getReport().getTotalStartTime()));
		String xlsFile = filePartName + ".xls";
		String logFile = filePartName + ".log";

		StringBuffer sb = new StringBuffer();
		sb.append(saveReportToXls(reporter, xlsFile));
		sb.append(MigrationReportUIController.extractLogs(reporter, logFile));
		return sb.toString();
	}

	/**
	 * @param reporter MigrationReporter
	 * @param xlsFile String
	 * @return xlsFile if successfully
	 */
	private String saveReportToXls(MigrationReporter reporter, String xlsFile) {
		List<List<String[]>> data = new ArrayList<List<String[]>>();
		List<DataFileImportResult> dataFileResults = reporter.getReport().getDataFileResults();
		List<String[]> dataFileResultArrays = new ArrayList<String[]>();
		data.add(dataFileResultArrays);
		FileSourceMigrationResultOverviewLabelProvider provider = new FileSourceMigrationResultOverviewLabelProvider();
		for (DataFileImportResult result : dataFileResults) {
			String[] value = new String[overviewTableViewerHeader.length];
			for (int i = 0; i < value.length; i++) {
				value[i] = provider.getColumnText(result, i);
			}
			dataFileResultArrays.add(value);
		}
		List<String[]> columns = new ArrayList<String[]>();
		columns.add(overviewTableViewerHeader);
		CUBRIDIOUtils.saveTable2Excel(new String[] {Messages.lblOverview}, columns, data, xlsFile);
		return xlsFile;
	}

	public void setOverviewTableViewerHeader(String[] header) {
		overviewTableViewerHeader = header;
	}
}
