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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.report.DBObjMigrationResult;
import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;
import com.cubrid.cubridmigration.core.engine.report.MigrationOverviewResult;
import com.cubrid.cubridmigration.core.engine.report.MigrationReport;
import com.cubrid.cubridmigration.core.engine.report.MigrationReportFileUtils;
import com.cubrid.cubridmigration.core.engine.report.RecordMigrationResult;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.ui.common.TextAppender;
import com.cubrid.cubridmigration.ui.history.MigrationReporter;
import com.cubrid.cubridmigration.ui.history.tableviewer.MigrationOverviewTableLabelProvider;
import com.cubrid.cubridmigration.ui.history.tableviewer.ObjectMigrationResultTableLabelProvider;
import com.cubrid.cubridmigration.ui.history.tableviewer.RecordMigrationResultTableLabelProvider;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizardFactory;

/**
 * 
 * MigrationReportUIController.
 * 
 * @author Kevin Cao
 * 
 */
public class MigrationReportUIController {
	protected static final String UTF_8 = "utf-8";
	protected static final String EMPTY_CELL_VALUE = "-";
	private static final Logger LOG = LogUtil.getLogger(MigrationReportUIController.class);

	protected static final String[] TAB_NAME = new String[] {Messages.lblOverview,
			Messages.lblDBObjects, Messages.lblDBRecords};

	public static final String[] TABLE_HEADER_OVERVIEW = new String[] {Messages.colObjects,
			Messages.colExpCount, Messages.colImpCount, Messages.colFailed, Messages.colProgress};

	public static final String[] TABLE_HEADER_OBJ = new String[] {Messages.colStatus,
			Messages.colType, Messages.colName, Messages.colDDL, Messages.colError};

	public static final String[] TABLE_HEADER_DATA = new String[] {Messages.colTableName,
			Messages.colTotal, Messages.colExpCount, Messages.colExpTime, Messages.colImpCount,
			Messages.colImpTime, Messages.colCompleted, Messages.colTotalElapsed, Messages.colOwnerName};

	/**
	 * @param reporter MigrationReporter
	 * @param noSupportedFile String
	 * @return the output file name will be returned if successfully
	 */
	static String extracNonSupportedReport(MigrationReporter reporter, String noSupportedFile) {
		try {
			String srcNonSptFile = MigrationReportFileUtils.extractNonSupport(reporter.getFileName());
			if (srcNonSptFile != null) {
				CUBRIDIOUtils.copyFile(new File(srcNonSptFile), new File(noSupportedFile));
				return ("\r\n") + (noSupportedFile);
			}
		} catch (IOException e) {
			LOG.error(e);
		}
		return "";
	}

	/**
	 * @param reporter MigrationReporter
	 * @param logFile String
	 * @return the output file name will be returned if successfully
	 */
	static String extractLogs(MigrationReporter reporter, String logFile) {
		try {
			String srcReportLogFile = MigrationReportFileUtils.extractLog(reporter.getFileName());
			if (srcReportLogFile != null) {
				CUBRIDIOUtils.copyFile(new File(srcReportLogFile), new File(logFile));
				return ("\r\n") + (logFile);
			}
		} catch (IOException e) {
			LOG.error(e);
		}
		return "";
	}

	/**
	 * @param report MigrationReport
	 * @param data List<List<String[]>>
	 */
	private void getObjectResults(MigrationReport report, List<List<String[]>> data) {
		List<DBObjMigrationResult> dbObjectsResult = report.getDbObjectsResult();
		List<String[]> dbObjectsArrays = new ArrayList<String[]>();
		data.add(dbObjectsArrays);
		ObjectMigrationResultTableLabelProvider objProvider = new ObjectMigrationResultTableLabelProvider();
		for (DBObjMigrationResult result : dbObjectsResult) {
			String[] value = new String[5];
			value[0] = result.isSucceed() ? "OK" : "Failed";
			for (int i = 1; i < value.length; i++) {
				value[i] = objProvider.getColumnText(result, i);
			}
			dbObjectsArrays.add(value);
		}
	}

	/**
	 * @param report MigrationReport
	 * @param data List<List<String[]>>
	 */
	private void getOverviewResults(MigrationReport report, List<List<String[]>> data) {
		List<MigrationOverviewResult> overviewResults = report.getOverviewResults();
		List<String[]> overviewArrays = new ArrayList<String[]>();
		data.add(overviewArrays);
		MigrationOverviewTableLabelProvider overviewProvider = new MigrationOverviewTableLabelProvider();
		for (MigrationOverviewResult result : overviewResults) {
			String[] value = new String[5];
			for (int i = 0; i < value.length; i++) {
				value[i] = overviewProvider.getColumnText(result, i);
			}
			overviewArrays.add(value);
		}
	}

	/**
	 * @param report MigrationReport
	 * @param data List<List<String[]>>
	 */
	private void getRecordResults(MigrationReport report, List<List<String[]>> data) {
		List<RecordMigrationResult> recMigResults = report.getRecMigResults();
		List<String[]> recMigArrays = new ArrayList<String[]>();
		data.add(recMigArrays);
		RecordMigrationResultTableLabelProvider provider = new RecordMigrationResultTableLabelProvider();
		for (RecordMigrationResult result : recMigResults) {
			String[] value = new String[9];
			for (int i = 0; i < value.length; i++) {
				value[i] = provider.getColumnText(result, i);
			}
			recMigArrays.add(value);
		}
	}

	/**
	 * If the migration is output to local files
	 * 
	 * @param report MigrationReport
	 * @return true if output to files.
	 */
	public boolean isFileOutputMigration(MigrationReport report) {
		final MigrationBriefReport brief = report.getBrief();
		return (brief != null && StringUtils.isNotBlank(brief.getOutputDir()));
	}

	/**
	 * loadLogText
	 * 
	 * @param reporter MigrationReporter
	 * @param textAppender TextAppender
	 */
	public void loadLogText(MigrationReporter reporter, TextAppender textAppender) {
		try {
			//LOGs
			String logFile = MigrationReportFileUtils.extractLog(reporter.getFileName());
			loadTextFromFile(logFile, textAppender, UTF_8);
		} catch (Exception e) {
			LOG.error("Read report file error.", e);
		}
	}

	/**
	 * loadNonSupportedObjectText
	 * 
	 * @param reporter MigrationReporter
	 * @param textAppender TextAppender
	 */
	public void loadNonSupportedObjectText(MigrationReporter reporter, TextAppender textAppender) {
		try {
			String nonSptFile = MigrationReportFileUtils.extractNonSupport(reporter.getFileName());
			loadTextFromFile(nonSptFile, textAppender, UTF_8);
		} catch (Exception e) {
			LOG.error("Read report file error.", e);
		}
	}

	/**
	 * loadTextFromFile
	 * 
	 * @param file Text file full name
	 * @param textAppender call back to fill something with text
	 * @param charset file encoding
	 */
	private void loadTextFromFile(String file, TextAppender textAppender, String charset) {
		InputStreamReader reader;
		try {
			if (file == null) {
				return;
			}
			final FileInputStream fin = new FileInputStream(file);
			reader = new InputStreamReader(fin, charset);
			try {
				char[] buff = new char[512];
				int count = reader.read(buff);
				while (count > 0) {
					if (count < buff.length) {
						textAppender.append(new String(buff).substring(0, count));
					} else {
						textAppender.append(new String(buff));
					}
					count = reader.read(buff);
				}
			} finally {
				fin.close();
				reader.close();
			}
		} catch (Exception e) {
			LOG.error("Read report file error.", e);
		}
	}

	/**
	 * @param reporter MigrationReporter
	 */
	public void openMigrationWizardByHistory(MigrationReporter reporter) {
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
	 * Save migration report to a directory
	 * 
	 * @param reporter MigrationReporter
	 * @param outputDir directory
	 * 
	 * @return output files
	 */
	public String saveReportToDirectory(MigrationReporter reporter, String outputDir) {
		MigrationReport report = reporter.getReport();
		String filePartName = outputDir
				+ File.separator
				+ CUBRIDTimeUtil.getDateFormat("yyyy_MM_dd_HH_mm_ss_SSS", Locale.US,
						TimeZone.getDefault()).format(new Date(report.getTotalStartTime()));
		String xlsFile = filePartName + ".xls";
		String logFile = filePartName + ".log";
		String noSupportedFile = filePartName + ".txt";

		StringBuffer savedFiles = new StringBuffer();
		savedFiles.append(saveReportToXls(report, xlsFile));
		savedFiles.append(extracNonSupportedReport(reporter, noSupportedFile));
		savedFiles.append(extractLogs(reporter, logFile));
		return savedFiles.toString();
	}

	/**
	 * @param report MigrationReport
	 * @param xlsFile String
	 * @return the output file name will be returned if successfully
	 */
	private String saveReportToXls(MigrationReport report, String xlsFile) {
		try {
			List<List<String[]>> data = new ArrayList<List<String[]>>();
			getOverviewResults(report, data);
			getObjectResults(report, data);
			getRecordResults(report, data);

			List<String[]> columns = new ArrayList<String[]>();
			columns.add(TABLE_HEADER_OVERVIEW);
			columns.add(TABLE_HEADER_OBJ);
			columns.add(TABLE_HEADER_DATA);

			CUBRIDIOUtils.saveTable2Excel(TAB_NAME, columns, data, xlsFile);
			return xlsFile;
		} catch (Exception ex) {
			LOG.error(ex);
		}
		return "";
	}
}
