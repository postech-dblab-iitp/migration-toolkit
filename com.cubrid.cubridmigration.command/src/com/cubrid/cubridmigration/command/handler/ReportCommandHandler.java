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
package com.cubrid.cubridmigration.command.handler;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.cubrid.cubridmigration.command.ConsoleUtils;
import com.cubrid.cubridmigration.core.engine.report.DBObjMigrationResult;
import com.cubrid.cubridmigration.core.engine.report.DataFileImportResult;
import com.cubrid.cubridmigration.core.engine.report.MigrationOverviewResult;
import com.cubrid.cubridmigration.core.engine.report.MigrationReport;
import com.cubrid.cubridmigration.core.engine.report.MigrationReportFileUtils;
import com.cubrid.cubridmigration.core.engine.report.RecordMigrationResult;

/**
 * ReportCommandHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-1-2 created by Kevin Cao
 */
public class ReportCommandHandler extends
		HistoryCommandHandler {

	private PrintStream outPrinter = System.out;
	private int pageSize = 10;

	/**
	 * 
	 * Print report of migration
	 * 
	 * @param args List<String>
	 * 
	 */
	public void handleCommand(List<String> args) {
		if (CollectionUtils.isEmpty(args)) {
			printHelp();
			printHistoryFiles();
			return;
		}
		File file = getHistoryFile(args);
		if (file == null) {
			printHistoryFiles();
			return;
		}
		try {
			outPrinter.println();
			outPrinter.println("Reading migration history file: <"
					+ file.getName() + ">");
			outPrinter.println();
			String rptFile = MigrationReportFileUtils.extractReport(file.getName());
			MigrationReport report = MigrationReport.loadFromReportFile(rptFile);
			printReport(report);

		} catch (IOException e) {
			outPrinter.println("Reading migration history file error:"
					+ e.getMessage());
		}
	}

	/**
	 * Print migration report
	 * 
	 * @param report MigrationReport
	 */
	private void printReport(MigrationReport report) {
		final List<MigrationOverviewResult> overviewResults = report.getOverviewResults();
		outPrinter.println("[Overview]");
		for (MigrationOverviewResult mor : overviewResults) {
			outPrinter.println("    [" + mor.getObjType() + "]");
			outPrinter.println("           Total:[" + mor.getTotalCount() + "]");
			outPrinter.println("        Exported:[" + mor.getExpCount() + "]");
			outPrinter.println("        Imported:[" + mor.getImpCount() + "]");
		}
		if (!waitForEnter()) {
			return;
		}
		outPrinter.println();
		outPrinter.println("[Schema migration]");
		final List<DBObjMigrationResult> dbObjectsResult = report.getDbObjectsResult();
		int pageCount = 1;
		for (DBObjMigrationResult rmr : dbObjectsResult) {
			outPrinter.println("    [" + rmr.getObjType() + "]"
					+ rmr.getObjName() + ":["
					+ (rmr.isSucceed() ? "successfully" : "failed") + "]");
			outPrinter.println("        DDL:[" + rmr.getDdl() + "]");
			if (!rmr.isSucceed()) {
				outPrinter.println("        Error:[" + rmr.getError() + "]");
			}
			if (pageCount >= pageSize) {
				pageCount = 1;
				if (!waitForEnter()) {
					return;
				}
			}
			pageCount++;
		}
		if (!waitForEnter()) {
			return;
		}
		outPrinter.println();
		outPrinter.println("[Data migration]");
		final List<RecordMigrationResult> recMigResults = report.getRecMigResults();
		pageCount = 1;
		for (RecordMigrationResult rmr : recMigResults) {
			outPrinter.println("    [" + rmr.getSource() + "] >> ["
					+ rmr.getTarget() + "]");
			outPrinter.println("           Total:[" + rmr.getTotalCount() + "]");
			outPrinter.println("        Exported:[" + rmr.getExpCount() + "]");
			outPrinter.println("        Imported:[" + rmr.getImpCount() + "]");
			if (pageCount >= pageSize) {
				pageCount = 1;
				if (!waitForEnter()) {
					return;
				}
			}
			pageCount++;
		}
		final List<DataFileImportResult> dataFileResults = report.getDataFileResults();
		pageCount = 1;
		for (DataFileImportResult rmr : dataFileResults) {
			outPrinter.println("    [" + rmr.getFileName() + "]");
			outPrinter.println("        Exported:[" + rmr.getExportCount()
					+ "]");
			outPrinter.println("        Imported:[" + rmr.getImportCount()
					+ "]");
			if (pageCount >= pageSize) {
				pageCount = 1;
				if (!waitForEnter()) {
					return;
				}
			}
			pageCount++;
		}
	}

	/**
	 * printHelp
	 * 
	 */
	protected void printHelp() {
		ConsoleUtils.printHelp("/com/cubrid/cubridmigration/command/help_report.txt");
	}

}
