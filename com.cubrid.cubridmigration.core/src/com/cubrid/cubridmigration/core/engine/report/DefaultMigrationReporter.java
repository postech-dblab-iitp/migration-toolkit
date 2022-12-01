/*
 * Copyright (C) 2012 Search Solution Corporation. All rights reserved by Search Solution. 
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
package com.cubrid.cubridmigration.core.engine.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.event.CreateObjectEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportGraphRecordEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.IMigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportGraphRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportSQLsEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationCanceledEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationFinishedEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationNoSupportEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationStartEvent;
import com.cubrid.cubridmigration.core.engine.event.StartExpTableEvent;
import com.cubrid.cubridmigration.core.engine.template.MigrationTemplateParser;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;

/**
 * Default Migration Reporter
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-1 created by Kevin Cao
 */
public abstract class DefaultMigrationReporter implements
		IMigrationReporter {
	protected static final Logger LOG = LogUtil.getLogger(DefaultMigrationReporter.class);

	protected final String fileName; //Only file name , without directory

	protected final MigrationConfiguration config;
	protected String configFile;

	protected MigrationReport report;
	protected File reportFile;

	protected PrintWriter pwLog;
	protected String logFileName;

	protected PrintWriter pwNonsupport;
	protected String nonSupFileName;

	protected String briefFile;

	public DefaultMigrationReporter(File file) {
		config = null;
		fileName = file.getName();
	}

	public DefaultMigrationReporter(MigrationConfiguration config, int startMode) {
		this.config = config;
		long timeTag = System.currentTimeMillis();
		fileName = timeTag + HIS_FILE_EX;

		final MigrationBriefReport brief = new MigrationBriefReport();
		brief.setStartMode(startMode);
		brief.setScriptName(config.getName());
		brief.setSourceType(config.getSourceType());
		brief.setDestType(config.getDestType());
		//brief.setReportFile(fileName);
		if (config.targetIsFile()) {
			brief.setOutputDir(config.getFileRepositroyPath());
		} else {
			brief.setOutputDir(null);
		}
		report = new MigrationReport();
		report.setBrief(brief);
		report.initReport(config, true);
		try {
			final String reportDir = PathUtils.getReportDir();
			briefFile = timeTag + MigrationBriefReport.EX_BRIEF;
			//Log file
			File logFile = new File(reportDir + "migration_log_" + timeTag + LOG_FILE_EX);
			logFileName = logFile.getName();
			PathUtils.createFile(logFile);
			pwLog = new PrintWriter(new OutputStreamWriter(new FileOutputStream(logFile), UTF_8));
			//Report file
			reportFile = new File(reportDir + "migration_report_" + timeTag + REPORT_FILE_EX);
			PathUtils.createFile(reportFile);
			//Objects can't be supported.
			File nonSupportFile = new File(reportDir + "migration_nonsupport_" + timeTag
					+ TXT_FILE_EX);
			PathUtils.createFile(nonSupportFile);
			pwNonsupport = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
					nonSupportFile), UTF_8));
			nonSupFileName = nonSupportFile.getName();
			//Configuration file
			configFile = "migration_script_" + timeTag + SCRIPT_FILE_EX;
			MigrationTemplateParser.save(config, reportDir + configFile, false);
			getSummary();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * add Event
	 * 
	 * @param event MigrationEvent
	 */
	public void addEvent(MigrationEvent event) {
		if (event.getLevel() <= config.getReportLevel()) {
			pwLog.append(CUBRIDTimeUtil.defaultFormatMilin(event.getEventTime())).append(" ");
			pwLog.append(event.toString());
			pwLog.append(" \r\n");
			pwLog.flush();
			if (config.getReportLevel() == MigrationConfiguration.RPT_LEVEL_DEBUG
					&& (event instanceof IMigrationErrorEvent)) {
				IMigrationErrorEvent ev = (IMigrationErrorEvent) event;
				//If report level is debug, reporter will printStackTrace into log file. 
				if (ev.getError() != null) {
					pwLog.append(" \r\n");
					ev.getError().printStackTrace(pwLog);
					pwLog.append(" \r\n");
					pwLog.flush();
				}
			}
		}
		long eventTime = event.getEventTime().getTime();
		if (event instanceof MigrationStartEvent) {
			report.setTotalStartTime(eventTime);
		} else if (event instanceof CreateObjectEvent) {
			CreateObjectEvent ev = (CreateObjectEvent) event;
			if (ev.isSuccess()) {
				DBObjMigrationResult dbor = report.getDBObjResult(ev.getDbObject());
				dbor.setSucceed(true);
				dbor.setDdl(ev.getDbObject().getDDL());
			} else {
				DBObjMigrationResult dbor = report.getDBObjResult(ev.getDbObject());
				dbor.setSucceed(false);
				dbor.setError(ev.getError().getMessage());
				dbor.setDdl(ev.getDbObject().getDDL());
			}
		} else if (event instanceof StartExpTableEvent) {
			StartExpTableEvent ev = (StartExpTableEvent) event;
			RecordMigrationResult recMigResults = report.getRecMigResults(ev.getSourceTable().getOwner(),
					ev.getSourceTable().getName(), ev.getSourceTable().getTarget());
			if (recMigResults.getStartExportTime() <= 0) {
				recMigResults.setStartExportTime(eventTime);
				recMigResults.setStartImportTime(eventTime);
			}
		} else if (event instanceof ExportRecordsEvent) {
			ExportRecordsEvent ev = (ExportRecordsEvent) event;
			report.addExpMigRecResult(ev);
		} else if (event instanceof ImportRecordsEvent) {
			ImportRecordsEvent ev = (ImportRecordsEvent) event;
			report.addImpMigRecResult(ev);
		} else if (event instanceof MigrationNoSupportEvent) {
			MigrationNoSupportEvent ev = (MigrationNoSupportEvent) event;
			DBObject dbObject = ev.getDbObject();
			if (dbObject == null) {
				return;
			}
			pwNonsupport.append(dbObject.getDDL());
			pwNonsupport.append("\n");
			pwNonsupport.flush();
		} else if (event instanceof MigrationFinishedEvent) {
			report.setTotalEndTime(eventTime);
		} else if (event instanceof ImportSQLsEvent) {
			report.addSQLImportEvent((ImportSQLsEvent) event);
		} else if (event instanceof ExportCSVEvent) {
			report.addExpCSVEvent((ExportCSVEvent) event);
		} else if (event instanceof ImportCSVEvent) {
			report.addImportCSVEvent((ImportCSVEvent) event);
		} else if (event instanceof MigrationCanceledEvent) {
			report.getBrief().setStatus(MigrationBriefReport.MS_CANCELED);
		} else if (event instanceof ExportGraphRecordEvent) {
			ExportGraphRecordEvent ev = (ExportGraphRecordEvent) event;
			report.addGraphExpMigRecResult(ev);
		} else if (event instanceof ImportGraphRecordsEvent) {
			ImportGraphRecordsEvent ev = (ImportGraphRecordsEvent) event;
			report.addGraphImpMigRecResult(ev);
		}
	}

	/**
	 * When migration finished.
	 */
	public void finished() {
		try {
			Closer.close(pwLog);
			Closer.close(pwNonsupport);

			final String fullBriefFile = PathUtils.getReportDir() + briefFile;
			final MigrationBriefReport brief = getReport().getBrief();
			brief.setStartTime(report.getTotalStartTime());
			brief.setEndTime(report.getTotalEndTime());
			if (MigrationBriefReport.MS_CANCELED != brief.getStatus()) {
				brief.setStatus(report.hasError() ? MigrationBriefReport.MS_FAILED
						: MigrationBriefReport.MS_SUCCESS);
			}
			brief.save2BriefFile(fullBriefFile);

			report.save2ReportFile(reportFile.getCanonicalPath());

			final String[] inputFiles = new String[] {fullBriefFile, reportFile.getCanonicalPath(),
					PathUtils.getReportDir() + logFileName,
					PathUtils.getReportDir() + nonSupFileName,
					PathUtils.getReportDir() + configFile};
			CUBRIDIOUtils.zip(PathUtils.getReportDir() + fileName, inputFiles, true);
		} catch (Exception e) {
			LOG.error("", e);
		}
	}

	/**
	 * Retrieves the report's file name.
	 * 
	 * @return report file name.
	 */
	public String getFileName() {
		return fileName;
	}

	/**
	 * Retrieves the report.
	 * 
	 * @return report
	 */
	public MigrationReport getReport() {
		return report;
	}

	/**
	 * Get the migration summary string
	 * 
	 */
	protected abstract void getSummary();

	/**
	 * Load the report information of migration history
	 * 
	 */
	public void loadMigrationHistory() {
		try {
			String mhFull = MigrationReportFileUtils.extractReport(fileName);
			if (mhFull == null) {
				throw new IllegalArgumentException("Invalid migration history file:" + fileName);
			}
			report = MigrationReport.loadFromReportFile(mhFull);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}