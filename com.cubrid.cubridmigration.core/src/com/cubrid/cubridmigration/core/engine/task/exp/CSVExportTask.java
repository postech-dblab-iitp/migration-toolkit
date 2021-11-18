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
package com.cubrid.cubridmigration.core.engine.task.exp;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.MigrationStatusManager;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.event.ExportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.executors.IRunnableExecutor;
import com.cubrid.cubridmigration.core.engine.task.ExportTask;

/**
 * CSVExportTask Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-3-11 created by Kevin Cao
 */
public class CSVExportTask extends
		ExportTask {

	protected final MigrationContext mrManager;

	protected final SourceCSVConfig csvFile;

	public CSVExportTask(MigrationContext mrManager, SourceCSVConfig sqlFile) {
		this.mrManager = mrManager;
		this.csvFile = sqlFile;
	}

	/**
	 * Run
	 */
	public void run() {
		if (null == importTaskExecutor || null == eventHandler) {
			return;
		}
		try {
			executeExportTask();
		} catch (Throwable ex) {
			eventHandler.handleEvent(new MigrationErrorEvent(ex));
		}
	}

	/**
	 * Execute export operation, parsing SQL file and call importing SQLs
	 * 
	 */
	protected void executeExportTask() {
		try {
			final MigrationConfiguration config = mrManager.getConfig();

			//CSVParser parser = new CSVParser();
			//new FileInputStream(csvFile.getName())
			Reader reader = new BufferedReader(new InputStreamReader(
					CUBRIDIOUtils.getFileInputStream(csvFile.getName()),
					config.getCsvSettings().getCharset()));
			final MigrationStatusManager statusMgr = mrManager.getStatusMgr();
			try {
				statusMgr.addExpCount("", csvFile.getName(), 0);
				CSVReader creader = new CSVReader(reader,
						config.getCsvSettings().getSeparateChar(),
						config.getCsvSettings().getQuoteChar(),
						config.getCsvSettings().getEscapeChar());
				List<String[]> data = new ArrayList<String[]>();
				String[] row = creader.readNext();
				if (row == null || row.length == 0) {
					return;
				}
				if (csvFile.isImportFirstRow()) {
					data.add(row);
				}

				long size = 0; //char length of every transaction
				final IRunnableExecutor importExecutor = getImportExecutor();
				row = creader.readNext();
				while (row != null && row.length > 0) {
					for (String ss : row) {
						size = size + (ss == null ? 0 : ss.length());
					}
					data.add(row);
					//Watching memory to avoid out of memory errors
					int status = MigrationStatusManager.STATUS_WAITING;
					int counter = 0;
					while (true) {
						status = statusMgr.isCommitNow(csvFile.getName(), data.size(),
								config.getCommitCount());
						if (status == MigrationStatusManager.STATUS_WAITING) {
							ThreadUtils.threadSleep(1000, null);
							counter++;
						} else {
							break;
						}
						//If waiting for 10 seconds, the data will be committed right now.
						if (counter >= 10) {
							status = MigrationStatusManager.STATUS_COMMIT;
							break;
						}
					}
					if (status == MigrationStatusManager.STATUS_COMMIT) {
						importExecutor.execute(taskFactory.createImportTask(csvFile, data, size));
						statusMgr.addExpCount("", csvFile.getName(), data.size());
						eventHandler.handleEvent(new ExportCSVEvent(csvFile, data.size()));
						data = new ArrayList<String[]>();
						size = 0;
					}
					row = creader.readNext();
				}
				if (!data.isEmpty()) {
					importExecutor.execute(taskFactory.createImportTask(csvFile, data, size));
					statusMgr.addExpCount("", csvFile.getName(), data.size());
					eventHandler.handleEvent(new ExportCSVEvent(csvFile, data.size()));
				}
			} finally {
				statusMgr.setExpFinished("", csvFile.getName());
				reader.close();
			}
		} catch (Exception e) {
			throw new NormalMigrationException(e);
		}
	}

	/**
	 * getImportExecutor
	 * 
	 * @return IRunnableExecutor
	 */
	private IRunnableExecutor getImportExecutor() {
		return mrManager.getImportRecordExecutor();
	}

}
