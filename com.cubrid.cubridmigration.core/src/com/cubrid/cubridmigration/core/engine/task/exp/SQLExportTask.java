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

import java.io.IOException;
import java.util.List;

import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.MigrationStatusManager;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.event.ExportSQLEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.task.ExportTask;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;
import com.cubrid.cubridmigration.core.io.SQLParser;

/**
 * SQLExportTask Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-8 created by Kevin Cao
 */
public class SQLExportTask extends
		ExportTask {

	protected final MigrationContext mrManager;

	protected final String sqlFile;

	public SQLExportTask(MigrationContext mrManager, String sqlFile) {
		this.mrManager = mrManager;
		this.sqlFile = sqlFile;
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
			final MigrationStatusManager statusMgr = mrManager.getStatusMgr();
			SQLParser.ISQLParsingCallback callBack = new SQLParser.ISQLParsingCallback() {

				public boolean isCommitNow(int sqlsSize) {
					//Watching memory to avoid out of memory errors
					int status = MigrationStatusManager.STATUS_WAITING;
					int counter = 0;
					while (true) {
						status = statusMgr.isCommitNow(sqlFile, sqlsSize, config.getCommitCount());
						if (status == MigrationStatusManager.STATUS_WAITING) {
							ThreadUtils.threadSleep(1000, null);
							counter++;
						} else {
							break;
						}
						if (counter >= 10) {
							status = MigrationStatusManager.STATUS_COMMIT;
							break;
						}
					}
					if (status == MigrationStatusManager.STATUS_COMMIT) {
						return true;
					}
					return false;
				}

				public void executeSQLs(List<String> sqlList, long size) {
					//Update export status
					SQLExportTask.this.eventHandler.handleEvent(new ExportSQLEvent(sqlFile,
							sqlList.size()));
					mrManager.getStatusMgr().addExpCount("", sqlFile, sqlList.size());
					//Import SQLs
					ImportTask task = taskFactory.createImportSQLTask(sqlFile, sqlList, size);
					if (config.isCreateConstrainsBeforeData()) {
						importTaskExecutor = mrManager.getImportRecordExecutor();
					}
					importTaskExecutor.execute(task);

				}
			};
			SQLParser.executeSQLFile(sqlFile, config.getSourceFileEncoding(),
					config.getCommitCount(), callBack);

		} catch (IOException e) {
			throw new NormalMigrationException(e);
		} finally {
			mrManager.getStatusMgr().setExpFinished("", sqlFile);
		}
	}
}
