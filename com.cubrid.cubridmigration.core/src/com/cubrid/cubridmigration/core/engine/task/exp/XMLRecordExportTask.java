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

import java.util.List;

import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.RecordExportedListener;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.ExportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.StartExpTableEvent;
import com.cubrid.cubridmigration.core.engine.task.ExportTask;

/**
 * JDBCExportRecordTask Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-8 created by Kevin Cao
 */
public class XMLRecordExportTask extends
		ExportTask {

	protected final MigrationContext mrManager;

	public XMLRecordExportTask(MigrationContext mrManager) {
		this.mrManager = mrManager;
	}

	/**
	 * Execute export operation
	 * 
	 */
	protected void executeExportTask() {
		exporter.exportAllRecords(new RecordExportedListener() {

			public void processRecords(String sourceTableName, List<Record> records) {
				//XML doesn't support SQL table
				SourceTableConfig stc = mrManager.getConfig().getExpEntryTableCfg(null,
						sourceTableName);
				if (stc == null || !stc.isMigrateData()) {
					return;
				}
				mrManager.getStatusMgr().addExpCount("", sourceTableName, records.size());
				eventHandler.handleEvent(new ExportRecordsEvent(stc, records.size()));
				importTaskExecutor = mrManager.getImportRecordExecutor();
				importTaskExecutor.execute((Runnable) taskFactory.createImportRecordsTask(stc,
						records));
			}

			public void startExportTable(String tableName) {
				SourceTableConfig stc = mrManager.getConfig().getExpEntryTableCfg(null, tableName);
				eventHandler.handleEvent(new StartExpTableEvent(stc));
			}

			public void endExportTable(String tableName) {
				mrManager.getStatusMgr().setExpFinished("", tableName);
			}
		});
	}
}
