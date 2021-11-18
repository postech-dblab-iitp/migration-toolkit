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

import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.task.ExportTask;

/**
 * 
 * TableSchemaExportTask responses to export a source table's schema for create
 * table to target database.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-9 created by Kevin Cao
 */
public class CSVTableSchemaExportTask extends
		ExportTask {

	private final SourceCSVConfig sourceTable;
	private final MigrationConfiguration config;

	public CSVTableSchemaExportTask(MigrationConfiguration config,
			SourceCSVConfig tt) {
		this.config = config;
		sourceTable = tt;
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
	 * Get target from configuration, if don't create table, the import task
	 * will do nothing except count down the counter.
	 */
	protected void executeExportTask() {
		Table target = config.getTargetTableSchema(sourceTable.getTarget());
		if (target == null) {
			return;
		}
		importTaskExecutor.execute((Runnable) taskFactory.createImportTableSchemaTask(
				target, sourceTable.isCreate(), sourceTable.isReplace()));
	}
}
