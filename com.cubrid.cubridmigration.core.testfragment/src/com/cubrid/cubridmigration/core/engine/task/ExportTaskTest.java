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
package com.cubrid.cubridmigration.core.engine.task;

import org.junit.Test;

import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.engine.IMigrationEventHandler;
import com.cubrid.cubridmigration.core.engine.RecordExportedListener;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.core.engine.executors.IRunnableExecutor;
import com.cubrid.cubridmigration.core.engine.exporter.IMigrationExporter;
import com.cubrid.cubridmigration.core.engine.task.exp.TableSchemaExportTask;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

public class ExportTaskTest {

	@Test
	public void testExportTask() {
		TableSchemaExportTask task = new TableSchemaExportTask(null, null);

		task.run();

		task.setImportTaskExecutor(new IRunnableExecutor() {

			public boolean isBusy() {
				return false;
			}

			public void execute(Runnable task) {

			}

			public void interrupt() {
				//  Auto-generated method stub

			}

			public void dispose() {
				//  Auto-generated method stub

			}
		});
		task.setMigrationEventHandler(null);
		task.setMigrationExporter(null);
		task.run();

		task.setImportTaskExecutor(null);
		task.setMigrationExporter(null);
		task.setMigrationEventHandler(new IMigrationEventHandler() {

			public void handleEvent(MigrationEvent event) {

			}

			public void dispose() {

			}

		});

		task.run();

		task.setImportTaskExecutor(null);
		task.setMigrationEventHandler(null);
		task.setMigrationExporter(new IMigrationExporter() {

			public DBObject exportTrigger(String tg) {
				return null;
			}

			public void exportTableRecords(SourceTableConfig st,
					RecordExportedListener oneNewRecord) {

			}

			public DBObject exportProcedure(String pd) {
				return null;
			}

			public DBObject exportFunction(String ft) {
				return null;
			}

			public void exportAllRecords(RecordExportedListener oneNewRecord) {

			}

			public void exportGraphVertexRecords(Vertex v,
					RecordExportedListener oneNewRecord) {
				
			}

			public void exportGraphEdgeRecords(Edge e,
					RecordExportedListener oneNewRecord) {
				
			}

		});

		task.run();
	}
}
