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
import com.cubrid.cubridmigration.core.engine.event.ExportGraphRecordEvent;
import com.cubrid.cubridmigration.core.engine.event.StartVertexTableEvent;
import com.cubrid.cubridmigration.core.engine.task.ExportTask;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

/**
 * JDBCExportRecordTask responses to read records from source database through
 * JDBC driver.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-8 created by Kevin Cao
 */
public class GraphVertexExportTask extends
		ExportTask {

	protected Vertex vertex;
	protected final MigrationContext mrManager;

	public GraphVertexExportTask(MigrationContext mrManager, Vertex v) {
		this.mrManager = mrManager;
		this.vertex = v;
	}

	/**
	 * Export source table's records
	 */
	protected void executeExportTask() {
		exporter.exportGraphVertexRecords(vertex, new RecordExportedListener() {
			public void processRecords(String sourceTableName, List<Record> records) {
				eventHandler.handleEvent(new ExportGraphRecordEvent(vertex, records.size()));
			}

			public void startExportTable(String tableName) {
				eventHandler.handleEvent(new StartVertexTableEvent(vertex));
			}

			public void endExportTable(String tableName) {
				mrManager.getStatusMgr().setExpFinished(null, vertex.getVertexLabel());
			}
		});
	}

	public Vertex getVertex() {
		return vertex;
	}
}
