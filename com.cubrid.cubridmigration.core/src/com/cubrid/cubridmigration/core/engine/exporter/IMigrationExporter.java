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
package com.cubrid.cubridmigration.core.engine.exporter;

import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.engine.RecordExportedListener;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

/**
 * IMigrationExporter Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-9 created by Kevin Cao
 */
public interface IMigrationExporter {

	/**
	 * Export table records
	 * 
	 * @param st SourceTableConfig
	 * @param oneNewRecord RecordExportedListener
	 */
	public void exportTableRecords(SourceTableConfig st,
			RecordExportedListener oneNewRecord);

	/**
	 * Export all tables
	 * 
	 * @param oneNewRecord RecordExportedListener
	 */
	public void exportAllRecords(RecordExportedListener oneNewRecord);

	/**
	 * Default return schema's DDL
	 * 
	 * @param ft function name with schema name :schema.function
	 * @return schema's DDL
	 */
	public DBObject exportFunction(String ft);

	/**
	 * Default return schema's DDL
	 * 
	 * @param pd procedure name with schema name :schema.procedure
	 * @return schema's DDL
	 */
	public DBObject exportProcedure(String pd);

	/**
	 * Default return schema's DDL
	 * 
	 * @param tg trigger name with schema name :schema.tigger
	 * @return schema's DDL
	 */
	public DBObject exportTrigger(String tg);
	
	public void exportGraphVertexRecords(Vertex v, 
			RecordExportedListener oneNewRecord);
	
	public void exportGraphEdgeRecords(Edge e, 
			RecordExportedListener oneNewRecord);
	
	public void exportCDCObject(Vertex vertex, Edge edge, RecordExportedListener mrManager);
}
