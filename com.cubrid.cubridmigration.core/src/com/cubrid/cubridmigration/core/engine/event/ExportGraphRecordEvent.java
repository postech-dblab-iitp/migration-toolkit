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
package com.cubrid.cubridmigration.core.engine.event;

import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

/**
 * 
 * MigrationCreateObjectEvent Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-11 created by Kevin Cao
 */
public class ExportGraphRecordEvent extends
		MigrationEvent {

	private final Vertex vertex;
	private final Edge edge;
	private final int recordCount;

	public ExportGraphRecordEvent(Vertex vertex, int recordCount) {
		this.vertex = vertex;
		this.edge = null;
		this.recordCount = recordCount;
	}
	
	public ExportGraphRecordEvent(Edge edge, int recordCount) {
		this.edge = edge;
		this.vertex = null;
		this.recordCount = recordCount;
	}

	public Vertex getVertex() {
		return vertex;
	}
	
	public Edge getEdge() {
		return edge;
	}

	public int getRecordCount() {
		return recordCount;
	}

	/**
	 * To String
	 * 
	 * @return String
	 */
	public String toString() {
		String name;
		if (vertex != null) {
			name = vertex.getVertexLabel();
		} else {
			name = edge.getEdgeLabel();
		}
		
		if (recordCount == 0) {
			return "No record of table [" + name + "] For Graphdb to be exported.";
		}
		return new StringBuffer().append("Exported ").append(recordCount).append(
				" records from table [").append(name).append(
				"] successfully.").toString();
	}

	/**
	 * The event's importance level
	 * 
	 * @return level
	 */
	public int getLevel() {
		return 2;
	}
}
