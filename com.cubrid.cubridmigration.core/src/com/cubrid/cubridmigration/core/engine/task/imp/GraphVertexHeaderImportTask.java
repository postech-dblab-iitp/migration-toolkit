package com.cubrid.cubridmigration.core.engine.task.imp;

import com.cubrid.cubridmigration.core.engine.task.ImportTask;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

public class GraphVertexHeaderImportTask extends ImportTask {

	private final Vertex vertex;
	
	public GraphVertexHeaderImportTask(Vertex v) {
		this.vertex = v;
	}
	
	@Override
	protected void executeImport() {
		importer.importVertexHeader(vertex);
	}
}
