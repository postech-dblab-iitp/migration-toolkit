package com.cubrid.cubridmigration.core.engine.task.imp;

import com.cubrid.cubridmigration.core.engine.task.ImportTask;
import com.cubrid.cubridmigration.graph.dbobj.Edge;

public class GraphEdgeHeaderImportTask extends ImportTask{

	private final Edge edge;
	
	public GraphEdgeHeaderImportTask(Edge e) {
		this.edge = e;
	}
	
	@Override
	protected void executeImport() {
		importer.importEdgeHeader(edge);
	}

}
