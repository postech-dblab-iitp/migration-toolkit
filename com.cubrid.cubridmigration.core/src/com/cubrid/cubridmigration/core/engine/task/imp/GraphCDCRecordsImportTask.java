package com.cubrid.cubridmigration.core.engine.task.imp;

import java.util.List;

import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

public class GraphCDCRecordsImportTask extends ImportTask {

	private final Vertex vertex;
	private final List<Record> vRecords;
	
	private final Edge edge;
//	private final List<Record> eRecords;
	
	public GraphCDCRecordsImportTask(Vertex v, Edge e, List<Record> vRecords) {
		this.vertex = v;
		this.vRecords = vRecords;
		this.edge = e;
//		this.eRecords = eRecords;
	}
	
	@Override
	protected void executeImport() {
		// TODO Auto-generated method stub
		importer.importCDCObject(vertex, edge, vRecords);
	}
}