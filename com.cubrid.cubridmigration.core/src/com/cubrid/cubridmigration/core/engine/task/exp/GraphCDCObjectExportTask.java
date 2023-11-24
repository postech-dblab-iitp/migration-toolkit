package com.cubrid.cubridmigration.core.engine.task.exp;

import java.util.List;

import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.RecordExportedListener;
import com.cubrid.cubridmigration.core.engine.event.ExportGraphRecordEvent;
import com.cubrid.cubridmigration.core.engine.event.StartVertexTableEvent;
import com.cubrid.cubridmigration.core.engine.task.ExportTask;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

public class GraphCDCObjectExportTask extends ExportTask {

	private final Vertex vertex;
	private final Edge edge;
	private final MigrationContext mrManager;
	
	public GraphCDCObjectExportTask(Vertex v, Edge e, MigrationContext mrManager) {
		this.vertex = v;
		this.edge = e;
		this.mrManager = mrManager;
	}
	
	@Override
	protected void executeExportTask() {
		// TODO Auto-generated method stub
		exporter.exportCDCObject(vertex, edge, new RecordExportedListener(){
			public void processRecords(String sourceTableName, List<Record> records) {
				eventHandler.handleEvent(new ExportGraphRecordEvent(vertex, records.size()));
				ImportTask task = taskFactory.createCDCRecordsTask(vertex, edge, records);

				importTaskExecutor = mrManager.getImportRecordExecutor();
				importTaskExecutor.execute((Runnable) task);
				//mrManager.getStatusMgr().addExpCount(null, vertex.getVertexLabel(), records.size());
				mrManager.getStatusMgr().addExpCount(null, vertex.getVertexLabel(), records.size());
			}

			public void startExportTable(String tableName) {
				eventHandler.handleEvent(new StartVertexTableEvent(vertex));
			}

			public void endExportTable(String tableName) {
				mrManager.getStatusMgr().setExpFinished(null, vertex.getVertexLabel());
			}
		});
	}
}
