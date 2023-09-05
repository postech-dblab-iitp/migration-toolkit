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

import java.util.List;

import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.exporter.IMigrationExporter;
import com.cubrid.cubridmigration.core.engine.importer.IMigrationImporter;
import com.cubrid.cubridmigration.core.engine.task.exp.CSVExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.CSVTableSchemaExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.FKExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.FunctionExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.GraphEdgeExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.GraphVertexExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.IndexExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.PKExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.ProcedureExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.SQLExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.SequenceExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.TableRecordExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.TableSchemaExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.TriggerExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.ViewSchemaExportTask;
import com.cubrid.cubridmigration.core.engine.task.exp.XMLRecordExportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.CSVImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.CleanDBTask;
import com.cubrid.cubridmigration.core.engine.task.imp.ExecuteSQLTask;
import com.cubrid.cubridmigration.core.engine.task.imp.FKImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.FunctionImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.GraphEdgeHeaderImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.GraphEdgeImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.GraphVertexHeaderImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.GraphVertexImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.IndexImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.PKImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.ProcedureImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.RecordImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.SQLImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.SequenceImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.TableSchemaImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.TriggerImportTask;
import com.cubrid.cubridmigration.core.engine.task.imp.UpdateAutoIncColCurrentValueTask;
import com.cubrid.cubridmigration.core.engine.task.imp.UpdateStatisticsTask;
import com.cubrid.cubridmigration.core.engine.task.imp.ViewSchemaImportTask;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

/**
 * TaskFactory responses to create migration tasks.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-30 created by Kevin Cao
 */
public class MigrationTaskFactory {

	private MigrationContext context;
	private IMigrationExporter exporter;
	private IMigrationImporter importer;

	public MigrationTaskFactory() {

	}

	/**
	 * initialize ExportTask
	 * 
	 * @param task ExportTask
	 * @param isExportRecords boolean
	 */
	private void initExportTask(ExportTask task, boolean isExportRecords) {
		if (isExportRecords) {
			task.setImportTaskExecutor(context.getImportRecordExecutor());
		} else {
			task.setImportTaskExecutor(context.getDbObjectExe());
		}

		task.setMigrationEventHandler(context.getEventsHandler());
		task.setMigrationExporter(exporter);
		task.setTaskFactory(this);
	}

	/**
	 * createExportFKTask
	 * 
	 * @param fkTable SourceTableConfig
	 * @return FKExportTask
	 */
	public FKExportTask createExportFKTask(SourceTableConfig fkTable) {
		FKExportTask task = new FKExportTask(context.getConfig(), fkTable);
		initExportTask(task, false);
		return task;
	}

	/**
	 * createExportFunctionTask
	 * 
	 * @param ft Function
	 * @return FunctionExportTask
	 */
	public FunctionExportTask createExportFunctionTask(String ft) {
		FunctionExportTask task = new FunctionExportTask(context.getConfig(), ft);
		initExportTask(task, false);
		return task;
	}

	/**
	 * createExportIndexTask
	 * 
	 * @param tb SourceTable
	 * @return IndexExportTask
	 */
	public IndexExportTask createExportIndexTask(SourceTableConfig tb) {
		IndexExportTask task = new IndexExportTask(context.getConfig(), tb);
		initExportTask(task, false);
		return task;
	}

	/**
	 * createExportPKTask
	 * 
	 * @param tb SourceTable
	 * @return PKExportTask
	 */
	public PKExportTask createExportPKTask(SourceTableConfig tb) {
		PKExportTask task = new PKExportTask(context.getConfig(), tb);
		initExportTask(task, false);
		return task;
	}

	/**
	 * createExportProcedureTask
	 * 
	 * @param pd Procedure
	 * @return ProcedureExportTask
	 */
	public ProcedureExportTask createExportProcedureTask(String pd) {
		ProcedureExportTask task = new ProcedureExportTask(context.getConfig(), pd);
		initExportTask(task, false);
		return task;
	}

	/**
	 * createExportTableRecordsTask
	 * 
	 * @param table SourceTable
	 * @return TableRecordExportTask
	 */
	public TableRecordExportTask createExportTableRecordsTask(SourceTableConfig table) {
		TableRecordExportTask task = new TableRecordExportTask(context, table);
		initExportTask(task, true);
		return task;
	}

	/**
	 * createExportAllRecordsTask
	 * 
	 * @return XMLRecordExportTask
	 */
	public XMLRecordExportTask createExportAllRecordsTask() {
		XMLRecordExportTask task = new XMLRecordExportTask(context);
		initExportTask(task, true);
		return task;
	}

	/**
	 * createExportSequenceTask
	 * 
	 * @param sq Sequence
	 * @return SequenceExportTask
	 */
	public SequenceExportTask createExportSequenceTask(SourceSequenceConfig sq) {
		SequenceExportTask task = new SequenceExportTask(context.getConfig(), sq);
		initExportTask(task, false);
		return task;
	}

	/**
	 * createExportTableSchemaTask
	 * 
	 * @param st SourceTable
	 * @return TableSchemaExportTask
	 */
	public TableSchemaExportTask createExportTableSchemaTask(SourceTableConfig st) {
		TableSchemaExportTask task = new TableSchemaExportTask(context.getConfig(), st);
		initExportTask(task, false);
		return task;
	}

	/**
	 * createExportTableSchemaTask
	 * 
	 * @param st SourceTable
	 * @return TableSchemaExportTask
	 */
	public CSVTableSchemaExportTask createExportCSVTableSchemaTask(SourceCSVConfig st) {
		CSVTableSchemaExportTask task = new CSVTableSchemaExportTask(context.getConfig(), st);
		initExportTask(task, false);
		return task;
	}

	/**
	 * createExportTriggerTask
	 * 
	 * @param tg Trigger
	 * @return TriggerExportTask
	 */
	public TriggerExportTask createExportTriggerTask(String tg) {
		TriggerExportTask task = new TriggerExportTask(context.getConfig(), tg);
		initExportTask(task, false);
		return task;
	}

	/**
	 * createExportViewTask
	 * 
	 * @param vw View
	 * @return ViewSchemaExportTask
	 */
	public ViewSchemaExportTask createExportViewTask(SourceConfig vw) {
		ViewSchemaExportTask task = new ViewSchemaExportTask(context.getConfig(), vw);
		initExportTask(task, false);
		return task;
	}

	/**
	 * Initialize the Import task
	 * 
	 * @param task ImportTask
	 */
	private void initImportTask(ImportTask task) {
		task.setImporter(importer);
		task.setMigrationEventHandler(context.getEventsHandler());
	}

	/**
	 * createImportTableSchemaTask
	 * 
	 * @param st TargetTable
	 * @param create boolean
	 * @param replace boolean
	 * @return TableSchemaImportTask
	 */
	public ImportTask createImportTableSchemaTask(Table st, boolean create, boolean replace) {
		TableSchemaImportTask task = new TableSchemaImportTask(st);
		initImportTask(task);
		return task;
	}

	/**
	 * createImportViewTask
	 * 
	 * @param vw View
	 * @return ViewSchemaImportTask
	 */
	public ImportTask createImportViewTask(View vw) {
		ViewSchemaImportTask task = new ViewSchemaImportTask(vw);
		initImportTask(task);
		return task;
	}

	/**
	 * createImportRecordsTask
	 * 
	 * @param tt SourceTableConfig
	 * @param recordsTobeImport recordsTobeImport
	 * @return ImportTask
	 */
	public ImportTask createImportRecordsTask(SourceTableConfig tt, List<Record> recordsTobeImport) {
		ImportTask task = new RecordImportTask(tt, recordsTobeImport);
		initImportTask(task);
		return new ImportDataTaskDecorator(context, task);
	}

	/**
	 * createImportPKTask
	 * 
	 * @param pk PK
	 * @return PKImportTask
	 */
	public ImportTask createImportPKTask(PK pk) {
		PKImportTask task = new PKImportTask(pk);
		initImportTask(task);
		return task;
	}

	/**
	 * createImportFKTask
	 * 
	 * @param fk FK
	 * @return FKImportTask
	 */
	public ImportTask createImportFKTask(FK fk) {
		FKImportTask task = new FKImportTask(fk);
		initImportTask(task);
		return task;
	}

	/**
	 * createImportIndexTask
	 * 
	 * @param tb TargetTable
	 * @return IndexImportTask
	 */
	public ImportTask createImportIndexTask(Table tb) {
		IndexImportTask task = new IndexImportTask(tb);
		initImportTask(task);
		return task;
	}

	/**
	 * createImportSequenceTask
	 * 
	 * @param sq Sequence
	 * @return SequenceImportTask
	 */
	public ImportTask createImportSequenceTask(Sequence sq) {
		SequenceImportTask task = new SequenceImportTask(sq);
		initImportTask(task);
		return task;
	}

	/**
	 * createImportFunctionTask
	 * 
	 * @param ft Function
	 * @return FunctionImportTask
	 */
	public ImportTask createImportFunctionTask(Function ft) {
		FunctionImportTask task = new FunctionImportTask(ft);
		initImportTask(task);
		return task;
	}

	/**
	 * createImportProcedureTask
	 * 
	 * @param pd Procedure
	 * @return ProcedureImportTask
	 */
	public ImportTask createImportProcedureTask(Procedure pd) {
		ProcedureImportTask task = new ProcedureImportTask(pd);
		initImportTask(task);
		return task;
	}

	/**
	 * createImportTriggerTask
	 * 
	 * @param tg Trigger
	 * @return TriggerImportTask
	 */
	public ImportTask createImportTriggerTask(Trigger tg) {
		TriggerImportTask task = new TriggerImportTask(tg);
		initImportTask(task);
		return task;
	}

	/**
	 * Create CleanDBTask
	 * 
	 * @return CleanDBTask
	 */
	public CleanDBTask createCleanDBTask() {
		final CleanDBTask task = new CleanDBTask(context.getConfig());
		initImportTask(task);
		return task;
	}

	/**
	 * Create CleanDBTask
	 * 
	 * @return CleanDBTask
	 */
	public UpdateStatisticsTask createUpdateStatisticsTask() {
		final UpdateStatisticsTask task = new UpdateStatisticsTask(context.getConfig());
		initImportTask(task);
		return task;
	}

	/**
	 * Create importing SQL task
	 * 
	 * @param sqlFile String
	 * @param sqlList List<String>
	 * @param size the bytes of SQL
	 * @return ImportTask
	 */
	public ImportTask createImportSQLTask(String sqlFile, List<String> sqlList, long size) {
		SQLImportTask task = new SQLImportTask(sqlFile, sqlList, size);
		task.setConfig(context.getConfig());
		task.setMrManager(context);
		initImportTask(task);
		return new ImportDataTaskDecorator(context, task);
	}

	/**
	 * createExportSQLTask
	 * 
	 * @param file String
	 * @return IMigrationTask
	 */
	public SQLExportTask createExportSQLTask(String file) {
		SQLExportTask task = new SQLExportTask(context, file);
		initExportTask(task, true);
		return task;
	}

	/**
	 * Create a task that executes SQLs on the target CUBRID database.
	 * 
	 * @param setc SourceEntryTableConfig
	 * @param sql to be executed
	 * @return ExecuteSQLTask
	 */
	public ExecuteSQLTask createExecuteSQLTask(SourceEntryTableConfig setc, String sql) {
		ExecuteSQLTask task = new ExecuteSQLTask(setc, sql);
		initImportTask(task);
		return task;
	}

	/**
	 * Create export CSV task
	 * 
	 * @param csv SourceCSVConfig
	 * @return IMigrationTask
	 */
	public IMigrationTask createExportCSVTask(SourceCSVConfig csv) {
		final CSVExportTask csvExportTask = new CSVExportTask(context, csv);
		initExportTask(csvExportTask, true);
		return csvExportTask;
	}

	/**
	 * Create import task
	 * 
	 * @param csvFile SourceCSVConfig
	 * @param data List<String[]>
	 * @param size long
	 * @return CSVImportTask
	 */
	public ImportTask createImportTask(SourceCSVConfig csvFile, List<String[]> data, long size) {
		final CSVImportTask task = new CSVImportTask(csvFile, data, size);
		initImportTask(task);
		task.setMrManager(context);
		return new ImportDataTaskDecorator(context, task);
	}

	public void setContext(MigrationContext context) {
		this.context = context;
	}

	public void setExporter(IMigrationExporter exporter) {
		this.exporter = exporter;
	}

	public void setImporter(IMigrationImporter importer) {
		this.importer = importer;
	}

	/**
	 * createUpdateAiColumnsCurValTask
	 * 
	 * @param config MigrationConfiguration
	 * @return IMigrationTask
	 */
	public IMigrationTask createUpdateAiColumnsCurValTask(MigrationConfiguration config) {
		UpdateAutoIncColCurrentValueTask result = new UpdateAutoIncColCurrentValueTask(config);
		initImportTask(result);
		return result;
	}
	
	public GraphVertexExportTask createVertexExportTask(Vertex v) {
		GraphVertexExportTask task = new GraphVertexExportTask(context, v);
		initExportTask(task, true);
		return task;
	}
	
	public ImportTask createImportVertexRecordsTask(Vertex v, List<Record> recordsTobeImport) {
		ImportTask task = new GraphVertexImportTask(v, recordsTobeImport);
		initImportTask(task);
		return new ImportDataTaskDecorator(context, task);
	}
	
	public GraphEdgeExportTask GraphEdgeExportTask(Edge e) {
		GraphEdgeExportTask task = new GraphEdgeExportTask(context, e);
		initExportTask(task, true);
		return task;
	}
	
	public ImportTask createImportEdgeRecordsTask(Edge e, List<Record> recordsTobeImport) {
		ImportTask task = new GraphEdgeImportTask(e, recordsTobeImport);
		initImportTask(task);
		return new ImportDataTaskDecorator(context, task);
	}
	
	public ImportTask createEdgeCSVHeaderTask(Edge e) {
		ImportTask task = new GraphEdgeHeaderImportTask(e);
		initImportTask(task);
		return task;
	}
	
	public ImportTask createVertexCSVHeaderTask(Vertex v) {
		ImportTask task = new GraphVertexHeaderImportTask(v);
		initImportTask(task);
		return task;
	}
}
