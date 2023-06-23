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
package com.cubrid.cubridmigration.core.engine.report;

import java.beans.XMLEncoder;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.ExportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportGraphRecordEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportGraphRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportSQLsEvent;

/**
 * 
 * Migration Report model
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-7 created by Kevin Cao
 */
public class MigrationReport implements
		Serializable {

	private static final long serialVersionUID = -6045872698227171614L;

	private final static String[] OVERVIEW_TYPES = new String[] {DBObject.OBJ_TYPE_TABLE,
			DBObject.OBJ_TYPE_VIEW, DBObject.OBJ_TYPE_PK, DBObject.OBJ_TYPE_FK,
			DBObject.OBJ_TYPE_INDEX, DBObject.OBJ_TYPE_SEQUENCE, DBObject.OBJ_TYPE_TRIGGER,
			DBObject.OBJ_TYPE_FUNCTION, DBObject.OBJ_TYPE_PROCEDURE, DBObject.OBJ_TYPE_RECORD};
	
	private final static String[] OVERVIEW_GRAPH_TYPES = new String[] {
		DBObject.OBJ_GRAPH_TYPE_VERTEX, DBObject.OBJ_GRAPH_TYPE_EDGE};
	

	/**
	 * get DBObj Name
	 * 
	 * @param obj DBObject
	 * @return name displayed in table
	 */
	private static String getDBObjName(DBObject obj) {
		if (obj instanceof PK && ("PRIMARY".equals(obj.getName()) || obj.getName() == null)) {
			return "primary key of " + ((PK) obj).getTable().getName();
		} else if (obj instanceof Index) {
			return "[" + ((Index) obj).getTable().getName() + "]" + obj.getName();
		}
		return obj.getName();
	}

	private long totalStartTime;
	private long totalEndTime;

	//private List<MigrationOverviewResult> overviewReults;
	private final List<DBObjMigrationResult> dbObjectsResult = new ArrayList<DBObjMigrationResult>();

	private final List<RecordMigrationResult> recMigResults = new ArrayList<RecordMigrationResult>();

	private final List<DataFileImportResult> dataFileResults = new ArrayList<DataFileImportResult>();

	private String configSummary = "";

	private MigrationBriefReport brief;

	private final List<String> errorSQLFiles = new ArrayList<String>();

	/**
	 * add DbObjects Result
	 * 
	 * @param dbObjectsResult DBObjMigrationResult
	 */
	public void addDbObjectsResult(DBObjMigrationResult dbObjectsResult) {
		this.dbObjectsResult.add(dbObjectsResult);
	}

	/**
	 * Add Export CSV event to report.
	 * 
	 * @param event ExportCSVEvent
	 */
	public void addExpCSVEvent(ExportCSVEvent event) {
		for (DataFileImportResult re : dataFileResults) {
			if (!re.getFileName().equals(event.getSourceCSV().getName())) {
				continue;
			}
			re.setExportCount(re.getExportCount() + event.getRecordCount());
			break;
		}
	}

	/**
	 * add Export Migration Record Result
	 * 
	 * @param event ExportRecordsEvent
	 */
	public void addExpMigRecResult(ExportRecordsEvent event) {
		System.out.println("addExpMigRecResult ExportRecordsEvent");
		RecordMigrationResult result = getRecMigResults(event.getSourceTable().getOwner(), 
				event.getSourceTable().getName(),
				event.getSourceTable().getTarget());
		result.setExpCount(result.getExpCount() + event.getRecordCount());
		if (result.getTotalCount() < result.getExpCount()) {
			result.setTotalCount(result.getExpCount());
		}
		if (result.getEndExportTime() < event.getEventTime().getTime()) {
			result.setEndExportTime(event.getEventTime().getTime());
		}
	}

	/**
	 * add Import Migration Record Result
	 * 
	 * @param event ImportRecordsEvent
	 */
	public void addImpMigRecResult(ImportRecordsEvent event) {
		System.out.println("addImpMigRecResult ImportRecordsEvent");
		RecordMigrationResult result = getRecMigResults(event.getSourceTable().getOwner(), 
				event.getSourceTable().getName(),
				event.getSourceTable().getTarget());
		if (event.isSuccess()) {
			result.setImpCount(result.getImpCount() + event.getRecordCount());
		}
		if (result.getStartImportTime() == 0) {
			result.setStartImportTime(event.getEventTime().getTime());
		}
		if (result.getEndImportTime() < event.getEventTime().getTime()) {
			result.setEndImportTime(event.getEventTime().getTime());
		}
		if (StringUtils.isNotBlank(event.getErrorFile())) {
			addErrorSQLFile(event.getErrorFile());
		}
	}
	
	/**
	 * add Export Migration Record Result
	 * 
	 * @param event ExportRecordsEvent
	 */
	public void addGraphExpMigRecResult(ExportGraphRecordEvent event) {
		if (event.getVertex() != null) {
			RecordMigrationResult result = getRecMigResults(null, DBObject.OBJ_GRAPH_TYPE_VERTEX, null);
			result.setExpCount(result.getExpCount() + event.getRecordCount());
			if (result.getTotalCount() < result.getExpCount()) {
				result.setTotalCount(result.getExpCount());
			}
			if (result.getEndExportTime() < event.getEventTime().getTime()) {
				result.setEndExportTime(event.getEventTime().getTime());
			}
		} 
		return;
	}

	/**
	 * add Import Migration Record Result
	 * 
	 * @param event ImportRecordsEvent
	 */
	public void addGraphImpMigRecResult(ImportGraphRecordsEvent event) {
		RecordMigrationResult result;
		if (event.getEdge() != null) {
			result = getRecMigResults(null, DBObject.OBJ_GRAPH_TYPE_EDGE, null);
		} else {
			result = getRecMigResults(null, DBObject.OBJ_GRAPH_TYPE_VERTEX, null);
		}

		if (event.isSuccess()) {
			//Temp
			if (result.getSource() == DBObject.OBJ_GRAPH_TYPE_EDGE) {
				result.setExpCount(result.getExpCount() + event.getRecordCount());
				result.setTotalCount(result.getExpCount());
			}
			result.setImpCount(result.getImpCount() + event.getRecordCount());
		}
		if (result.getStartImportTime() == 0) {
			result.setStartImportTime(event.getEventTime().getTime());
		}
		if (result.getEndImportTime() < event.getEventTime().getTime()) {
			result.setEndImportTime(event.getEventTime().getTime());
		}
		if (StringUtils.isNotBlank(event.getErrorFile())) {
			addErrorSQLFile(event.getErrorFile());
		}
	}

	/**
	 * Add a event of SQL importing
	 * 
	 * @param result ImportSQLsEvent
	 */
	public void addImportCSVEvent(ImportCSVEvent result) {
		for (DataFileImportResult re : dataFileResults) {
			if (!re.getFileName().equals(result.getCsv().getName())) {
				continue;
			}
			//re.setExportCount(re.getExportCount() + result.getRecordCount());
			if (result.isSuccess()) {
				re.setImportCount(re.getImportCount() + result.getRecordCount());
			}
			if (StringUtils.isNotBlank(result.getErrorFile())) {
				addErrorSQLFile(result.getErrorFile());
			}
			break;
		}
	}

	/**
	 * Add a event of SQL importing
	 * 
	 * @param result ImportSQLsEvent
	 */
	public void addSQLImportEvent(ImportSQLsEvent result) {
		for (DataFileImportResult re : dataFileResults) {
			if (!re.getFileName().equals(result.getSqlFile())) {
				continue;
			}
			re.setExportCount(re.getExportCount() + result.getRecordCount());
			if (result.isSuccess()) {
				re.setImportCount(re.getImportCount() + result.getRecordCount());
			}
			if (StringUtils.isNotBlank(result.getErrorFile())) {
				addErrorSQLFile(result.getErrorFile());
			}
			break;
		}
	}

	/**
	 * create a new DBObjectMigrationResult object
	 * 
	 * @param dbo DBObject
	 */
	private void createDBObjMigResult(DBObject dbo) {
		DBObjMigrationResult objResult = new DBObjMigrationResult();
		objResult.setObjName(getDBObjName(dbo));
		objResult.setObjType(dbo.getObjType());
		dbObjectsResult.add(objResult);
	}
	
	public String getConfigSummary() {
		return configSummary;
	}

	/**
	 * getSqlResults
	 * 
	 * @return List<SQLImportResult>
	 */
	public List<DataFileImportResult> getDataFileResults() {
		return new ArrayList<DataFileImportResult>(dataFileResults);
	}

	/**
	 * 
	 * Retrieves the DBObjMigrationResults
	 * 
	 * @return List<DBObjMigrationResult>
	 */
	public List<DBObjMigrationResult> getDbObjectsResult() {
		return new ArrayList<DBObjMigrationResult>(dbObjectsResult);
	}

	/**
	 * Retrieves the database object's migration result by DB Object.
	 * 
	 * @param obj DBObject
	 * @return DBObjMigrationResult
	 */
	public DBObjMigrationResult getDBObjResult(DBObject obj) {
		for (DBObjMigrationResult or : dbObjectsResult) {
			if (or.getObjName().equals(getDBObjName(obj))
					&& or.getObjType().equals(obj.getObjType())) {
				return or;
			}
		}
		DBObjMigrationResult result = new DBObjMigrationResult();
		result.setObjName(getDBObjName(obj));
		result.setObjType(obj.getObjType());
		dbObjectsResult.add(result);
		return result;
	}
	
	public List<MigrationOverviewResult> printOverviewResults() {
		int sourceType = brief.getSourceType();
		int destType = brief.getDestType();
		
		if (sourceType == 4 && destType == 1) {
			return getOverviewResults();
		} else {
			return getGraphOverviewResults();
		}
	}

	/**
	 * Sum the overview results of migration
	 * 
	 * @return List of MigrationOverviewResult
	 */
	public List<MigrationOverviewResult> getOverviewResults() {
		Map<String, MigrationOverviewResult> map = new HashMap<String, MigrationOverviewResult>();
		//DB objects overview
		for (DBObjMigrationResult rs : dbObjectsResult) {
			MigrationOverviewResult mor = map.get(rs.getObjType());
			if (mor == null) {
				mor = new MigrationOverviewResult();
				map.put(rs.getObjType(), mor);
				mor.setObjType(rs.getObjType());
			}
			mor.incExpCount(1);
			if (rs.isSucceed()) {
				mor.incImpCount(1);
			}
			mor.incTotalCount(1);
		}
		//Records overview
		MigrationOverviewResult recMor = map.get(DBObject.OBJ_TYPE_RECORD);
		if (recMor == null) {
			recMor = new MigrationOverviewResult();
			map.put(DBObject.OBJ_TYPE_RECORD, recMor);
			recMor.setObjType(DBObject.OBJ_TYPE_RECORD);
		}
		for (RecordMigrationResult rs : recMigResults) {
			recMor.incExpCount(rs.getExpCount());
			recMor.incImpCount(rs.getImpCount());
			recMor.incTotalCount(rs.getTotalCount());
		}
		//build result list by a order.
		List<MigrationOverviewResult> result = new ArrayList<MigrationOverviewResult>();
		for (String type : OVERVIEW_TYPES) {
			MigrationOverviewResult mor = map.get(type);
			if (mor == null) {
				mor = new MigrationOverviewResult();
				mor.setObjType(type);
				mor.setExpCount(0);
				mor.setImpCount(0);
				mor.setTotalCount(0);
			}
			result.add(mor);
		}
		return result;
	}
	
	/**
	 * Sum the overview results of migration
	 * 
	 * @return List of MigrationOverviewResult
	 */
	public List<MigrationOverviewResult> getGraphOverviewResults() {
		Map<String, MigrationOverviewResult> map = new HashMap<String, MigrationOverviewResult>();
		//Records overview
		MigrationOverviewResult vRecMor = map.get(DBObject.OBJ_GRAPH_TYPE_VERTEX);
		if (vRecMor == null) {
			vRecMor = new MigrationOverviewResult();
			map.put(DBObject.OBJ_GRAPH_TYPE_VERTEX, vRecMor);
			vRecMor.setObjType(DBObject.OBJ_GRAPH_TYPE_VERTEX);
		}
		for (RecordMigrationResult rs : recMigResults) {
			if (rs.getSource() == DBObject.OBJ_GRAPH_TYPE_VERTEX) {
				vRecMor.incExpCount(rs.getExpCount());
				vRecMor.incImpCount(rs.getImpCount());
				vRecMor.incTotalCount(rs.getTotalCount());
			}
		}
		
		MigrationOverviewResult eRecMor = map.get(DBObject.OBJ_GRAPH_TYPE_EDGE);
		if (eRecMor == null) {
			eRecMor = new MigrationOverviewResult();
			map.put(DBObject.OBJ_GRAPH_TYPE_EDGE, eRecMor);
			eRecMor.setObjType(DBObject.OBJ_GRAPH_TYPE_EDGE);
		}
		for (RecordMigrationResult rs : recMigResults) {
			if (rs.getSource() == DBObject.OBJ_GRAPH_TYPE_EDGE) {
				eRecMor.incExpCount(rs.getExpCount());
				eRecMor.incImpCount(rs.getImpCount());
				eRecMor.incTotalCount(rs.getTotalCount());
			}
		}
		
		//build result list by a order.
		List<MigrationOverviewResult> result = new ArrayList<MigrationOverviewResult>();
		for (String type : OVERVIEW_GRAPH_TYPES) {
			MigrationOverviewResult mor = map.get(type);
			if (mor == null) {
				mor = new MigrationOverviewResult();
				mor.setObjType(type);
				mor.setExpCount(0);
				mor.setImpCount(0);
				mor.setTotalCount(0);
			}
			result.add(mor);
		}
		return result;
	}

	public List<RecordMigrationResult> getRecMigResults() {
		return recMigResults;
	}

	/**
	 * get Record Migration Results
	 * 
	 * @param source String
	 * @param target String
	 * @return RecordMigrationResult
	 */
	public RecordMigrationResult getRecMigResults(String owner, String source, String target) {
		for (RecordMigrationResult rmr : recMigResults) {
			// for single schema
			String rmrSource = rmr.getSource();
			if (owner == null 
					&& rmrSource.equalsIgnoreCase(source)) {
				return rmr;
			}
			// for multi schema
			String srcSchema = rmr.getSrcSchema();
			if (srcSchema != null 
					&& srcSchema.equalsIgnoreCase(owner) 
					&& rmrSource.equalsIgnoreCase(source)) {
				return rmr;
			}
		}
		RecordMigrationResult result = new RecordMigrationResult();
		result.setSource(source);
		result.setTarget(target);
		recMigResults.add(result);
		return result;
	}

	/**
	 * Retrieves the end time of migration
	 * 
	 * @return total end time
	 */
	public long getTotalEndTime() {
		return totalEndTime;
	}

	/**
	 * Retrieves the start time of migration
	 * 
	 * @return Long of start time
	 */
	public long getTotalStartTime() {
		return totalStartTime;
	}

	/**
	 * Check if the report has error.
	 * 
	 * @return true if it has error.
	 */
	public boolean hasError() {
		for (DBObjMigrationResult rst : dbObjectsResult) {
			if (StringUtils.isNotBlank(rst.getError())) {
				return true;
			}
		}
		for (RecordMigrationResult rst : recMigResults) {
			if (rst.getExpCount() != rst.getImpCount()) {
				return true;
			}
		}
		for (DataFileImportResult rst : dataFileResults) {
			if (rst.getExportCount() != rst.getImportCount()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Initialize the report.
	 * 
	 * @param config of Migration
	 * @param restore boolean
	 */
	public void initReport(MigrationConfiguration config, boolean restore) {
		dbObjectsResult.clear();
		recMigResults.clear();
		totalStartTime = 0L;
		totalEndTime = 0L;

		if (config.targetIsGraph()) {
			RecordMigrationResult vResult = new RecordMigrationResult();
			vResult.setSource(DBObject.OBJ_GRAPH_TYPE_VERTEX);
			vResult.setExpCount(0);
			vResult.setImpCount(0);
			vResult.setTotalCount(0);
			recMigResults.add(vResult);
			
			RecordMigrationResult eResult = new RecordMigrationResult();
			eResult.setExpCount(0);
			eResult.setImpCount(0);
			eResult.setSource(DBObject.OBJ_GRAPH_TYPE_EDGE);
			eResult.setTotalCount(0);
			recMigResults.add(eResult);

			return;
		}
		
		if (config.sourceIsSQL()) {
			List<String> files = config.getSqlFiles();
			for (String fl : files) {
				DataFileImportResult sir = new DataFileImportResult();
				sir.setFileName(fl);
				sir.setExportCount(0);
				sir.setImportCount(0);
				dataFileResults.add(sir);
			}
			return;
		}
		if (config.sourceIsCSV()) {
			List<SourceCSVConfig> files = config.getCSVConfigs();
			for (SourceCSVConfig fl : files) {
				DataFileImportResult sir = new DataFileImportResult();
				sir.setFileName(fl.getName());
				sir.setExportCount(0);
				sir.setImportCount(0);
				dataFileResults.add(sir);
			}
			return;
		}
		List<PK> pks = new ArrayList<PK>();
		List<FK> fks = new ArrayList<FK>();
		List<Index> indexes = new ArrayList<Index>();
		for (SourceEntryTableConfig setc : config.getExpEntryTableCfg()) {
			if (!setc.isCreateNewTable()) {
				continue;
			}
			Table tt = config.getTargetTableSchema(setc.getTarget());
			if (tt == null) {
				continue;
			}
			if (getDBObjResult(tt) != null) {
				continue;
			}
			createDBObjMigResult(tt);
			//If it is not creating new table, the PK,FK and index will not be recreated. 
			if (!setc.isCreateNewTable()) {
				continue;
			}

			PK pk = tt.getPk();
			if (setc.isCreatePK() && pk != null && !pk.getPkColumns().isEmpty()) {
				pks.add(pk);
			}
			fks.addAll(tt.getFks());
			indexes.addAll(tt.getIndexes());
		}

		for (SourceSQLTableConfig setc : config.getExpSQLCfg()) {
			if (!setc.isCreateNewTable()) {
				continue;
			}
			Table tt = config.getTargetTableSchema(setc.getTarget());
			if (tt == null) {
				continue;
			}
			if (getDBObjResult(tt) != null) {
				continue;
			}
			createDBObjMigResult(tt);
		}

		for (PK pk : pks) {
			createDBObjMigResult(pk);
		}
		for (FK fk : fks) {
			createDBObjMigResult(fk);
		}
		for (Index idx : indexes) {
			createDBObjMigResult(idx);
		}
		List<View> views = config.getTargetViewSchema();
		for (View vw : views) {
			createDBObjMigResult(vw);
		}
		List<Sequence> sequences = config.getTargetSerialSchema();
		for (Sequence sq : sequences) {
			createDBObjMigResult(sq);
		}

		List<SourceEntryTableConfig> allExportTables = config.getExpEntryTableCfg();
		for (SourceTableConfig stc : allExportTables) {
			if (!stc.isCreateNewTable() && !stc.isMigrateData()) {
				continue;
			}
			RecordMigrationResult result = new RecordMigrationResult();
			result.setSrcSchema(stc.getOwner());
			result.setSource(stc.getName());
			result.setTarget(stc.getTarget());
			if (!restore) {
				result.setTotalCount(0);
			} else if (stc.isMigrateData()) {
				Table table = config.getSrcTableSchema(stc.getOwner(), stc.getName());
				if (table != null) {
					result.setTotalCount(table.getTableRowCount());
				}

			}
			recMigResults.add(result);
		}

		List<SourceSQLTableConfig> allExportSqlTables = config.getExpSQLCfg();
		for (SourceSQLTableConfig sstc : allExportSqlTables) {
			if (!sstc.isCreateNewTable() && !sstc.isMigrateData()) {
				continue;
			}
			RecordMigrationResult result = new RecordMigrationResult();
			result.setSource(sstc.getName());
			result.setTarget(sstc.getTarget());
			if (!restore) {
				result.setTotalCount(0);
			} else if (sstc.isMigrateData()) {
				Table srcTableSchema = config.getSrcTableSchema(sstc.getOwner(), sstc.getName());
				result.setTotalCount(srcTableSchema == null ? 0 : srcTableSchema.getTableRowCount());
			}
			recMigResults.add(result);
		}
	}

	public void setConfigSummary(String configSummary) {
		this.configSummary = configSummary;
	}

	/**
	 * Set SQL importing results
	 * 
	 * @param sqlResults List<SQLImportResult>
	 */
	public void setDataFileResults(List<DataFileImportResult> sqlResults) {
		this.dataFileResults.clear();
		if (sqlResults == null) {
			return;
		}
		this.dataFileResults.addAll(sqlResults);
	}

	/**
	 * Set DbObjects Result
	 * 
	 * @param values List<DBObjMigrationResult>
	 */
	public void setDbObjectsResult(List<DBObjMigrationResult> values) {
		this.dbObjectsResult.clear();
		if (values != null) {
			dbObjectsResult.addAll(values);
		}
	}

	/**
	 * Set Record Migration Results
	 * 
	 * @param values List<RecordMigrationResult>
	 */
	public void setRecMigResults(List<RecordMigrationResult> values) {
		recMigResults.clear();
		if (values != null) {
			recMigResults.addAll(values);
		}
	}

	/**
	 * Set the end time of migration
	 * 
	 * @param totalEndTime Long of start time
	 */
	public void setTotalEndTime(long totalEndTime) {
		this.totalEndTime = totalEndTime;
	}

	/**
	 * Set the start time of migration
	 * 
	 * @param totalStartTime Long of start time
	 */
	public void setTotalStartTime(long totalStartTime) {
		this.totalStartTime = totalStartTime;
	}

	public MigrationBriefReport getBrief() {
		return brief;
	}

	public void setBrief(MigrationBriefReport brief) {
		this.brief = brief;
	}

	/**
	 * Save to a xml file
	 * 
	 * @param reportFile output file
	 */
	public void save2ReportFile(String reportFile) {
		XMLEncoder xe = null;
		MigrationBriefReport mbr = brief;
		try {
			final FileOutputStream out = new FileOutputStream(reportFile);
			xe = new XMLEncoder(out);
			//Don't save brief information to file
			brief = null;
			xe.writeObject(this);
			xe.flush();
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		} finally {
			brief = mbr;
			Closer.close(xe);
		}
	}

	/**
	 * loadFromReportFile
	 * 
	 * @param reportFile String
	 * @return MigrationReport
	 */
	public static MigrationReport loadFromReportFile(String reportFile) {
		return (MigrationReport) CUBRIDIOUtils.loadObjectFromXML(reportFile);
	}

	/**
	 * Add error sql file to report
	 * 
	 * @param fileName full name of the file
	 */
	public void addErrorSQLFile(String fileName) {
		if (errorSQLFiles.indexOf(fileName) >= 0) {
			return;
		}
		errorSQLFiles.add(fileName);
	}

	/**
	 * Retrieves the copy of errorSQLFiles
	 * 
	 * @return errorSQLFiles
	 */
	public List<String> getErrorSQLFiles() {
		return new ArrayList<String>(errorSQLFiles);
	}

	/**
	 * Set error sql file to report
	 * 
	 * @param errorSQLFiles List<String>
	 */
	public void setErrorSQLFiles(List<String> errorSQLFiles) {
		this.errorSQLFiles.clear();
		if (errorSQLFiles == null) {
			return;
		}
		for (String file : errorSQLFiles) {
			addErrorSQLFile(file);
		}
	}
}
