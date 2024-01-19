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
package com.cubrid.cubridmigration.core.engine.exporter.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.JDBCConManager;
import com.cubrid.cubridmigration.core.engine.MigrationStatusManager;
import com.cubrid.cubridmigration.core.engine.RecordExportedListener;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.exporter.MigrationExporter;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.cubrid.export.CUBRIDExportHelper;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.oracle.export.OracleExportHelper;
import com.cubrid.cubridmigration.tibero.export.TiberoExportHelper;

/**
 * 
 * JDBCMigrationExporter Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-9 created by Kevin Cao
 */
public class JDBCExporter extends
		MigrationExporter {
	protected final static Logger LOG = LogUtil.getLogger(JDBCExporter.class);

	/**
	 * JDBCObjContainer to reuse the JDBC objects
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2014-2-25 created by Kevin Cao
	 */
	protected static class JDBCObjContainer {
		private Connection conn = null;
		private PreparedStatement stmt = null; //NOPMD
		private ResultSet rs = null; //NOPMD

		public Connection getConn() {
			return conn;
		}

		public void setConn(Connection conn) {
			this.conn = conn;
		}

		public PreparedStatement getStmt() {
			return stmt;
		}

		public void setStmt(PreparedStatement stmt) {
			this.stmt = stmt;
		}

		public ResultSet getRs() {
			return rs;
		}

		public void setRs(ResultSet rs) {
			this.rs = rs;
		}
	}

	protected JDBCConManager connManager;
	protected MigrationStatusManager msm;

	//	public JDBCExporter() {
	//	}

	/**
	 * Export all records of all tables
	 * 
	 * @param oneNewRecord processor
	 */
	public void exportAllRecords(RecordExportedListener oneNewRecord) {
		// This method will not be called by clients 
		//		for (SourceEntryTableConfig st : config.getExportEntryTables()) {
		//			exportTableRecords(st, oneNewRecord);
		//		}
		//		for (SourceSQLTableConfig st : config.getExportSQLTables()) {
		//			exportTableRecords(st, oneNewRecord);
		//		}
	}

	/**
	 * Go to next record of result set
	 * 
	 * @param rs result set
	 * @return success or failed
	 */
	protected boolean nextRecord(ResultSet rs) {
		try {
			return rs.next();
		} catch (SQLException e) {
			throw new NormalMigrationException(e);
		}
	}

	/**
	 * Create a new record with target table columns configurations and source
	 * values
	 * 
	 * @param st source table
	 * @param expCols source table's export columns
	 * @param rs result set
	 * @return new record object
	 */
	protected Record createNewRecord(Table st, List<SourceColumnConfig> expCols, ResultSet rs) {
		try {
			Record record = new Record();
			final DBExportHelper srcDBExportHelper = getSrcDBExportHelper();
			for (int ci = 1; ci <= expCols.size(); ci++) {
				SourceColumnConfig cc = expCols.get(ci - 1);
				Column sCol = st.getColumnByName(cc.getName());
				Object value = srcDBExportHelper.getJdbcObject(rs, sCol);
				record.addColumnValue(sCol, value);
			}
			return record;
		} catch (NormalMigrationException e) {
			LOG.error("", e);
			eventHandler.handleEvent(new MigrationErrorEvent(e));
		} catch (SQLException e) {
			LOG.error("", e);
			eventHandler.handleEvent(new MigrationErrorEvent(new NormalMigrationException(
					"Transform table [" + st.getName() + "] record error.", e)));
		} catch (Exception e) {
			LOG.error("", e);
			eventHandler.handleEvent(new MigrationErrorEvent(new NormalMigrationException(
					"Transform table [" + st.getName() + "] record error.", e)));
		}
		return null;
	}

	/**
	 * Export source data records
	 * 
	 * @param stc source table configuration
	 * @param newRecordProcessor to process new records
	 */
	public void exportTableRecords(SourceTableConfig stc, RecordExportedListener newRecordProcessor) {
		//Start normal exporting.
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]exportTableRecordsByPaging()");
		}
		Table sTable = config.getSrcTableSchema(stc.getOwner(), stc.getName());
		if (sTable == null) {
			throw new NormalMigrationException("Table " + stc.getName() + " was not found.");
		}
		final PK srcPK = sTable.getPk();
		Connection conn = connManager.getSourceConnection(); //NOPMD
		try {
			final DBExportHelper expHelper = getSrcDBExportHelper();
			PK pk = expHelper.supportFastSearchWithPK(conn) ? srcPK : null;
			newRecordProcessor.startExportTable(stc.getName());
			List<Record> records = new ArrayList<Record>();
			List<SourceColumnConfig> expColConfs = stc.getColumnConfigList();
			long totalExported = 0L;
			long intPageCount = config.getPageFetchCount();
			String sql = expHelper.getSelectSQL(stc);
			while (true) {
				if (interrupted) {
					return;
				}
				long realPageCount = intPageCount;
				if (!config.isImplicitEstimate()) {
					realPageCount = Math.min(sTable.getTableRowCount() - totalExported,
							intPageCount);
				}
				String pagesql = expHelper.getPagedSelectSQL(sql, realPageCount, totalExported, pk);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[SQL]PAGINATED=" + pagesql);
				}
				long recordCountOfQuery = handleSQL(conn, pagesql, stc, sTable, expColConfs,
						records, newRecordProcessor);
				totalExported = totalExported + recordCountOfQuery;
				//Stop fetching condition: no result;less then fetching count;great then total count
				if (isLatestPage(sTable, totalExported, recordCountOfQuery)) {
					break;
				}
			}
			if (!records.isEmpty()) {
				newRecordProcessor.processRecords(stc.getName(), records);
			}
		} finally {
			newRecordProcessor.endExportTable(stc.getName());
			connManager.closeSrc(conn);
		}
	}

	/**
	 * When new record was exported, CMT should make a choice to commit or
	 * continue or waiting for more free memory.
	 * 
	 * @param stc SourceTableConfig
	 * @param newRecordProcessor RecordExportedListener
	 * @param sTable Table
	 * @param records List<Record> Notice:it will be cleared after committed.
	 */
	protected void handleCommit(SourceTableConfig stc, RecordExportedListener newRecordProcessor,
			Table sTable, List<Record> records) {
		//Watching memory to avoid out of memory errors
		int status = MigrationStatusManager.STATUS_WAITING;
		int counter = 0;
		while (true) {
			status = msm.isCommitNow(sTable.getName(), records.size(), config.getCommitCount());
			if (status == MigrationStatusManager.STATUS_WAITING) {
				ThreadUtils.threadSleep(1000, null);
				counter++;
			} else {
				break;
			}
			if (counter >= 10) {
				status = MigrationStatusManager.STATUS_COMMIT;
				break;
			}
		}
		if (MigrationStatusManager.STATUS_COMMIT == status) {
			newRecordProcessor.processRecords(stc.getName(), records);
			// After records processed, clear it.
			records.clear();
		}
	}

	/**
	 * Execute the selection SQL and handle the result set.
	 * 
	 * @param conn Connection
	 * @param sql String
	 * @param stc SourceTableConfig
	 * @param sTable Source Table
	 * @param expColConfs List<SourceColumnConfig> of Source Table
	 * @param records data cache
	 * @param newRecsHandler processor
	 * @return how many records were handled.
	 */
	protected long handleSQL(Connection conn, String sql, SourceTableConfig stc, Table sTable,
			List<SourceColumnConfig> expColConfs, List<Record> records,
			RecordExportedListener newRecsHandler) {
		JDBCObjContainer joc = new JDBCObjContainer();
		joc.setConn(conn);
		try {
			long totalExported = 0;
			//Execute SQL with retry
			joc = getResultSet(sql, null, joc);
			if (joc.getRs() == null) {
				return totalExported;
			}
			while (nextRecord(joc.getRs())) {
				if (interrupted) {
					return totalExported;
				}
				totalExported++;
				Record record = createNewRecord(sTable, expColConfs, joc.getRs());
				if (record == null) {
					continue;
				}
				records.add(record);
				handleCommit(stc, newRecsHandler, sTable, records);
			}
			return totalExported;
		} finally {
			Closer.close(joc.getRs());
			Closer.close(joc.getStmt());
		}
	}

	/**
	 * If it is page SQL with page query parameters
	 * 
	 * @param sql originate SQL
	 * @param pageSQL replaced by page query parameters SQL
	 * @return true if it is the SQL with page query parameters
	 */
	protected boolean isWithPageQueryParamSQL(String sql, String pageSQL) {
		return !sql.equals(pageSQL);
	}

	/**
	 * Get result set with retry.
	 * 
	 * @param sql to be executed.
	 * @param params parameters to be set to execute SQL
	 * @param joc to return result set and statement.
	 */
	protected JDBCObjContainer getResultSet(String sql, Object[] params, JDBCObjContainer joc) {
		if (joc.getConn() == null) {
			throw new IllegalArgumentException("Connection can't be NULL.");
		}
		//Reset objects.
		joc.setStmt(null);
		joc.setRs(null);
		PreparedStatement stmt = null; //NOPMD
		ResultSet rs = null; //NOPMD
		int retryCount = 0;
		while (true) {
			try {
				stmt = joc.getConn().prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY); //NOPMD
				getSrcDBExportHelper().configStatement(stmt);
				if (params != null && params.length > 0) {
					for (int i = 0; i < params.length; i++) {
						stmt.setObject(i + 1, params[i]);
					}
				}
				rs = stmt.executeQuery(); //NOPMD
				
				break;
			} catch (Exception ex) {
				//Release statement and result set.
				Closer.close(rs);
				Closer.close(stmt);
				stmt = null;
				rs = null;
				ThreadUtils.threadSleep(2000, eventHandler);
				retryCount++;
				if (retryCount == 3) {
					System.out.println(sql);
					throw new NormalMigrationException(ex);
				}
			}
		}
		joc.setStmt(stmt);
		joc.setRs(rs);
		return joc;
	}

	/**
	 * Retrieves the source DB export helper
	 * 
	 * @return DBExportHelper
	 */
	protected DBExportHelper getSrcDBExportHelper() {
		return config.getSourceDBType().getExportHelper();
	}

	public void setConnManager(JDBCConManager connManager) {
		this.connManager = connManager;
	}

	public void setStatusManager(MigrationStatusManager msm) {
		this.msm = msm;
	}

	/**
	 * @param sTable
	 * @param exportedRecords
	 * @param recordCountOfCurrentPage
	 * @return
	 */
	protected boolean isLatestPage(Table sTable, long exportedRecords, long recordCountOfCurrentPage) {
		int sourceDBTypeID = config.getSourceDBType().getID();
		if (config.isImplicitEstimate()
		        && (sourceDBTypeID == DatabaseType.ORACLE.getID()
		        ||  sourceDBTypeID == DatabaseType.MYSQL.getID() || sourceDBTypeID == DatabaseType.TIBERO.getID())) {
			return true;
		}
		
		if (sTable == null) {
			return true;
		}
		
		return recordCountOfCurrentPage == 0
				|| recordCountOfCurrentPage < config.getPageFetchCount()
				|| (!config.isImplicitEstimate() && exportedRecords >= sTable.getTableRowCount());
	}
	
	protected boolean isGraphLatestPage(Table sTable, long exportedRecords, long recordCountOfCurrentPage) {
		int sourceDBTypeID = config.getSourceDBType().getID();
		if (config.isImplicitEstimate()
		        && (sourceDBTypeID == DatabaseType.ORACLE.getID()
		        ||  sourceDBTypeID == DatabaseType.MYSQL.getID() || sourceDBTypeID == DatabaseType.TIBERO.getID())) {
			return true;
		}
		
		if (sTable == null) {
			System.out.println("sTable is null");
			return true;
		}
		
		return recordCountOfCurrentPage == 0
				|| recordCountOfCurrentPage < config.getPageFetchCount()
				|| (!config.isImplicitEstimate() && exportedRecords >= sTable.getTableRowCount());
	}

	public void exportGraphVertexRecords(Vertex v, RecordExportedListener newRecordProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]exportGraphVertexRecords()");
		}
		Table sTable = config.getSrcTableSchema(v.getOwner(), v.getTableName());
		if (sTable == null) {
			throw new NormalMigrationException("Table " + v.getVertexLabel() + " was not found.");
		}
		final PK srcPK = sTable.getPk();
		Connection conn = connManager.getSourceConnection(); //NOPMD
		try {
			final DBExportHelper expHelper = getSrcDBExportHelper();
			DBExportHelper graphExHelper =  getExportHelperType(expHelper);
			PK pk = graphExHelper.supportFastSearchWithPK(conn) ? srcPK : null;
			newRecordProcessor.startExportTable(v.getVertexLabel());
			List<Record> records = new ArrayList<Record>();
			long totalExported = 0L;
			long intPageCount = config.getPageFetchCount();
			String sql = graphExHelper.getGraphSelectSQL(v, config.targetIsCSV());
			while (true) {
				if (interrupted) {
					return;
				}
				long realPageCount = intPageCount;
				if (!config.isImplicitEstimate()) {
					realPageCount = Math.min(sTable.getTableRowCount() - totalExported,
							intPageCount);
				}
				String pagesql;
				
				if (config.targetIsCSV()) {
					pagesql = graphExHelper.getPagedSelectSQLForVertexCSV(v, sql, realPageCount, totalExported, pk);
				} else {
					pagesql = graphExHelper.getPagedSelectSQL(sql, realPageCount, totalExported, pk);
				}
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("[SQL]PAGINATED=" + pagesql);
				}
				
				long recordCountOfQuery = 0L;
				
				if (config.isCdc()) {
					recordCountOfQuery = cdcHandleRecord(conn, pagesql, v, sTable,
							records, newRecordProcessor);
					totalExported = totalExported + recordCountOfQuery;
				} else {
					recordCountOfQuery = graphHandleSQL(conn, pagesql, v, sTable,
							records, newRecordProcessor);
					totalExported = totalExported + recordCountOfQuery;
				}
				
				//Stop fetching condition: no result;less then fetching count;great then total count
				if (isLatestPage(sTable, totalExported, recordCountOfQuery)) {
					break;
				}
			}
			if (!records.isEmpty()) {
				newRecordProcessor.processRecords(v.getVertexLabel(), records);
			}
		} finally {
			newRecordProcessor.endExportTable(v.getVertexLabel());
			connManager.closeSrc(conn);
		}
	}

	public void exportGraphEdgeRecords(Edge e, RecordExportedListener newRecordProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]exportGraphEdgeRecords()");
		}
		
		if (e.getEdgeType() == Edge.JOINTABLE_TYPE) {
			exportGraphJoinEdgeRecords(e, newRecordProcessor);
			return;
		}
		
		if (config.targetIsCSV()) {
			exportGraphEdgeRecordForCSV(e, newRecordProcessor);
			return;
		}
		
		if (config.isCdc()) {
			exportGraphFKEdgeForCDC(e, newRecordProcessor);
			return;
		}
		
		try {
			newRecordProcessor.startExportTable(e.getEdgeLabel());
			newRecordProcessor.processRecords(e.getEdgeLabel(), null);
		} finally {
			newRecordProcessor.endExportTable(e.getEdgeLabel());
		}
	}
	
	protected List<Record> createGraphNewRecordForEdgeCDC(Edge e, List<Column> cols, Connection conn) {
		BufferedReader reader = null;
		
		try {
			List<Record> recList = new ArrayList<Record>();
			
			String cubridEnv = System.getenv("CUBRID");
			String cdcOutputDir = cubridEnv + File.separator + "cdc_output";
			File dir = new File(cdcOutputDir);
			
			String[] fileNameArr = dir.list();
			
			for (String fileName : fileNameArr) {
				Record rec = new Record();
				
				reader = new BufferedReader(new FileReader(cdcOutputDir + File.separator + fileName));
				
				String line = reader.readLine();
				String oid = null;
				
				if (line != null) {
					String[] valueArr = line.split(":");
					
					oid = valueArr[1];
				} else {
					return null;
				}
				
				LOG.info("edge Label : " + e.getEdgeLabel());
				LOG.info("edge oid Value : " + e.getOid());
				LOG.info("file oid Value : " + oid);
				
				String longToString = String.valueOf(e.getOid());
				
				if (oid.equals(longToString)) {
					
					LOG.info("each of oid is same");
					
					while ((line = reader.readLine()) != null) {
						String[] colVal = line.split(":");
						
						Column col = cols.get(Integer.parseInt(colVal[0]));
						String value = colVal[1];
						
						rec.addColumnValue(col, value);
					}
					
					recList.add(rec);
				} else {
					LOG.info("oid is not same");
				}
			}
			
			return recList;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("exception from export file data part");
		} finally {
			Closer.close(reader);
		}
		
		return null;
	}
	
	protected List<Record> createGraphNewFKEdgeCDC(Edge e, List<Column> cols, Connection conn) {
		BufferedReader reader = null;
		
		Vertex endVertex = e.getEndVertex();
		
		cols = endVertex.getColumnList();
		
		try {
			List<Record> recList = new ArrayList<Record>();
			
			String cubridEnv = System.getenv("CUBRID");
			String cdcOutputDir = cubridEnv + File.separator + "cdc_output";
			File dir = new File(cdcOutputDir);
			
			String[] fileNameArr = dir.list();
			
			for (String fileName : fileNameArr) {
				Record rec = new Record();
				
				reader = new BufferedReader(new FileReader(cdcOutputDir + File.separator + fileName));
				
				String line = reader.readLine();
				String oid = null;
				
				if (line != null) {
					String[] valueArr = line.split(":");
					
					oid = valueArr[1];
				} else {
					return null;
				}
				
//				LOG.info("edge Label : " + e.getEdgeLabel());
//				LOG.info("edge oid Value : " + e.getOid());
//				LOG.info("file oid Value : " + oid);
				
//				String longToString = String.valueOf(e.getOid());
				String longToString = String.valueOf(endVertex.getOid());
				
				if (oid.equals(longToString)) {
					
//					LOG.info("each of oid is same");
					
					while ((line = reader.readLine()) != null) {
						String[] colVal = line.split(":");
						
						Column col = cols.get(Integer.parseInt(colVal[0]));
						String value = colVal[1];
						
						rec.addColumnValue(col, value);
					}
					
					recList.add(rec);
				} else {
					LOG.info("oid is not same");
				}
			}
			
			return recList;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("exception from export file data part");
		} finally {
			Closer.close(reader);
		}
		
		return null;
	}
	
	protected void exportGraphEdgeRecordForCSV(Edge e, RecordExportedListener newRecordProcessor) { 
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]exportGraphVertexRecords()");
		}
		
		//Table sTable = config.getSrcTableSchemaForEdge(e.getOwner(), e.getEdgeLabel());
		Table sTable = config.getSrcTableSchemaForEdge(e.getOwner(), e.getStartVertexName());
		Connection conn = connManager.getSourceConnection(); //NOPMD
		
		long countOfRecords = graphFkEdgeCountSQL(conn, e);
		
		try {
			final DBExportHelper expHelper = getSrcDBExportHelper();
			DBExportHelper graphExHelper = expHelper;
//			PK pk = graphExHelper.supportFastSearchWithPK(conn) ? srcPK : null;
			newRecordProcessor.startExportTable(e.getEdgeLabel());
			List<Record> records = new ArrayList<Record>();
			long totalExported = 0L;
			long intPageCount = config.getPageFetchCount();
			String sql = graphExHelper.getGraphSelectSQL(e);
			while (true) {
				if (interrupted) {
					return;
				}
				long realPageCount = intPageCount;
				if (!config.isImplicitEstimate()) {
					realPageCount = Math.min(countOfRecords - totalExported,
							intPageCount);
				}
				String pageSql;
				
				pageSql = graphExHelper.getPagedFkRecords(e, sql, realPageCount, totalExported, hasMultiSchema(conn));
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("[SQL]PAGINATED=" + pageSql);
				}
				long recordCountOfQuery = graphEdgeHandleSQL(conn, pageSql, e, sTable,
						records, newRecordProcessor);
				totalExported = totalExported + recordCountOfQuery;
				//Stop fetching condition: no result;less then fetching count;great then total count
				if (isLatestPage(sTable, totalExported, recordCountOfQuery)) {
					break;
				}
			}
			if (!records.isEmpty()) {
				newRecordProcessor.processRecords(e.getEdgeLabel(), records);
			}
		} finally {
			newRecordProcessor.endExportTable(e.getEdgeLabel());
			connManager.closeSrc(conn);
		}
	}
	
	protected void exportGraphFKEdgeForCDC(Edge e, RecordExportedListener newRecordProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]exportGraphVertexRecords()");
		}
		Connection conn = connManager.getSourceConnection(); //NOPMD
		
		String endVertexName = e.getEndVertexName();
		
		Table sTable = config.getSrcTableSchema(e.getOwner(), endVertexName);
		if (sTable == null) {
			throw new NormalMigrationException("Table " + e.getEdgeLabel() + " was not found.");
		}
		final PK srcPK = sTable.getPk();
		try {
			final DBExportHelper expHelper = getSrcDBExportHelper();
			DBExportHelper graphExHelper = getExportHelperType(expHelper);
			PK pk = graphExHelper.supportFastSearchWithPK(conn) ? srcPK : null;
			newRecordProcessor.startExportTable(e.getEdgeLabel());
			List<Record> records = new ArrayList<Record>();
			long totalExported = 0L;
			long intPageCount = config.getPageFetchCount();
			String sql = graphExHelper.getGraphSelectSQL(e);
			while (true) {
				if (interrupted) {
					return;
				}
				long realPageCount = intPageCount;
				if (!config.isImplicitEstimate()) {
					realPageCount = Math.min(sTable.getTableRowCount() - totalExported,
							intPageCount);
				}
				String pagesql;

				pagesql = graphExHelper.getPagedSelectSQL(sql, realPageCount, totalExported, pk);
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("[SQL]PAGINATED=" + pagesql);
				}
				
				long recordCountOfQuery;
				
				recordCountOfQuery = cdcHandleFk(conn, e, records, newRecordProcessor);
				totalExported = totalExported + recordCountOfQuery;					
				
				//Stop fetching condition: no result;less then fetching count;great then total count
				if (isLatestPage(sTable, totalExported, recordCountOfQuery)) {
					break;
				}
			}
			if (!records.isEmpty()) {
				newRecordProcessor.processRecords(e.getEdgeLabel(), records);
			}
		} finally {
			newRecordProcessor.endExportTable(e.getEdgeLabel());
			connManager.closeSrc(conn);
		}
	}
	
	protected long graphFkEdgeCountSQL(Connection conn, Edge e) {		
		Map<String, String> fkMapping = e.getfkCol2RefMapping();
		
		String sql;
		
		sql= editFkRecordCounterSql(e, fkMapping);
		
		JDBCObjContainer joc = new JDBCObjContainer();
		joc.setConn(conn);
		
		long totalExported = 0;
		
		try {
			//Execute SQL with retry
			joc = getResultSet(sql, null, joc);
			if (joc.getRs() == null) {
				return totalExported;
			}
			
			while(joc.getRs().next()){
				totalExported = joc.getRs().getLong(1);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return totalExported;
	}
	
	private String editFkRecordCounterSql(Edge e, Map<String, String> fkMapping) {
		List<String> keySet = e.getFKColumnNames();
		
		String startVertexName;
		String endVertexName;
		
		if (e.getStartVertexName().equals(e.getEndVertexName())) {
			startVertexName = e.getStartVertexName() + "_1";
			endVertexName = e.getEndVertexName() + "_2";
		} else {
			startVertexName = e.getStartVertexName();
			endVertexName = e.getEndVertexName();
		}
		
		String fkCol = keySet.get(0);
		String refCol = fkMapping.get(keySet.get(0));
		
		StringBuffer buffer = new StringBuffer();
		buffer.append("SELECT COUNT(1) FROM ");
		
		buffer.append("(SELECT ");
		buffer.append(fkCol);
		buffer.append(" FROM ");
		buffer.append(e.getStartVertexName());
		buffer.append(" ) as ");
		buffer.append(startVertexName);
		buffer.append(", ");
		
		buffer.append("(SELECT ");
		buffer.append(refCol);
		buffer.append(" FROM ");
		buffer.append(e.getEndVertexName());
		buffer.append(" ) as ");
		buffer.append(endVertexName);
		
		buffer.append(" where ");
		buffer.append(startVertexName + "." + fkCol);
		buffer.append(" = ");
		buffer.append(endVertexName + "." + refCol);
		
		return buffer.toString();
	}
	
	protected void exportGraphJoinEdgeRecords(Edge e, RecordExportedListener newRecordProcessor) { 
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]exportGraphVertexRecords()");
		}
		Table sTable = config.getSrcTableSchema(e.getOwner(), e.getEdgeLabel());
		if (sTable == null) {
			throw new NormalMigrationException("Table " + e.getEdgeLabel() + " was not found.");
		}
		final PK srcPK = sTable.getPk();
		Connection conn = connManager.getSourceConnection(); //NOPMD
		try {
			final DBExportHelper expHelper = getSrcDBExportHelper();
			DBExportHelper graphExHelper = getExportHelperType(expHelper);
			PK pk = graphExHelper.supportFastSearchWithPK(conn) ? srcPK : null;
			newRecordProcessor.startExportTable(e.getEdgeLabel());
			List<Record> records = new ArrayList<Record>();
			long totalExported = 0L;
			long intPageCount = config.getPageFetchCount();
			String sql = graphExHelper.getGraphSelectSQL(e);
			while (true) {
				if (interrupted) {
					return;
				}
				long realPageCount = intPageCount;
				if (!config.isImplicitEstimate()) {
					realPageCount = Math.min(sTable.getTableRowCount() - totalExported,
							intPageCount);
				}
				String pagesql;
				
				if (config.targetIsCSV()) {
					pagesql = graphExHelper.getPagedSelectSQLForEdgeCSV(e, sql, realPageCount, totalExported, pk, hasMultiSchema(conn));
				} else {
					pagesql = graphExHelper.getPagedSelectSQL(sql, realPageCount, totalExported, pk);
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug("[SQL]PAGINATED=" + pagesql);
				}
				
				long recordCountOfQuery;
				
				if (config.isCdc()) {
					recordCountOfQuery = cdcHandleRecord(conn, pagesql, e, sTable,
							records, newRecordProcessor);
					totalExported = totalExported + recordCountOfQuery;					
				} else {
					recordCountOfQuery = graphEdgeHandleSQL(conn, pagesql, e, sTable,
							records, newRecordProcessor);
					totalExported = totalExported + recordCountOfQuery;					
				}
				
				//Stop fetching condition: no result;less then fetching count;great then total count
				if (isLatestPage(sTable, totalExported, recordCountOfQuery)) {
					break;
				}
			}
			if (!records.isEmpty()) {
				newRecordProcessor.processRecords(e.getEdgeLabel(), records);
			}
		} finally {
			newRecordProcessor.endExportTable(e.getEdgeLabel());
			connManager.closeSrc(conn);
		}
	}
	
	protected long graphHandleSQL(Connection conn, String sql, Vertex vertex, Table sTable, List<Record> records,
			RecordExportedListener newRecsHandler) {
		JDBCObjContainer joc = new JDBCObjContainer();
		joc.setConn(conn);
		try {
			long totalExported = 0;
			//Execute SQL with retry
			joc = getResultSet(sql, null, joc);
			if (joc.getRs() == null) {
				return totalExported;
			}
			while (nextRecord(joc.getRs())) {
				if (interrupted) {
					return totalExported;
				}
				totalExported++;
				Record record;
				
				if (config.targetIsCSV()) {
					record = createGraphNewRecordForVertexCSV(vertex, vertex.getColumnList(), joc.getRs());
				} else {
					record = createGraphNewRecord(sTable, vertex.getColumnList(), joc.getRs());
				}
				
				if (record == null) {
					continue;
				}
				records.add(record);
				handleGraphCommit(vertex.getVertexLabel(), newRecsHandler, sTable, records);
			}
			
			return totalExported;
		} finally {
			Closer.close(joc.getRs());
			Closer.close(joc.getStmt());
		}
	}
	
	protected long cdcHandleRecord(Connection conn, String sql, Vertex vertex, Table sTable, List<Record> records,
			RecordExportedListener newRecsHandler) {
		
		long totalExported = 0;
		List<Record> recordList;
		
		recordList = createGraphNewRecordForVertexCDC(vertex, vertex.getColumnList(), conn);
		
		if (recordList != null) {
			totalExported = recordList.size();
		}
		
		records.addAll(recordList);
		
		return totalExported;
	}
	
	protected long cdcHandleRecord(Connection conn, String sql, Edge edge, Table sTable, List<Record> records,
			RecordExportedListener newRecsHandler) {
		
		long totalExported = 0;
		List<Record> recordList;
		
		recordList = createGraphNewRecordForEdgeCDC(edge, edge.getColumnList(), conn);
		
		if (recordList != null) {
			totalExported = recordList.size();
		}
		
		records.addAll(recordList);
		
		return totalExported;
	}
	
	protected long cdcHandleFk(Connection conn, Edge edge, List<Record> records,
			RecordExportedListener newRecsHandler) {
		
		long totalExported = 0;
		List<Record> recordList;
		
		recordList = createGraphNewFKEdgeCDC(edge, edge.getColumnList(), conn);
		
		if (recordList != null) {
			totalExported = recordList.size();
		}
		
		records.addAll(recordList);
		
		return totalExported;
	}

	protected long graphEdgeHandleSQL(Connection conn, String sql, Edge edge, Table sTable, List<Record> records,
			RecordExportedListener newRecsHandler) {
		JDBCObjContainer joc = new JDBCObjContainer();
		joc.setConn(conn);
		try {
			long totalExported = 0;
			//Execute SQL with retry
			joc = getResultSet(sql, null, joc);
			if (joc.getRs() == null) {
				return totalExported;
			}
			while (nextRecord(joc.getRs())) {
				if (interrupted) {
					return totalExported;
				}
				totalExported++;
				Record record;
				
				if (config.targetIsCSV()) {
					record = createGraphNewRecordForFkCSV(edge, edge.getColumnList(), joc.getRs());
				} else {
					record = createGraphNewRecord(sTable, edge.getColumnList(), joc.getRs());
				}
				
				if (record == null) {
					continue;
				}
				records.add(record);
				handleGraphCommitForFkCSV(edge.getEdgeLabel(), newRecsHandler, edge, records);
			}
			return totalExported;
		} finally {
			Closer.close(joc.getRs());
			Closer.close(joc.getStmt());
		}
	}
	
	protected Record createGraphNewRecord(Table st, List<Column> cols, ResultSet rs) {
		try {
			Record record = new Record();
			final DBExportHelper srcDBExportHelper = getSrcDBExportHelper();
			for (int ci = 1; ci <= cols.size(); ci++) {
				Column cc = cols.get(ci - 1);
				if (!cc.getSupportGraphDataType()) {
					continue;
				}
				Column sCol = st.getColumnByName(cc.getName());
				Object value = srcDBExportHelper.getJdbcObject(rs, sCol);
				
				// Tibero JDBC DATE TYPE Issue
				if (sCol.getDataType().equals("DATE")) {
					if (value instanceof java.sql.Timestamp) {
						value = new java.sql.Date(((java.sql.Timestamp) value).getTime());
					}
				}
				record.addColumnValue(sCol, value);
			}
			return record;
		} catch (NormalMigrationException e) {
			LOG.error("", e);
			eventHandler.handleEvent(new MigrationErrorEvent(e));
		} catch (SQLException e) {
			LOG.error("", e);
			eventHandler.handleEvent(new MigrationErrorEvent(new NormalMigrationException(
					"Transform table [" + st.getName() + "] record error.", e)));
		} catch (Exception e) {
			LOG.error("", e);
			eventHandler.handleEvent(new MigrationErrorEvent(new NormalMigrationException(
					"Transform table [" + st.getName() + "] record error.", e)));
		}
		return null;
	}
	
	protected List<Record> createGraphNewRecordForVertexCDC(Vertex v, List<Column> cols, Connection conn) {
		BufferedReader reader = null;
		File dir = null;
		
		try {
			List<Record> recList = new ArrayList<Record>();
			
			String cubridEnv = System.getenv("CUBRID");
			String cdcOutputDir = cubridEnv + File.separator + "cdc_output";
			dir = new File(cdcOutputDir);
			
			String[] fileNameArr = dir.list();
			
			for (String fileName : fileNameArr) {
				Record rec = new Record();
				
				reader = new BufferedReader(new FileReader(cdcOutputDir + File.separator + fileName));
				
				String line = reader.readLine();
				String oid = null;
				
				if (line != null) {
					String[] valueArr = line.split(":");
					
					oid = valueArr[1];
				} else {
					return null;
				}
//				
//				LOG.info("vertex Label : " + v.getVertexLabel());
//				LOG.info("vertex oid Value : " + v.getOid());
//				LOG.info("file oid Value : " + oid);
//				
				String longToString = String.valueOf(v.getOid());
				
				if (oid.equals(longToString)) {
					
//					LOG.info("each of oid is same");
					
					while ((line = reader.readLine()) != null) {
						String[] colVal = line.split(":");
						
						Column col = cols.get(Integer.parseInt(colVal[0]));
						String value = colVal[1];
						
						rec.addColumnValue(col, value);
					}
					
					recList.add(rec);
				} else {
//					LOG.info("oid is not same");
				}
			}
			
			return recList;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("exception from export file data part");
		} finally {
			Closer.close(reader);
		}
		
		return null;
	}
	
	protected Record createGraphNewRecordForVertexCSV(Vertex v, List<Column> cols, ResultSet rs) {
		try {
			Record record = new Record();
			final DBExportHelper srcDBExportHelper = getSrcDBExportHelper();
			for (int ci = 1; ci <= cols.size(); ci++) {
				Column cc = cols.get(ci - 1);
				if (!cc.getSupportGraphDataType()) {
					continue;
				}
				Column sCol = v.getColumnByName(cc.getName());
				Object value = srcDBExportHelper.getJdbcObjectForCSV(rs, sCol);
				record.addColumnValue(sCol, value);
			}
			return record;
		} catch (NormalMigrationException e) {
			LOG.error("", e);
			eventHandler.handleEvent(new MigrationErrorEvent(e));
		} catch (SQLException e) {
			LOG.error("", e);
			eventHandler.handleEvent(new MigrationErrorEvent(new NormalMigrationException(
					"Transform table [" + v.getName() + "] record error.", e)));
		} catch (Exception e) {
			LOG.error("", e);
			eventHandler.handleEvent(new MigrationErrorEvent(new NormalMigrationException(
					"Transform table [" + v.getName() + "] record error.", e)));
		}
		return null;
	}
	
	protected Record createGraphNewRecordForFkCSV(Edge e, List<Column> cols, ResultSet rs) {
		try {
			Record record = new Record();
			final DBExportHelper srcDBExportHelper = getSrcDBExportHelper();
			for (int ci = 1; ci <= cols.size(); ci++) {
				Column cc = cols.get(ci - 1);
				if (!cc.getSupportGraphDataType()) {
					continue;
				}
				Column sCol = e.getColumnbyName(cc.getName());
				Object value = srcDBExportHelper.getJdbcObjectForCSV(rs, sCol);
				record.addColumnValue(sCol, value);
			}
			return record;
		} catch (NormalMigrationException ex) {
			LOG.error("", ex);
			eventHandler.handleEvent(new MigrationErrorEvent(ex));
		} catch (SQLException ex) {
			LOG.error("", ex);
			eventHandler.handleEvent(new MigrationErrorEvent(new NormalMigrationException(
					"Transform foreign key [" + e.getEdgeLabel() + "] record error.", ex)));
		} catch (Exception ex) {
			LOG.error("", ex);
			eventHandler.handleEvent(new MigrationErrorEvent(new NormalMigrationException(
					"Transform foreign key [" + e.getEdgeLabel() + "] record error.", ex)));
		}
		return null;
	}
	
	protected void handleGraphCommit(String tableName, RecordExportedListener newRecordProcessor,
			Table sTable, List<Record> records) {
		//Watching memory to avoid out of memory errors
		int status = MigrationStatusManager.STATUS_WAITING;
		int counter = 0;
		while (true) {
			status = msm.isCommitNow(sTable.getName(), records.size(), config.getCommitCount());
			if (status == MigrationStatusManager.STATUS_WAITING) {
				ThreadUtils.threadSleep(1000, null);
				counter++;
			} else {
				break;
			}
			if (counter >= 10) {
				status = MigrationStatusManager.STATUS_COMMIT;
				break;
			}
		}
		if (MigrationStatusManager.STATUS_COMMIT == status) {
			newRecordProcessor.processRecords(tableName, records);
			// After records processed, clear it.
			records.clear();
		}
	}
	
	protected void handleGraphCommitForFkCSV(String tableName, RecordExportedListener newRecordProcessor,
			Edge edge, List<Record> records) {
		//Watching memory to avoid out of memory errors
		int status = MigrationStatusManager.STATUS_WAITING;
		int counter = 0;
		while (true) {
			status = msm.isCommitNow(edge.getEdgeLabel(), records.size(), config.getCommitCount());
			if (status == MigrationStatusManager.STATUS_WAITING) {
				ThreadUtils.threadSleep(1000, null);
				counter++;
			} else {
				break;
			}
			if (counter >= 10) {
				status = MigrationStatusManager.STATUS_COMMIT;
				break;
			}
		}
		if (MigrationStatusManager.STATUS_COMMIT == status) {
			newRecordProcessor.processRecords(tableName, records);
			// After records processed, clear it.
			records.clear();
		}
	}
	
	private boolean hasMultiSchema(Connection con) {
		int versionValue = 0;
		try {
			versionValue = (con.getMetaData().getDatabaseMajorVersion() * 10) + con.getMetaData().getDatabaseMinorVersion();
		} catch (SQLException e) {
			e.printStackTrace();
			
			return true;
		}
		
		return versionValue >= 112;
	}
	
	private DBExportHelper getExportHelperType(DBExportHelper exportHelper) {
		if (exportHelper instanceof CUBRIDExportHelper) {
			return (CUBRIDExportHelper) exportHelper;
		} else if (exportHelper instanceof OracleExportHelper) {
			return (OracleExportHelper) exportHelper;
		} else if (exportHelper instanceof TiberoExportHelper) {
			return (TiberoExportHelper) exportHelper;
		} else {
			return (CUBRIDExportHelper) exportHelper;
		}
	}

	public void exportCDCObject(Vertex vertex, Edge edge, RecordExportedListener newRecordProcessor) {
		
		LOG.info("vertex name: " + vertex.getVertexLabel());
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]exportGraphVertexRecords()");
		}
		Table sTable = config.getSrcTableSchema(vertex.getOwner(), vertex.getTableName());
		if (sTable == null) {
			throw new NormalMigrationException("Table " + vertex.getVertexLabel() + " was not found.");
		}
		final PK srcPK = sTable.getPk();
		Connection conn = connManager.getSourceConnection(); //NOPMD
		try {
			final DBExportHelper expHelper = getSrcDBExportHelper();
			DBExportHelper graphExHelper =  getExportHelperType(expHelper);
			PK pk = graphExHelper.supportFastSearchWithPK(conn) ? srcPK : null;
			newRecordProcessor.startExportTable(vertex.getVertexLabel());
			List<Record> records = new ArrayList<Record>();
			long totalExported = 0L;
			long intPageCount = config.getPageFetchCount();
			String sql = graphExHelper.getGraphSelectSQL(vertex, config.targetIsCSV());
			while (true) {
				if (interrupted) {
					return;
				}
				long realPageCount = intPageCount;
				if (!config.isImplicitEstimate()) {
					realPageCount = Math.min(sTable.getTableRowCount() - totalExported,
							intPageCount);
				}
				String pagesql;
				
				if (config.targetIsCSV()) {
					pagesql = graphExHelper.getPagedSelectSQL(vertex, sql, realPageCount, totalExported, pk);
				} else {
					pagesql = graphExHelper.getPagedSelectSQL(sql, realPageCount, totalExported, pk);
				}
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("[SQL]PAGINATED=" + pagesql);
				}
				
				long recordCountOfQuery = 0L;
				
				if (config.isCdc()) {
					recordCountOfQuery = cdcHandleRecord(conn, pagesql, vertex, sTable,
							records, newRecordProcessor);
					totalExported = totalExported + recordCountOfQuery;
				} else {
					recordCountOfQuery = graphHandleSQL(conn, pagesql, vertex, sTable,
							records, newRecordProcessor);
					totalExported = totalExported + recordCountOfQuery;
				}
				
				//Stop fetching condition: no result;less then fetching count;great then total count
				if (isLatestPage(sTable, totalExported, recordCountOfQuery)) {
					break;
				}
			}
			if (!records.isEmpty()) {
				newRecordProcessor.processRecords(vertex.getVertexLabel(), records);
			}
		} finally {
			newRecordProcessor.endExportTable(vertex.getVertexLabel());
			connManager.closeSrc(conn);
		}
		
	}
}
