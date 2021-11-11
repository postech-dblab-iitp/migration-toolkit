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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
		        ||  sourceDBTypeID == DatabaseType.MYSQL.getID())) {
			return true;
		}
		
		return recordCountOfCurrentPage == 0
				|| recordCountOfCurrentPage < config.getPageFetchCount()
				|| (!config.isImplicitEstimate() && exportedRecords >= sTable.getTableRowCount());
	}
}
