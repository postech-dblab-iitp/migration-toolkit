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
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.RecordExportedListener;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDSQLHelper;

/**
 * CUBRIDJDBCExporter: export data by paging.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-9-24 created by Kevin Cao
 */
public class CUBRIDJDBCExporter extends
		JDBCExporter {
	/**
	 * 
	 * NavRSStatus: The result of navigating the selection result set.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2014-1-16 created by Kevin Cao
	 */
	private static class ExportingStatus {
		private long totalTobeExported;
		private long totalExported;
		private int rsSize;
		//NULL means starting from head.
		private Object[] startIDValueForNextFetching;
		private Object[] endIDValue;
		private Object startIDFromTarget;

		public Object getStartIDFromTarget() {
			return startIDFromTarget;
		}

		public void setStartIDFromTarget(Object startIDFromTarget) {
			this.startIDFromTarget = startIDFromTarget;
		}

		/**
		 * Set current result size and append total exported records count.
		 * 
		 * @param rsSize current result set size.
		 */
		public void changeRsSizeAndCountTotal(int rsSize) {
			setRsSize(rsSize);
			totalExported = totalExported + rsSize;
		}

		public Object[] getStartIDValueForNextFetching() {
			return startIDValueForNextFetching;
		}

		public int getRsSize() {
			return rsSize;
		}

		public long getTotalExported() {
			return totalExported;
		}

		public void setStartIDValueForNextFetching(Object[] currentIDValue) {
			this.startIDValueForNextFetching = currentIDValue;
		}

		public void setRsSize(int rsSize) {
			this.rsSize = rsSize;
		}

		public Object[] getEndIDValue() {
			return endIDValue;
		}

		public void setEndIDValue(Object[] endIDValue) {
			this.endIDValue = endIDValue;
		}

		public long getTotalTobeExported() {
			return totalTobeExported;
		}

		public void setTotalTobeExported(long totalTobeExported) {
			this.totalTobeExported = totalTobeExported;
		}

		//		public void setTotalExported(long totalExported) {
		//			this.totalExported = totalExported;
		//		}
	}

	private CUBRIDSQLHelper cubSQLHelper = CUBRIDSQLHelper.getInstance(null);

	/**
	 * If the source cubrid supports the special exporting.
	 * 
	 * @param srcPK source PK
	 * @return true if support PK exporting
	 */
	private boolean isSupportSpecialExporting(PK srcPK) {
		Driver driver = config.getSourceConParams().getDriver();
		if (driver == null) {
			throw new IllegalArgumentException("Invalid input connection parameters.");
		}
		if (srcPK == null || srcPK.getPkColumns().isEmpty()) {
			return false;
		}
		if (srcPK.getPkColumns().size() == 1) {
			return true;
		}
		int majorVersion = driver.getMajorVersion();
		int minorVersion = driver.getMinorVersion();
		//only 8.4.1 or later will be supported.
		if (majorVersion < 8 || (majorVersion == 8 && minorVersion < 4)) {
			return false;
		}

		return true;
	}

	/**
	 * Export source data records by paging query
	 * 
	 * @param stc SourceTableConfig
	 * @param newRecsHandler RecordExportedListener
	 */
	public void exportTableRecords(SourceTableConfig stc, RecordExportedListener newRecsHandler) {
		Table sTable = config.getSrcTableSchema(stc.getOwner(), stc.getName());
		if (sTable == null) {
			throw new NormalMigrationException("Table " + stc.getName() + " was not found.");
		}
		final PK srcPK = sTable.getPk();
		//Special table has special exporting logic.
		if (stc instanceof SourceEntryTableConfig) {
			SourceEntryTableConfig setc = (SourceEntryTableConfig) stc;
			//			if (StringUtils.isNotBlank(setc.getExpOptColumn())) {
			//				exportWithSpecialCols(setc, newRecsHandler,
			//						new String[]{setc.getExpOptColumn() });
			//				return;
			//			}
			//Entry table exporting, not SQL table and the source table has a single-column PK.
			if (isSupportSpecialExporting(srcPK) && setc.isEnableExpOpt()) {
				exportWithSpecialCols(setc, newRecsHandler,
						srcPK.getPkColumns().toArray(new String[] {}));
				return;
			}
		}

		super.exportTableRecords(stc, newRecsHandler);
	}

	/**
	 * Export source data records by paging query with special exporting
	 * optimization.
	 * 
	 * @param setc SourceEntryTableConfig
	 * @param newRecsHandler RecordExportedListener
	 * @param spCols a special column to help speed up paging fetching.
	 */
	private void exportWithSpecialCols(SourceEntryTableConfig setc,
			RecordExportedListener newRecsHandler, String[] spCols) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[Method in]exportTableRecordsWithSpecialColumn(" + spCols + ")");
		}
		Table sTable = config.getSrcTableSchema(setc.getOwner(), setc.getName());
		if (sTable == null) {
			throw new NormalMigrationException("Table " + setc.getName() + " was not found.");
		}
		Connection conn = connManager.getSourceConnection(); //NOPMD
		try {
			newRecsHandler.startExportTable(setc.getName());
			List<Record> records = new ArrayList<Record>();
			List<SourceColumnConfig> expColConfs = setc.getColumnConfigList();

			String startSQL = getPagingSQL(setc, spCols, true);
			String noStartSQL = getPagingSQL(setc, spCols, false);

			ExportingStatus expStatus = new ExportingStatus();
			initExportingStatus(conn, setc, spCols, expStatus);
			JDBCObjContainer joc = new JDBCObjContainer();
			joc.setConn(conn);
			boolean isFirstQuery = !(isStartFromTargetMax(setc) && expStatus.getStartIDFromTarget() != null);
			while (true) {
				if (interrupted) {
					return;
				}
				//Define which SQL should be executed.
				//The startSQL only be executed at the first time.
				final long pageSize = Math.min(
						expStatus.totalTobeExported - expStatus.getTotalExported(),
						config.getPageFetchCount());
				String finalSQL;
				if (isFirstQuery) {
					finalSQL = startSQL;
					isFirstQuery = false;
				} else {
					finalSQL = noStartSQL;
				}
				final Object[] startValues = expStatus.getStartIDValueForNextFetching();
				List<Object> params = getParams(startValues);
				params.add(pageSize);
				getResultSet(finalSQL, params.toArray(new Object[] {}), joc);
				//Start handle result set.
				handleResultSet(joc, setc, sTable, expColConfs, records, newRecsHandler, expStatus,
						spCols);
				//If end fetching.
				if (isEndFetching(expStatus)) {
					break;
				}
			}
			if (!records.isEmpty()) {
				newRecsHandler.processRecords(setc.getName(), records);
			}
		} finally {
			newRecsHandler.endExportTable(setc.getName());
			connManager.closeSrc(conn);
		}
	}

	/**
	 * Stop fetching condition: no result;less then fetching count;great then
	 * total count
	 * 
	 * @param expStatus ExportingStatus
	 * @return true retrieved if fetching should be ended.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private boolean isEndFetching(ExportingStatus expStatus) {
		//No more records fetched.
		if (expStatus.getRsSize() == 0) {
			return true;
		}
		//Next fetching result may be empty.
		if (expStatus.getRsSize() < config.getPageFetchCount()) {
			return true;
		}
		//Total size is matched.
		if (expStatus.getTotalExported() >= expStatus.getTotalTobeExported()) {
			return true;
		}
		//ID value is greater than max ID value
		if (expStatus.getEndIDValue() != null && expStatus.getEndIDValue().length == 1) {
			Object oEnd = expStatus.getEndIDValue()[0];
			Object oStart = expStatus.getStartIDValueForNextFetching()[0];
			if (oEnd instanceof Comparable && oStart instanceof Comparable) {
				Comparable c1 = (Comparable) oStart;
				Comparable c2 = (Comparable) oEnd;
				return c1.compareTo(c2) >= 0;
			}
		}
		return false;
	}

	/**
	 * Initialize the exporting status before exporting start, including
	 * fetching minimum/maxim/total count of source table.
	 * 
	 * @param conn Source DB JDBC connection
	 * @param setc Migration entry table configuration
	 * @param spCols the column will be used into optimization.
	 * @param es Exporting status object to be initialized.
	 */
	private void initExportingStatus(Connection conn, SourceEntryTableConfig setc, String[] spCols,
			ExportingStatus es) {
		StringBuffer sql = new StringBuffer();
		String[] qtCols = getQuotedCols(spCols);
		sql.append("SELECT ");
		for (int i = 0; i < qtCols.length; i++) {
			sql.append(" MIN(").append(qtCols[i]).append(") AS M").append(i).append(", MAX(").append(
					qtCols[i]).append(")AS A").append(i).append(",");
		}
		sql.append(" COUNT(*)AS C FROM \"").append(setc.getName() + "\" ");
		if (StringUtils.isNotBlank(setc.getCondition())) {
			if (!setc.getCondition().trim().toLowerCase(Locale.US).startsWith("WHERE")) {
				sql.append("WHERE");
			}
			sql.append(" ").append(setc.getCondition());
		}
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql.toString());
			if (rs.next()) {
				Object[] startValues = new Object[spCols.length];
				Object[] endValues = new Object[spCols.length];
				for (int i = 0; i < spCols.length; i++) {
					startValues[i] = rs.getObject("M" + i);
					endValues[i] = rs.getObject("A" + i);
				}
				es.setStartIDValueForNextFetching(startValues);
				es.setEndIDValue(endValues);
				es.setTotalTobeExported(rs.getLong("C"));
			}
		} catch (SQLException ex) {
			LOG.error("Error: fetching maxim value of " + setc.getName() + ".", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		if (isStartFromTargetMax(setc)) {
			final Object startIDFromTarget = getStartIDFromTarget(setc, spCols);
			es.setStartIDFromTarget(startIDFromTarget);
			if (startIDFromTarget != null) {
				es.setStartIDValueForNextFetching(new Object[] {startIDFromTarget});
			}
		}
	}

	/**
	 * Is start from target max ID value.
	 * 
	 * @param setc SourceEntryTableConfig
	 * @return true if start from target table's maxim ID column's value.
	 */
	private boolean isStartFromTargetMax(SourceEntryTableConfig setc) {
		return !setc.isCreateNewTable() && !setc.isReplace() && setc.isStartFromTargetMax();
	}

	/**
	 * Retrieves the selection SQL statement which has special column to be
	 * indexed. The SQL has an ordered statement and a parameter as start point.
	 * For example: select f1,f2 from test where f1>? order by f1 for
	 * orderby_num() between 1 and 100.
	 * 
	 * @param setc SourceEntryTableConfig
	 * @param spCols PK column or indexed column.
	 * @param isFirstPage If it is fetching first page.
	 * @return Selection SQL statement with paging.
	 */
	private String getPagingSQL(SourceEntryTableConfig setc, String[] spCols, boolean isFirstPage) {
		final DBExportHelper expHelper = getSrcDBExportHelper();
		StringBuffer sql = new StringBuffer(expHelper.getSelectSQL(setc));
		final String[] quotedObjNames = getQuotedCols(spCols);
		if (StringUtils.isBlank(setc.getCondition())) {
			sql.append(" WHERE (");
		} else {
			sql.append(" AND (");
		}
		sql.append(getPageCondition(quotedObjNames, isFirstPage));
		sql.append(") ORDER BY ");
		for (int i = 0; i < spCols.length; i++) {
			if (i > 0) {
				sql.append(",");
			}
			sql.append(quotedObjNames[i]);
		}
		sql.append(" FOR ORDERBY_NUM() BETWEEN 1 AND ?");
		return sql.toString();
	}

	/**
	 * Retrieves the quoted columns
	 * 
	 * @param spCols input columns
	 * @return quoted columns
	 */
	private String[] getQuotedCols(String[] spCols) {
		final String[] quotedObjNames = new String[spCols.length];
		for (int i = 0; i < spCols.length; i++) {
			quotedObjNames[i] = cubSQLHelper.getQuotedObjName(spCols[i]);
		}
		return quotedObjNames;
	}

	/**
	 * Retrieves the target table specified column's maxim value. Only
	 * single-column primary key is supported.
	 * 
	 * @param setc SourceEntryTableConfig
	 * @param spCols special columns.
	 * @return Object and may be NULL.
	 */
	private Object getStartIDFromTarget(SourceEntryTableConfig setc, String[] spCols) {
		if (spCols != null && spCols.length == 1 && config.targetIsOnline()
				&& isStartFromTargetMax(setc)) {
			Connection conn = null;
			Statement stmt = null;
			ResultSet rs = null;
			try {
				conn = config.getTargetConParams().createConnection();
				stmt = conn.createStatement();
				final SourceColumnConfig columnConfig = setc.getColumnConfig(spCols[0]);
				if (columnConfig == null) {
					return null;
				}
				rs = stmt.executeQuery("SELECT MAX(\"" + columnConfig.getTarget() + "\") FROM \""
						+ setc.getTarget() + "\"");
				if (rs.next()) {
					return rs.getObject(1);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Closer.close(rs);
				Closer.close(stmt);
				Closer.close(conn);
			}
		}
		return null;
	}

	/**
	 * Execute the selection SQL and handle the result set.
	 * 
	 * @param joc JDBCObjContainer
	 * @param stc SourceTableConfig
	 * @param sTable Source Table
	 * @param expColConfs List<SourceColumnConfig> of Source Table
	 * @param records data cache
	 * @param newRecordProcessor processor
	 * @param expStatus ExportingStatus. In method, some properties will be
	 *        changed.
	 * @param spCols special columns
	 */
	private void handleResultSet(JDBCObjContainer joc, SourceEntryTableConfig stc, Table sTable,
			List<SourceColumnConfig> expColConfs, List<Record> records,
			RecordExportedListener newRecordProcessor, ExportingStatus expStatus, String[] spCols) {
		try {
			ResultSet rs = joc.getRs();
			if (rs == null) {
				expStatus.setRsSize(0);
				return;
			}
			int thisTimeRecords = 0;
			//The last ID column's value will be start point of next fetching. 
			Object[] currentIDValue = new Object[spCols.length];
			while (nextRecord(rs)) {
				if (interrupted) {
					expStatus.changeRsSizeAndCountTotal(thisTimeRecords);
					expStatus.setStartIDValueForNextFetching(currentIDValue);
					return;
				}
				for (int i = 0; i < spCols.length; i++) {
					try {
						currentIDValue[i] = rs.getObject(spCols[i]);
					} catch (SQLException e) {
						throw new NormalMigrationException("Get current ID error.", e);
					}
				}

				thisTimeRecords++;
				Record record = createNewRecord(sTable, expColConfs, rs);
				if (record == null) {
					continue;
				}
				records.add(record);
				handleCommit(stc, newRecordProcessor, sTable, records);
			}
			expStatus.changeRsSizeAndCountTotal(thisTimeRecords);
			expStatus.setStartIDValueForNextFetching(currentIDValue);
		} finally {
			Closer.close(joc.getRs());
			Closer.close(joc.getStmt());
		}
	}

	/**
	 * Get page condition
	 * 
	 * @param columns column names
	 * @param isFirst if it is the first query.
	 * @return condition SQL
	 */
	public static String getPageCondition(String[] columns, boolean isFirst) {
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < columns.length; i++) {
			String sy;
			if (i == (columns.length - 1)) {
				if (isFirst) {
					sy = ">=";
				} else {
					sy = ">";
				}
			} else {
				sy = "=";
			}
			if (result.length() > 0) {
				result.append(" AND ");
			} else {
				result.append("(");
			}
			result.append(columns[i]).append(sy).append("?");
		}
		result.append(")");
		if (columns.length > 1) {
			result.insert(0, getPageCondition(Arrays.copyOf(columns, columns.length - 1), isFirst)
					+ " OR ");
		}
		return result.toString();
	}

	/**
	 * Page query parameters
	 * 
	 * @param values of the Key columns
	 * @return parameters to be set
	 */
	public static List<Object> getParams(Object[] values) {
		List<Object> result = new ArrayList<Object>();
		for (int i = 0; i < values.length; i++) {
			result.add(values[i]);
		}
		if (values.length > 1) {
			result.addAll(0, getParams(Arrays.copyOf(values, values.length - 1)));
		}
		return result;
	}
}
