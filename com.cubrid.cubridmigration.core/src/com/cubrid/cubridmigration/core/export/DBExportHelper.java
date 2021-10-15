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
package com.cubrid.cubridmigration.core.export;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.IDependOnDatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.export.handler.BlobTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.BytesTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.CharTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.ClobTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.DateTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.DefaultHandler;
import com.cubrid.cubridmigration.core.export.handler.IntTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.LongBytesTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.NumberTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.TimeTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.TimestampTypeHandler;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.cubrid.export.CUBRIDExportHelper;

/**
 * a class help to export database data and verify database sql statement
 * 
 * @author moulinwang Kevin Cao
 * 
 */
public abstract class DBExportHelper implements
		IDependOnDatabaseType {
	protected static final Logger LOG = LogUtil.getLogger(CUBRIDExportHelper.class);

	protected static final int DEFAULT_FETCH_SIZE = 500;

	public static final int LONGNVARCHAR = -16;
	public static final int NCHAR = -15;
	public static final int NCLOB = 2011;
	public static final int NVARCHAR = -9;

	protected static final String PARTITITON_INDEX = "pidx"; // be same as Exporter.PARTITITON_INDEX
	protected Map<Integer, IExportDataHandler> handlerMap1 = new HashMap<Integer, IExportDataHandler>();
	protected Map<String, IExportDataHandler> handlerMap2 = new HashMap<String, IExportDataHandler>();

	public DBExportHelper() {
		handlerMap1.put(Types.CHAR, new CharTypeHandler());
		handlerMap1.put(Types.VARCHAR, new CharTypeHandler());
		handlerMap1.put(Types.NCHAR, new CharTypeHandler());
		handlerMap1.put(Types.NVARCHAR, new CharTypeHandler());

		handlerMap1.put(Types.BINARY, new BytesTypeHandler());
		handlerMap1.put(Types.VARBINARY, new BytesTypeHandler());

		handlerMap1.put(Types.BIT, new DefaultHandler());
		handlerMap1.put(Types.TINYINT, new IntTypeHandler());
		handlerMap1.put(Types.SMALLINT, new IntTypeHandler());
		handlerMap1.put(Types.INTEGER, new DefaultHandler());
		handlerMap1.put(Types.BIGINT, new DefaultHandler());
		handlerMap1.put(Types.REAL, new DefaultHandler());
		handlerMap1.put(Types.FLOAT, new DefaultHandler());
		handlerMap1.put(Types.DOUBLE, new DefaultHandler());

		handlerMap1.put(Types.DECIMAL, new NumberTypeHandler());
		handlerMap1.put(Types.NUMERIC, new NumberTypeHandler());

		handlerMap1.put(Types.DATE, new DateTypeHandler());
		handlerMap1.put(Types.TIME, new TimeTypeHandler());
		handlerMap1.put(Types.TIMESTAMP, new TimestampTypeHandler());

		handlerMap1.put(Types.LONGVARBINARY, new LongBytesTypeHandler());
		handlerMap1.put(Types.BLOB, new BlobTypeHandler());

		handlerMap1.put(Types.LONGVARCHAR, new ClobTypeHandler());
		handlerMap1.put(Types.LONGNVARCHAR, new ClobTypeHandler());
		handlerMap1.put(Types.CLOB, new ClobTypeHandler());
		handlerMap1.put(Types.NCLOB, new ClobTypeHandler());
	}

	/**
	 * get JDBC Object
	 * 
	 * @param rs ResultSet
	 * @param column Column
	 * @return Object
	 * @throws SQLException e
	 */
	public Object getJdbcObject(final ResultSet rs, final Column column) throws SQLException {
		if (column == null) {
			throw new RuntimeException("Column can't be null.");
		}
		Integer dataTypeID = column.getJdbcIDOfDataType();
		if (dataTypeID != null) {
			IExportDataHandler edh = handlerMap1.get(dataTypeID);
			if (edh == null) {
				return rs.getObject(column.getName());
			}
			return edh.getJdbcObject(rs, column);
		}
		LOG.error("Unknown SQL data type:" + column.getDataType() + "(Column name="
				+ column.getName() + ")");
		return null;
	}

	/**
	 * return database object name
	 * 
	 * @param objectName String
	 * @return String
	 */
	protected abstract String getQuotedObjName(String objectName);

	/**
	 * to return SELECT SQL
	 * 
	 * @param stc SourceTableConfig
	 * @return String
	 */
	public String getSelectSQL(final SourceTableConfig stc) {
		//SQL table
		if (stc instanceof SourceSQLTableConfig) {
			return ((SourceSQLTableConfig) stc).getSql();
		}
		//Entry table 
		SourceEntryTableConfig setc = (SourceEntryTableConfig) stc;
		StringBuffer buf = new StringBuffer(256);
		buf.append("SELECT ");
		final List<SourceColumnConfig> columnList = setc.getColumnConfigList();
		for (int i = 0; i < columnList.size(); i++) {
			if (i > 0) {
				buf.append(',');
			}
			buf.append(getQuotedObjName(columnList.get(i).getName()));
		}
		buf.append(" FROM ");
		// it will make a query with a schema and table name 
		// if it required a schema name when there create sql such as SCOTT.EMP
		addSchemaPrefix(setc, buf);
		buf.append(getQuotedObjName(setc.getName()));

		String condition = setc.getCondition();
		if (StringUtils.isNotBlank(condition)) {
			condition = condition.trim();
			if (!condition.toLowerCase(Locale.US).startsWith("where")) {
				buf.append(" WHERE ");
			}
			if (condition.trim().endsWith(";")) {
				condition = condition.substring(0, condition.length() - 1);
			}
			buf.append(" ").append(condition);
		}
		return buf.toString();
	}

	/**
	 * If add a schema prefix before the table name.
	 * 
	 * @param setc SourceEntryTableConfig
	 * @param buf StringBuffer
	 */
	protected void addSchemaPrefix(SourceEntryTableConfig setc, StringBuffer buf) {
		if (StringUtils.isNotBlank(setc.getOwner())) {
			buf.append(getQuotedObjName(setc.getOwner())).append(".");
		}
	}

	/**
	 * to return SELECT SQL
	 * 
	 * @param setc SourceEntryTableConfig
	 * @return String
	 */
	public String getSelectCountSQL(final SourceEntryTableConfig setc) {
		StringBuffer buf = new StringBuffer(256);
		buf.append("SELECT COUNT(1) ");
		buf.append(" FROM ");
		// it will make a query with a schema and table name 
		// if it required a schema name when there create sql such as SCOTT.EMP
		addSchemaPrefix(setc, buf);
		buf.append(getQuotedObjName(setc.getName()));

		String condition = setc.getCondition();
		if (StringUtils.isNotBlank(condition)) {
			if (!condition.trim().toLowerCase(Locale.US).startsWith("where")) {
				buf.append(" WHERE ");
			}
			condition = condition.trim();
			if (condition.trim().endsWith(";")) {
				condition = condition.substring(0, condition.length() - 1);
			}
			buf.append(" ").append(condition);
		}
		return buf.toString();
	}

	//	/**
	//	 * return database special table name
	//	 * 
	//	 * @param sourceTable Table
	//	 * @return String
	//	 */
	//	public String getTableNameKey(SourceTable sourceTable) {
	//		return sourceTable.getName();
	//	}

	/**
	 * If source DB is online, fill the tableRowCount attribute of source
	 * tables.
	 * 
	 * The attribute may be used by some other function such as monitor and
	 * reporter.
	 * 
	 * It is not recommended to call this method often because of it may take a
	 * long time.
	 * 
	 * @param config MigrationConfiguration
	 */
	public void fillTablesRowCount(MigrationConfiguration config) {
		if (!config.sourceIsOnline()) {
			return;
		}
		if (config.isImplicitEstimate()) {
			setAllTableRowCountTo0(config);
			return;
		}
		try {
			Connection con = config.getSourceConParams().createConnection();
			if (con == null) {
				return;
			}
			Statement stmt = con.createStatement();
			if (stmt == null) {
				return;
			}

			try {
				for (SourceEntryTableConfig setc : config.getExpEntryTableCfg()) {
					if (!setc.isMigrateData()) {
						continue;
					}
					// It will put an owner name from Table object 
					// whenever counting total records
					// because SourceEntryTableConfig.setOwner() is volatility.
					Table tbl = config.getSrcTableSchema(setc.getOwner(), setc.getName());
					if (tbl != null && StringUtils.isNotEmpty(tbl.getOwner())) {
						setc.setOwner(tbl.getOwner());
					}
					String sql = getSelectCountSQL(setc);
					setTableRowCount(config, stmt, setc.getOwner(), setc.getName(), sql);
				}
				SQLHelper sqlHelper = config.getSourceDBType().getSQLHelper(null);
				for (SourceSQLTableConfig sstc : config.getExpSQLCfg()) {
					if (!sstc.isMigrateData()) {
						continue;
					}
					String cleanSQL = cleanSQLTableSQL(sstc);
					String executableSQL = sqlHelper.replacePageQueryParameters(cleanSQL,
							Long.MAX_VALUE, 0);

					boolean useDerivedQuery = true;

					String trimSql = executableSQL.toLowerCase();
					int sp = trimSql.indexOf("select");
					int ep = trimSql.indexOf("from");
					int lmt = trimSql.indexOf("limit");
					if (sp != -1 && ep != -1 && lmt == -1) {
						sp += "select".length();
						String pre = executableSQL.substring(0, sp);
						String post = executableSQL.substring(ep, executableSQL.length());
						String finalsql = pre + " COUNT(1) " + post;
						if (setTableRowCount(config, stmt, sstc.getOwner(), sstc.getName(),
								finalsql)) {
							useDerivedQuery = false;
						}
					}

					if (useDerivedQuery) {
						executableSQL = "SELECT COUNT(1) FROM (" + executableSQL + ") tbl";
						setTableRowCount(config, stmt, sstc.getOwner(), sstc.getName(),
								executableSQL);
					}
				}
			} finally {
				stmt.close();
				con.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param sstc SourceSQLTableConfig
	 * @return cleaned SQL
	 */
	protected String cleanSQLTableSQL(SourceSQLTableConfig sstc) {
		String cleanSQL = sstc.getSql().trim();
		if (cleanSQL.endsWith(";")) {
			cleanSQL = new StringBuffer(cleanSQL).deleteCharAt(cleanSQL.length() - 1).toString();
			sstc.setSql(cleanSQL);
		}
		return cleanSQL;
	}

	/**
	 * @param config MigrationConfiguration
	 */
	protected void setAllTableRowCountTo0(MigrationConfiguration config) {
		for (SourceEntryTableConfig setc : config.getExpEntryTableCfg()) {
			if (!setc.isMigrateData()) {
				continue;
			}
			Table tbl = config.getSrcTableSchema(setc.getOwner(), setc.getName());
			if (tbl == null) {
				continue;
			}
			tbl.setTableRowCount(0);
		}

		for (SourceSQLTableConfig sstc : config.getExpSQLCfg()) {
			if (!sstc.isMigrateData()) {
				continue;
			}
			Table tbl = config.getSrcTableSchema(sstc.getOwner(), sstc.getName());
			if (tbl == null) {
				continue;
			}
			tbl.setTableRowCount(0);
		}
	}

	/**
	 * Set a table's row count
	 * 
	 * @param config MigrationConfiguration
	 * @param stat Statement
	 * @param schema table's schema name
	 * @param stname String
	 * @param sql String
	 * 
	 * @return true if setting successfully
	 */
	private boolean setTableRowCount(MigrationConfiguration config, Statement stat, String schema,
			String stname, String sql) {
		Table tbl = config.getSrcTableSchema(schema, stname);
		if (tbl == null) {
			return false;
		}

		try {
			ResultSet rs = stat.executeQuery(sql); //NOPMD
			try {
				if (rs.next()) {
					tbl.setTableRowCount(rs.getLong(1));
				} else {
					tbl.setTableRowCount(0);
				}
			} finally {
				rs.close();
			}
		} catch (SQLException e) {
			LOG.error("SQL error", e);
			return false;
		}

		return true;
	}

	/**
	 * config Statement Object
	 * 
	 * @param stmt Statement
	 */
	public void configStatement(Statement stmt) {
		try {
			stmt.setFetchSize(DEFAULT_FETCH_SIZE);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	//	public abstract String getPagedSelectSQL(final Table sourceTable,
	//			final List<SourceColumnConfig> columnList, int rows,
	//			long exportedRecords);

	/**
	 * Retrieves the SQL with page condition
	 * 
	 * @param sql to be change
	 * @param pageSize record count per-page
	 * @param exportedRecords record count to be skipped.
	 * @param pk table's primary key
	 * @return SQL
	 */
	public abstract String getPagedSelectSQL(String sql, long pageSize, long exportedRecords, PK pk);

	/**
	 * Is support fast search with PK.
	 * 
	 * @param conn Connection
	 * @return true or false
	 */
	public boolean supportFastSearchWithPK(Connection conn) {
		return false;
	}

	/**
	 * Retrieves the current value of input serial.
	 * 
	 * @param sourceConParams JDBC connection configuration
	 * @param sq sequence to be synchronized.
	 * @return The current value of the input SQ
	 */
	public BigInteger getSerialStartValue(ConnParameters sourceConParams, SourceSequenceConfig sq) {
		return null;
	}

}
