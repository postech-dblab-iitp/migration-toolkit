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
package com.cubrid.cubridmigration.mysql.meta;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.DBUtils;
import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IBuildSchemaFilter;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObjectFactory;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.PartitionTable;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.dbobject.Version;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.mysql.MySQLDataTypeHelper;
import com.cubrid.cubridmigration.mysql.dbobj.MySQLTrigger;

/**
 * 
 * ReverseEngineeringMysqlJdbc
 * 
 * @author moulinwang
 * @author Jessie Huang
 * @version 1.0 - 2009-9-15
 */
public final class MySQLSchemaFetcher extends
		AbstractJDBCSchemaFetcher {
	private final static Logger LOG = LogUtil.getLogger(MySQLSchemaFetcher.class);

	private static final String SHOW_AUTOINCREMENT_MAXVAL = "SHOW TABLE STATUS LIKE ?";
	private static final String SHOW_CHARSET = "show VARIABLES LIKE 'character_set_database'";
	private static final String SHOW_DB = "SHOW CREATE DATABASE ";

	private static final String SHOW_FUNCTION = "SHOW CREATE FUNCTION ";
	private static final String SHOW_PROCEDURE = "SHOW CREATE PROCEDURE ";
	private static final String SHOW_TABLE = "SHOW CREATE TABLE ";
	private static final String SHOW_VIEW = "SHOW CREATE VIEW ";

	//private static final String SCHEMA_SELECT = "SHOW DATABASES";

	/**
	 * get db prop info
	 * 
	 * @param conParams ConnectionParameters
	 * @return Map<String, String> @ e
	 */
	public static Map<String, String> getDatabaseProperties(
			ConnParameters conParams) {
		try {
			Map<String, String> databaseProperties = new HashMap<String, String>();
			Driver driver = conParams.getDriver();
			if (driver == null) {
				throw new RuntimeException("JDBC driver can't be null.");
			}
			Properties prop = new Properties();
			prop.put("user", conParams.getConUser());
			prop.put("password", conParams.getConPassword());
			DriverPropertyInfo[] info = driver.getPropertyInfo(
					conParams.getUrl(), prop);

			for (DriverPropertyInfo driverInfo : info) {
				databaseProperties.put(driverInfo.name, driverInfo.value);
			}

			return databaseProperties;
		} catch (Exception e) {
			LOG.error(LogUtil.getExceptionString(e));
			throw new RuntimeException(e);
		}
	}

	public MySQLSchemaFetcher() {
		factory = new DBObjectFactory() {

			public Trigger createTrigger() {
				return new MySQLTrigger();
			}

		};
	}

	//	/**
	//	 * Returns a list of all schemata from the given JDBC connection
	//	 * 
	//	 * @param conn Connection
	//	 * @return returns a GRT XML string containing a list of schemata names @ e
	//	 */
	//	public List<String> getSchemata(final Connection conn) {
	//		Statement stmt = null; // NOPMD
	//		ResultSet rs = null; // NOPMD
	//		try {
	//			final List<String> schemataList = new ArrayList<String>();
	//
	//			stmt = conn.createStatement();
	//			rs = stmt.executeQuery(SCHEMA_SELECT);
	//
	//			while (rs.next()) {
	//				schemataList.add(rs.getString(1));
	//			}
	//
	//			return schemataList;
	//		} catch (SQLException e) {
	//			throw new RuntimeException(e);
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//		}
	//	}

	//	/**
	//	 * get db char set
	//	 * 
	//	 * @param conn Connection
	//	 * @param dbName String
	//	 * @return db charSet
	//	 * @throws SQLException e
	//	 */
	//	public String getCharSet(final Connection conn, final String dbName) throws SQLException {
	//		if (dbName == null || dbName.trim().equals("")) {
	//			throw new IllegalArgumentException("The database name is null!");
	//		}
	//
	//		PreparedStatement stmt = null; // NOPMD
	//		ResultSet rs = null; // NOPMD
	//		try {
	//			final String sqlStr = SHOW_DB + getQuoteStr(dbName);
	//			stmt = conn.prepareStatement(sqlStr);
	//			rs = stmt.executeQuery();
	//
	//			String databaseDDL = null;
	//
	//			if (rs.next()) {
	//				databaseDDL = rs.getString(2);
	//			}
	//
	//			return getCharset(databaseDDL);
	//
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//		}
	//	}

	/**
	 * buildCatalog
	 * 
	 * @param conn Connection
	 * @param cp String
	 * @param filter IBuildSchemaFilter
	 * 
	 * @return Catalog
	 * @throws SQLException e
	 */
	public Catalog buildCatalog(final Connection conn, ConnParameters cp,
			IBuildSchemaFilter filter) throws SQLException {
		final Catalog catalog = super.buildCatalog(conn, cp, filter);

		final String charset = getCharSetByDBVariables(conn);
		catalog.setCharset(charset);
		catalog.setDatabaseType(DatabaseType.MYSQL);

		final String dbDDL = getDBDDL(conn, catalog.getName());
		catalog.setCreateSql(dbDDL);

		final List<Schema> schemaList = catalog.getSchemas();
		final SQLHelper sqlHelper = cp.getDatabaseType().getSQLHelper(null);
		for (Schema schema : schemaList) {
			// get tables
			final List<Table> tableList = schema.getTables();

			for (Table table : tableList) {
				table.setDDL(getTableDDL(conn, table.getName()));
			}

			// get views
			final List<View> viewList = schema.getViews();

			for (View view : viewList) {
				view.setDDL(getViewDDL(conn, view.getName()));
				view.setQuerySpec(sqlHelper.getViewQuerySpec(view.getDDL()));
			}
		}

		catalog.setTimezone(getTimezone(conn));

		// get partitions
		buildPartitions(conn, catalog, catalog.getSchemas().get(0));

		return catalog;
	}

	//	/**
	//	 * getCharset
	//	 * 
	//	 * @param databaseDDL String
	//	 * @return database Charset
	//	 */
	//	public static String getCharset(String databaseDDL) {
	//		String patternCharset = "CREATE DATABASE .* DEFAULT CHARACTER SET (.*) ..";
	//		Pattern pattern = Pattern.compile(patternCharset);
	//		Matcher matcher = pattern.matcher(databaseDDL);
	//		boolean matchFound = matcher.find();
	//
	//		if (matchFound) {
	//			return matcher.group(1);
	//		}
	//
	//		return null;
	//	}

	/**
	 * build Partitions MySQL 5.1 support Partition
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @throws SQLException e
	 */
	protected void buildPartitions(final Connection conn,
			final Catalog catalog, final Schema schema) throws SQLException {
		Version version = catalog.getVersion();

		if (!isSupportParitionVersion(version)) {
			return;
		}

		String sqlStr = "SELECT * FROM INFORMATION_SCHEMA.PARTITIONS "
				+ "WHERE TABLE_SCHEMA=? AND PARTITION_NAME IS NOT NULL "
				+ "ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION";

		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD
		try {
			stmt = conn.prepareStatement(sqlStr);
			stmt.setString(1, schema.getName());
			rs = stmt.executeQuery();

			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");

				String partitionName = rs.getString("PARTITION_NAME");
				String partitionMethod = rs.getString("PARTITION_METHOD");
				String partitionExp = rs.getString("PARTITION_EXPRESSION");
				int partitionPosition = rs.getInt("PARTITION_ORDINAL_POSITION");
				String partitionDesc = rs.getString("PARTITION_DESCRIPTION");

				String subPartitionName = rs.getString("SUBPARTITION_NAME");
				String subPartitionMethod = rs.getString("SUBPARTITION_METHOD");
				String subPartitionExp = rs.getString("SUBPARTITION_EXPRESSION");
				int subpartitionPosition = rs.getInt("SUBPARTITION_ORDINAL_POSITION");

				Table table = schema.getTableByName(tableName);

				if (table == null) {
					continue;
				}

				PartitionInfo partitionInfo = table.getPartitionInfo();

				if (partitionInfo == null) {
					partitionInfo = factory.createPartitionInfo();
					table.setPartitionInfo(partitionInfo);
				}

				partitionInfo.setPartitionMethod(partitionMethod);
				partitionInfo.setPartitionExp(partitionExp);
				partitionInfo.setPartitionFunc(DBUtils.parsePartitionFunc(partitionExp));
				partitionInfo.setPartitionColumns(DBUtils.parsePartitionColumns(
						table, partitionExp));

				partitionInfo.setSubPartitionMethod(subPartitionMethod);
				partitionInfo.setSubPartitionExp(subPartitionExp);
				partitionInfo.setSubPartitionFunc(DBUtils.parsePartitionFunc(subPartitionExp));

				PartitionTable partitionTable = factory.createPartitionTable();
				partitionTable.setPartitionName(partitionName);
				partitionTable.setPartitionDesc(partitionDesc);
				partitionTable.setPartitionIdx(partitionPosition);

				PartitionTable subPartitionTable = factory.createPartitionTable();
				subPartitionTable.setPartitionName(subPartitionName);
				subPartitionTable.setPartitionDesc(partitionDesc);
				subPartitionTable.setPartitionIdx(subpartitionPosition);

				partitionInfo.addPartition(partitionTable);
				partitionInfo.addSubPartition(subPartitionTable);

				partitionInfo.setDDL(getSourcePartitionDDL(table));
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * Fetch all stored procedures of the given schemata.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	protected void buildProcedures(Connection conn, Catalog catalog,
			Schema schema, IBuildSchemaFilter filter) throws SQLException {
		Version version = catalog.getVersion();

		if (version.getDbMajorVersion() < 5) {
			return;
		}

		List<Schema> schemaList = catalog.getSchemas();

		for (Schema sc : schemaList) {
			// get procedures
			List<Procedure> procList = getAllProcedures(conn, catalog.getName());
			sc.setProcedures(procList);

			// get functions
			List<Function> funcList = getAllFunctions(conn, catalog.getName());
			sc.setFunctions(funcList);
		}
	}

	/**
	 * getSQLTable
	 * 
	 * @param resultSetMeta ResultSetMetaData
	 * 
	 * @return SourceTable
	 * @throws SQLException e
	 */
	public Table buildSQLTable(ResultSetMetaData resultSetMeta) throws SQLException {
		Table sourceTable = super.buildSQLTable(resultSetMeta);
		List<Column> columns = sourceTable.getColumns();
		MySQLDataTypeHelper dtHelper = MySQLDataTypeHelper.getInstance(null);
		for (Column column : columns) {
			if (isNULLType(column.getDataType())) {
				column.setDataType("varchar");
				column.setJdbcIDOfDataType(Types.VARCHAR);
			}
			String dataType = column.getDataType().toLowerCase(Locale.US);

			if ("integer".equals(dataType)) {
				dataType = "int";
			} else if ("integer unsigned".equals(dataType)) {
				dataType = "int unsigned";
			}
			column.setDataType(dataType);
			column.setShownDataType(dtHelper.getShownDataType(column));
			if (dtHelper.isEnum(dataType)) {
				DataTypeInstance dti = dtHelper.parseDTInstance(column.getShownDataType());
				column.setDataTypeInstance(dti);
			}
		}

		return sourceTable;
	}

	/**
	 * get meta data and build table columns
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table return
	 * @throws SQLException e
	 */
	protected void buildTableColumns(final Connection conn,
			final Catalog catalog, final Schema schema, final Table table) throws SQLException {
		super.buildTableColumns(conn, catalog, schema, table);

		// get auto increment max value
		final Long nextVal = getAutoIncNextValByTableName(conn, table.getName());

		String sqlStr = "DESC " + getQuoteStr(table.getName());

		ResultSet rs = null; // NOPMD
		PreparedStatement stmt = null; // NOPMD
		try {
			stmt = conn.prepareStatement(sqlStr);
			rs = stmt.executeQuery();
			MySQLDataTypeHelper dtHelper = MySQLDataTypeHelper.getInstance(null);
			while (rs.next()) {
				final String columnName = rs.getString("FIELD");
				final String columnType = rs.getString("TYPE");

				final Column column = table.getColumnByName(columnName);

				if (column != null && columnType != null) {
					column.setShownDataType(columnType);
					if (dtHelper.isEnum(column.getDataType())) {
						DataTypeInstance dti = dtHelper.parseDTInstance(column.getShownDataType());
						column.setDataTypeInstance(dti);
					}
				}

				if (column != null && nextVal != null
						&& column.isAutoIncrement()) {
					column.setAutoIncSeedVal(nextVal);
				}
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		final Version version = catalog.getVersion();

		if (version.getDbMajorVersion() >= 5) {
			sqlStr = "SELECT COLUMN_NAME, CHARACTER_SET_NAME "
					+ "FROM INFORMATION_SCHEMA.COLUMNS "
					+ "WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";

			try {
				stmt = conn.prepareStatement(sqlStr);
				stmt.setString(1, schema.getName());
				stmt.setString(2, table.getName());
				rs = stmt.executeQuery();

				while (rs.next()) {
					final String columnName = rs.getString(1);
					final String charset = rs.getString(2);

					final Column column = table.getColumnByName(columnName);

					if (column != null && charset != null) {
						column.setCharset(charset);
					}
				}
			} finally {
				Closer.close(rs);
				Closer.close(stmt);
			}
		}

		final List<Column> list = table.getColumns();

		//In mysql ,the JDBC driver will recongnize the tinyint(1) as bit(1)
		for (Column column : list) {
			if ("BIT".equalsIgnoreCase(column.getDataType())) {
				if (column.getShownDataType().startsWith("bit")) {
					column.setDataType("bit");
				} else {
					column.setDataType("tinyint");
					column.setJdbcIDOfDataType(Types.TINYINT);
				}
			}
		}

	}

	/**
	 * build Table's Indexes
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableIndexes(final Connection conn,
			final Catalog catalog, final Schema schema, final Table table) throws SQLException {
		super.buildTableIndexes(conn, catalog, schema, table);
		Statement stmt = null; //NOPMD
		ResultSet rs = null; //NOPMD
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SHOW INDEX FROM "
					+ getQuoteStr(table.getName()));

			while (rs.next()) {
				String indexName = rs.getString("KEY_NAME");
				//String indexType = rs.getString("INDEX_TYPE");
				// filter duplicate key_name
				FK fk = table.getFKByName(indexName);
				if (fk != null) {
					table.removeIndex(indexName);
					continue;
				}

				Index index = table.getIndexByName(indexName);
				if (index == null) {
					continue;
				}
			}

		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		// remove duplicate PK index
		final PK pk = table.getPk();
		if (pk == null) {
			return;
		}
		table.removeIndex(pk.getName());
		setUniquColumnByIndex(table);
	}

	/**
	 * Fetch all stored Triggers of the given schemata.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	protected void buildTriggers(Connection conn, Catalog catalog,
			Schema schema, IBuildSchemaFilter filter) throws SQLException {
		Version version = catalog.getVersion();

		if (version.getDbMajorVersion() < 5) { // 5.0.2 support trigger
			return;
		}

		List<Schema> schemaList = catalog.getSchemas();

		for (Schema sc : schemaList) {
			// get triggers
			List<Trigger> trigList = getAllTriggers(conn, catalog.getName());
			sc.setTriggers(trigList);
		}
	}

	/**
	 * build View's Columns
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param view View
	 * @throws SQLException e
	 */
	protected void buildViewColumns(final Connection conn,
			final Catalog catalog, final Schema schema, final View view) throws SQLException {
		super.buildViewColumns(conn, catalog, schema, view);

		// get shown data type
		String sqlStr = "DESC " + getQuoteStr(view.getName());

		ResultSet rs = null; // NOPMD
		PreparedStatement stmt = null; // NOPMD
		try {
			stmt = conn.prepareStatement(sqlStr);
			rs = stmt.executeQuery();

			while (rs.next()) {
				String columnName = rs.getString("FIELD");
				String columnType = rs.getString("TYPE");

				Column column = view.getColumnByName(columnName);

				if (column != null && columnType != null) {
					column.setShownDataType(columnType);
				}
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		List<Column> list = view.getColumns();

		for (Column column : list) {

			if (column.getDataType().equals("BIT")) {
				column.setDataType("boolean");
			}
		}
	}

	/**
	 * get All Functions
	 * 
	 * @param conn Connection
	 * @param dbName String
	 * @return all Functions
	 * @throws SQLException e
	 */
	private List<Function> getAllFunctions(final Connection conn,
			final String dbName) throws SQLException {
		final List<String> list = this.getRountines(conn, dbName, "FUNCTION");
		final List<Function> funcs = new ArrayList<Function>();

		for (String name : list) {
			final Function func = factory.createFunction();
			func.setName(name);
			final String funcDDL = getFunctionDDL(conn, name);
			func.setFuncDDL(funcDDL);
			funcs.add(func);
		}

		return funcs;
	}

	/**
	 * get All Procedures
	 * 
	 * @param conn Connection
	 * @param dbName String
	 * @return all Procedures
	 * @throws SQLException e
	 */
	private List<Procedure> getAllProcedures(final Connection conn,
			final String dbName) throws SQLException {
		final List<String> list = this.getRountines(conn, dbName, "PROCEDURE");
		final List<Procedure> procs = new ArrayList<Procedure>();

		for (String name : list) {
			final Procedure proc = factory.createProcedure();
			proc.setName(name);
			final String procDDL = getProcedureDDL(conn, name);
			proc.setProcedureDDL(procDDL);
			procs.add(proc);
		}

		return procs;
	}

	/**
	 * get All Triggers
	 * 
	 * @param conn Connection
	 * @param dbName the db name
	 * @return all triggers
	 * @throws SQLException e
	 */
	private List<Trigger> getAllTriggers(final Connection conn,
			final String dbName) throws SQLException {
		PreparedStatement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			final String sqlStr = "SELECT TRIGGER_NAME,EVENT_MANIPULATION,"
					+ "EVENT_OBJECT_TABLE,ACTION_TIMING,ACTION_STATEMENT "
					+ "FROM INFORMATION_SCHEMA.TRIGGERS "
					+ "WHERE TRIGGER_SCHEMA=? ORDER BY TRIGGER_NAME";
			stmt = conn.prepareStatement(sqlStr);
			stmt.setString(1, dbName);
			rs = stmt.executeQuery();

			final List<Trigger> trigs = new ArrayList<Trigger>();

			while (rs.next()) {
				final MySQLTrigger trig = (MySQLTrigger) factory.createTrigger();
				trig.setName(rs.getString("TRIGGER_NAME"));
				trig.setEventManipulation(rs.getString("EVENT_MANIPULATION"));
				trig.setEventTable(rs.getString("EVENT_OBJECT_TABLE"));
				trig.setActionTiming(rs.getString("ACTION_TIMING"));
				trig.setActionStmt(rs.getString("ACTION_STATEMENT"));

				trigs.add(trig);
			}

			return trigs;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * get Auto_Increment Max Value By TableName
	 * 
	 * @param conn Connection
	 * @param tableName String
	 * @return long
	 * @throws SQLException e
	 */
	protected Long getAutoIncNextValByTableName(final Connection conn,
			final String tableName) throws SQLException {
		Long result = null;
		PreparedStatement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			final String sqlStr = SHOW_AUTOINCREMENT_MAXVAL;
			stmt = conn.prepareStatement(sqlStr);
			stmt.setString(1, tableName);
			rs = stmt.executeQuery();

			if (rs.next()) {
				result = rs.getLong("AUTO_INCREMENT");
			}

			return result;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

	}

	/**
	 * get DB char set
	 * 
	 * @param conn Connection
	 * @return DB charSet
	 * @throws SQLException e
	 */
	protected String getCharSetByDBVariables(final Connection conn) throws SQLException {

		PreparedStatement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			final String sqlStr = SHOW_CHARSET;
			stmt = conn.prepareStatement(sqlStr);
			rs = stmt.executeQuery();

			String charSet = null;

			if (rs.next()) {
				charSet = rs.getString("value");
			}

			return charSet;

		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * get database ddl
	 * 
	 * @param conn Connection
	 * @param dbName String
	 * @return String create db ddl
	 */
	protected String getDBDDL(final Connection conn, final String dbName) {
		if (StringUtils.isBlank(dbName)) {
			throw new IllegalArgumentException("The DB name is null!");
		}

		Statement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(SHOW_DB + getQuoteStr(dbName));

			String ddl = null;

			while (rs.next()) {
				ddl = rs.getString(2);
			}

			return ddl;
		} catch (SQLException ex) {
			LOG.error("Get MySQL Database DDL error.", ex);
			return "";
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	protected DBExportHelper getExportHelper() {
		return DatabaseType.MYSQL.getExportHelper();
	}

	/**
	 * get function's DDL by name
	 * 
	 * @param conn Connection
	 * @param functionName the function'name
	 * @return the function's DDL
	 * @throws SQLException e
	 */
	private String getFunctionDDL(final Connection conn,
			final String functionName) throws SQLException {
		Statement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(SHOW_FUNCTION + getQuoteStr(functionName));

			String ddl = null;

			if (rs.next()) {
				ddl = rs.getString(3);
			}

			return ddl;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * get Procedure DDL by name
	 * 
	 * @param conn Connection
	 * @param procedureName the procedure'name
	 * @return the procedure'DDL
	 * @throws SQLException e
	 */
	private String getProcedureDDL(final Connection conn,
			final String procedureName) throws SQLException {
		Statement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(SHOW_PROCEDURE + getQuoteStr(procedureName));

			String ddl = null;

			if (rs.next()) {
				ddl = rs.getString(3);
			}

			return ddl;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * return database object name
	 * 
	 * @param mysqlObjectName String
	 * @return String
	 */
	private String getQuoteStr(String mysqlObjectName) {
		return "`" + mysqlObjectName + "`";
	}

	/**
	 * get All Rountines
	 * 
	 * @param conn Connection
	 * @param dbName the db name
	 * @param type procedure/function
	 * @return all Rountines
	 * @throws SQLException e
	 */
	private List<String> getRountines(final Connection conn,
			final String dbName, final String type) throws SQLException {
		PreparedStatement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			final String sqlStr = "SELECT name FROM mysql.proc  "
					+ "WHERE db=? AND type=? ORDER BY name";
			stmt = conn.prepareStatement(sqlStr);
			stmt.setString(1, dbName);
			stmt.setString(2, type);
			rs = stmt.executeQuery();

			final List<String> list = new ArrayList<String>();

			while (rs.next()) {
				list.add(rs.getString(1));
			}

			return list;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * getSourcePartitionDLL
	 * 
	 * @param sourceTable Table
	 * @return String
	 */
	protected String getSourcePartitionDDL(Table sourceTable) {
		String ddl = sourceTable.getDDL();
		if (ddl.indexOf("PARTITION BY") > -1) {
			return ddl.substring(ddl.indexOf("PARTITION BY"), ddl.length() - 2);
		}
		return "";
	}

	/**
	 * get TABLE DDL
	 * 
	 * @param conn Connection
	 * @param tableName String
	 * @return String
	 * @throws SQLException e
	 */
	protected String getTableDDL(final Connection conn, final String tableName) throws SQLException {
		if (StringUtils.isBlank(tableName)) {
			throw new IllegalArgumentException("The table name is null!");
		}

		Statement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(SHOW_TABLE + getQuoteStr(tableName));

			String ddl = null;

			while (rs.next()) {
				ddl = rs.getString(2);
			}

			return ddl;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	//	/**
	//	 * get table's row count by schema name
	//	 * 
	//	 * @param conn Connection
	//	 * @param schemaName String
	//	 * @return Map<String, String> @ e
	//	 */
	//	protected Map<String, String> getTableRowCntBySchemaName(
	//			final Connection conn, final String schemaName) {
	//		final Map<String, String> map = new HashMap<String, String>();
	//		ResultSet rs = null; // NOPMD
	//		PreparedStatement stmt = null; // NOPMD
	//		try {
	//			final String tableType = "BASE TABLE";
	//			final String sqlStr = "SELECT TABLE_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE=? AND TABLE_SCHEMA=?";
	//			stmt = conn.prepareStatement(sqlStr);
	//			stmt.setString(1, tableType);
	//			stmt.setString(2, schemaName);
	//			rs = stmt.executeQuery();
	//
	//			while (rs.next()) {
	//				map.put(rs.getString(1), rs.getString(2));
	//			}
	//
	//			return map;
	//		} catch (SQLException e) {
	//			throw new RuntimeException(e);
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//		}
	//	}

	/**
	 * get time zone
	 * 
	 * @param conn Connection
	 * @return String time zone
	 * @throws SQLException e
	 */
	protected String getTimezone(final Connection conn) throws SQLException {

		Statement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		int tzOffset = 0;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT EXTRACT(HOUR FROM TIMEDIFF(NOW() ,UTC_TIMESTAMP())) AS OFFSET");

			if (rs.next()) {
				tzOffset = rs.getInt("OFFSET");
			}
		} catch (Exception e) {
			LOG.error("Get MySQL timezone error", e);
			tzOffset = Calendar.getInstance().getTimeZone().getRawOffset();
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		try {
			return TimeZoneUtils.getTZFromOffset(tzOffset);
		} catch (Exception e) {
			LOG.error("Get MySQL timezone error", e);
		}
		return "";
	}

	/**
	 * get VIEW DDL
	 * 
	 * @param conn Connection
	 * @param viewName String
	 * @return String
	 * @throws SQLException e
	 */
	protected String getViewDDL(final Connection conn, final String viewName) throws SQLException {
		if (StringUtils.isBlank(viewName)) {
			throw new IllegalArgumentException("The view name is null!");
		}

		Statement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(SHOW_VIEW + getQuoteStr(viewName));

			String ddl = null;

			while (rs.next()) {
				ddl = rs.getString(2);
			}

			return ddl;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * Only mysql 5.1 or later version can support table partitions
	 * 
	 * @param version of Mysql
	 * @return true if support.
	 */
	protected boolean isSupportParitionVersion(Version version) {
		return !(version.getDbMajorVersion() < 5 || (version.getDbMajorVersion() == 5 && version.getDbMinorVersion() < 1));
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.MYSQL;
	}

	//	/**
	//	 * 
	//	 * buildAllSchemas
	//	 * 
	//	 * @param conn Connection
	//	 * @param catalog Catalog
	//	 * @param schema Schema
	//	 * @param tables Map<String, Table>
	//	 * @throws SQLException ex
	//	 */
	//	protected void buildAllSchemas(Connection conn, Catalog catalog,
	//			Schema schema, Map<String, Table> tables) throws SQLException {
	//	}
}