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
package com.cubrid.cubridmigration.mssql.meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IBuildSchemaFilter;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObjectFactory;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.PartitionTable;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.mssql.MSSQLDataTypeHelper;
import com.cubrid.cubridmigration.mssql.MSSQLSQLHelper;
import com.cubrid.cubridmigration.mssql.dbobj.MSSQLPartitionSchemas;
import com.cubrid.cubridmigration.mssql.export.MSSQLExportHelper;

/**
 * 
 * MSSQLSchemaFetcher
 * 
 * @author Kevin Cao
 * @version 1.0 - 2010-4-15
 */
public final class MSSQLSchemaFetcher extends
		AbstractJDBCSchemaFetcher {

	private static final String MSSQL_TYPE_HIERARCHYID = "hierarchyid";
	private static final String CATALOG_NAME = "catalogName";
	private final static Logger LOG = LogUtil.getLogger(MSSQLSchemaFetcher.class);

	private static final String OBJECT_TYPE_VIEW = "VIEW";

	private static final String SHOW_CHARSET = "SELECT [DEFAULT_CHARACTER_SET_NAME] FROM [catalogName].[INFORMATION_SCHEMA].[SCHEMATA] "
			+ "WHERE [CATALOG_NAME]=? AND [SCHEMA_NAME]='dbo'";
	private static final String SHOW_DDL = "SELECT [definition]  FROM [catalogName].[sys].[all_sql_modules] a, [catalogName].[sys].[all_objects] b"
			+ " WHERE b.[schema_id]=? and a.[object_id]=b.[object_id] "
			+ "and b.[name]=? and b.[type_desc]=? ";

	private static final String SHOW_IDENTITY = "SELECT a.[name] tablename, b.[name] columnname, "
			+ "cast(b.[seed_value] as bigint) seed_value, "
			+ "cast(b.[increment_value] as bigint) increment_value, "
			+ "cast(b.[last_value] as bigint) last_value "
			+ "FROM [catalogName].[sys].[tables] a, [catalogName].[sys].[identity_columns] b "
			+ "Where a.[object_id]=b.[object_id] and a.[schema_id]=?";

	private static final String SHOW_SCHEMA_ID = "SELECT [schema_id],[name] FROM [catalogName].[sys].[schemas] WHERE [name] in ("
			+ "SELECT distinct [TABLE_SCHEMA] FROM [catalogName].[INFORMATION_SCHEMA].[TABLES])";

	private static final String USER_DEF_DATA_TYPE = "select t.name as username,t2.name as realname"
			+ " from sys.systypes t,sys.systypes t2 where t.xtype<>t.xusertype and t.xtype=t2.xusertype";

	private static final Map<String, String> CHARSET_MAPPING = new HashMap<String, String>();
	static {
		CHARSET_MAPPING.put("cp936", "GBK");
		CHARSET_MAPPING.put("cp932", "EUC-JP");
		CHARSET_MAPPING.put("cp949", "EUC-KR");
	}

	private Map<String, Integer> schemaNameIDMap = new HashMap<String, Integer>();

	public MSSQLSchemaFetcher() {
		super();
		factory = new DBObjectFactory() {
			/**
			 * return DataType
			 * 
			 * @return DataType
			 */
			public DataType createDataType() {
				return new DataType();
				// new MSSQLDataTypeHelper()
			}
		};
	}

	/**
	 * build catalog
	 * 
	 * @param conn Connection
	 * @param cp ConnParameters
	 * @param filter IBuildSchemaFilter
	 * 
	 * @return Catalog
	 * @throws SQLException e
	 */
	public Catalog buildCatalog(final Connection conn, ConnParameters cp, IBuildSchemaFilter filter) throws SQLException {
		final Catalog catalog = super.buildCatalog(conn, cp, filter);
		final String catalogName = cp.getDbName();
		String charset = getCatalogCharset(conn, catalog);
		catalog.setCharset(charset);
		final List<Schema> schemaList = catalog.getSchemas();
		final SQLHelper sqlHelper = cp.getDatabaseType().getSQLHelper(null);
		for (Schema schema : schemaList) {
			ResultSet rs = null; // NOPMD
			PreparedStatement stmt = null; //NOPMD		
			try {
				stmt = conn.prepareStatement(SHOW_IDENTITY.replace(CATALOG_NAME, catalogName));
				stmt.setInt(1, schemaNameIDMap.get(schema.getName()));
				rs = stmt.executeQuery();

				while (rs.next()) {
					Table table = schema.getTableByName(rs.getString("tablename"));
					if (table == null) {
						continue;
					}
					Column column = table.getColumnByName(rs.getString("columnname"));
					if (column == null) {
						continue;
					}
					column.setAutoIncrement(true);
					Long incrementValue = rs.getLong("increment_value");
					incrementValue = incrementValue == null ? 1 : incrementValue;
					column.setAutoIncIncrVal(incrementValue);
					Long lastValue = rs.getLong("last_value");
					if (null == lastValue) {
						column.setAutoIncSeedVal(rs.getLong("seed_value"));
					} else {
						column.setAutoIncSeedVal(lastValue + incrementValue);
					}
				}
			} finally {
				Closer.close(rs);
				Closer.close(stmt);
			}

			// get views
			final List<View> viewList = schema.getViews();

			for (View view : viewList) {
				String viewDDL = getObjectDDL(conn, catalogName, schema.getName(), view.getName(),
						OBJECT_TYPE_VIEW);
				view.setDDL(viewDDL);
				view.setQuerySpec(sqlHelper.getViewQuerySpec(viewDDL));
				view.setOwner(schema.getName());
			}
			// get partitions
			buildPartitions(conn, catalog, schema);
		}
		catalog.setTimezone(getTimezone(conn));
		return catalog;
	}

	/**
	 * build Partitions
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @throws SQLException e
	 */
	private void buildPartitions(final Connection conn, final Catalog catalog, final Schema schema) throws SQLException {
		String sql = "SELECT a.[name], a.[data_space_id], b.[type_desc], b.[fanout], b.[boundary_value_on_right], a.[function_id] "
				+ " FROM [sys].[partition_schemes] a,[sys].[partition_functions] b"
				+ " WHERE a.[function_id]=b.[function_id] AND a.[type]='PS' and b.[type]='R'";

		Map<Long, MSSQLPartitionSchemas> partSchemas = new HashMap<Long, MSSQLPartitionSchemas>();
		ResultSet rs = null; //NOPMD
		Statement stmt = null; //NOPMD
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				MSSQLPartitionSchemas ps = new MSSQLPartitionSchemas();
				ps.setName(rs.getString("name"));
				Long dataSpaceID = rs.getLong("data_space_id");
				ps.setDataSpaceId(dataSpaceID);
				ps.setPartitionType(rs.getString("type_desc"));
				ps.setPartitionCount(rs.getInt("fanout"));
				ps.setBoundaryValueOnRight(rs.getBoolean("boundary_value_on_right"));
				ps.setFunctionId(rs.getInt("function_id"));
				partSchemas.put(dataSpaceID, ps);
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		Collection<MSSQLPartitionSchemas> psList = partSchemas.values();

		for (MSSQLPartitionSchemas ps : psList) {
			sql = "SELECT cast([value] as varchar(255)) value FROM [sys].[partition_range_values]"
					+ " WHERE [function_id]=";
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql + ps.getFunctionId());
				final ArrayList<String> prvs = new ArrayList<String>();
				while (rs.next()) {
					prvs.add(rs.getString("value"));
				}
				ps.setPartitionRangeValues(prvs);
			} finally {
				Closer.close(rs);
				Closer.close(stmt);
			}

			sql = "SELECT a.[parameter_id], b.[name] FROM [sys].[partition_parameters] a, [sys].[types] b"
					+ " WHERE a.[system_type_id]=b.[system_type_id] AND a.[function_id]=";
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql + ps.getFunctionId());

				while (rs.next()) {
					ps.setParameterId(rs.getInt("parameter_id"));
					ps.setSystemType(rs.getString("name"));
				}
			} finally {
				Closer.close(rs);
				Closer.close(stmt);
			}
		}

		sql = "SELECT b.[name], a.[data_space_id], a.[index_id], a.[object_id] "
				+ " FROM [sys].[indexes] a, [sys].[tables] b,[sys].[schemas] s"
				+ " WHERE a.[index_id ]< 2 AND a.[object_id]=b.[object_id] and b.[schema_id]=s.[schema_id] and s.[name]='"
				+ schema.getName() + "' order by b.[name],a.[data_space_id]";
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				Long dataSpaceId = rs.getLong("data_space_id");
				MSSQLPartitionSchemas ps = partSchemas.get(dataSpaceId);
				if (ps == null) {
					continue;
				}
				String tableName = rs.getString("name");
				Table table = schema.getTableByName(tableName);
				if (table == null) {
					continue;
				}
				PartitionInfo partInfo = factory.createPartitionInfo();
				partInfo.setPartitionMethod(ps.getPartitionType());
				partInfo.setPartitionCount(ps.getPartitionCount());
				partInfo.setBoundaryValueOnRight(ps.getBoundaryValueOnRight());
				partInfo.setPartitionColumnCount(1);

				int tableID = rs.getInt("object_id");
				int indexID = rs.getInt("index_id");
				Column partCol = getPartitionColumn(conn, tableID, indexID, ps, table);
				if (partCol == null) {
					continue;
				}
				partInfo.addPartitionColumn(partCol);
				partInfo.setPartitionExp(partCol.getName());
				partInfo.setPartitionFunc(null);

				int i = 0;
				for (; i < ps.getPartitionCount() - 1; i++) {
					PartitionTable partition = factory.createPartitionTable();
					partition.setPartitionName(tableName + "_p" + i);
					String partitionDesc = ps.getPartitionRangeValues().get(i);

					partition.setPartitionDesc(partitionDesc);
					partition.setPartitionIdx(i);
					partInfo.addPartition(partition);
				}
				PartitionTable partition = factory.createPartitionTable();
				partition.setPartitionName(tableName + "_p" + i);
				partition.setPartitionDesc("MAXVALUE");
				partition.setPartitionIdx(i);
				partInfo.addPartition(partition);

				table.setPartitionInfo(partInfo);
				partInfo.setDDL(MSSQLSQLHelper.getInstance(null).getTablePartitonDDL(table));
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
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
		MSSQLDataTypeHelper dtHelper = MSSQLDataTypeHelper.getInstance(null);
		for (Column column : columns) {
			column.setShownDataType(dtHelper.getShownDataType(column));
		}
		return sourceTable;
	}

	/**
	 * extract Table's Columns
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableColumns(final Connection conn, final Catalog catalog,
			final Schema schema, final Table table) throws SQLException {
		ResultSet rs = null; //NOPMD
		try {
			rs = conn.getMetaData().getColumns(catalog.getName(), schema.getName(),
					table.getName(), null);
			MSSQLDataTypeHelper dtHelper = MSSQLDataTypeHelper.getInstance(null);
			final Map<String, List<DataType>> supportedDataType = catalog.getSupportedDataType();
			if (table.getName().equalsIgnoreCase("ProductDocument")) {
				System.out.println();
			}
			while (rs.next()) {
				// create new column
				final Column column = factory.createColumn();
				column.setTableOrView(table);
				final String typeName = rs.getString("COLUMN_NAME");
				column.setName(typeName);
				//remove identity key words
				String dataType = rs.getString("TYPE_NAME");
				int idIndex = dataType.indexOf("identity");
				if (idIndex >= 0) {
					dataType = dataType.substring(0, idIndex).trim();
				}
				//Remove '()'
				idIndex = dataType.indexOf("(");
				if (idIndex > 0) {
					dataType = dataType.substring(0, idIndex).trim();
				}
				final List<DataType> dtList = supportedDataType.get(dataType);
				if (CollectionUtils.isEmpty(dtList) || dtList.get(0) == null) {
					continue;
				}
				column.setDataType(dtList.get(0).getTypeName());
				column.setCharLength(rs.getInt("COLUMN_SIZE"));
				column.setPrecision(column.getCharLength());
				column.setScale(rs.getInt("DECIMAL_DIGITS"));
				// make sure precision is greater than scale
				if (column.getScale() != null && column.getPrecision() < column.getScale()) {
					column.setPrecision(16);

					if (column.getPrecision() < column.getScale()) {
						column.setPrecision(column.getScale() + 1);
					}
				}
				column.setJdbcIDOfDataType(rs.getInt("DATA_TYPE"));
				column.setNullable(rs.getInt("NULLABLE") == java.sql.DatabaseMetaData.columnNullable);
				// set column default value
				String defaultValue = rs.getString("COLUMN_DEF");
				if (defaultValue != null) {
					if ("(NULL)".equals(defaultValue)) {
						defaultValue = null;
					} else if (defaultValue.startsWith("(")) {
						//Some default value like ('default')
						defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
						//Some default value like ((0))
						if (defaultValue.startsWith("(")) {
							defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
						}
					}
				}
				column.setDefaultValue(defaultValue);
				String shownDataType = dtHelper.getShownDataType(column);
				column.setShownDataType(shownDataType);
				table.addColumn(column);
			}
		} finally {
			Closer.close(rs);
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
	protected void buildTableIndexes(final Connection conn, final Catalog catalog,
			final Schema schema, final Table table) throws SQLException {
		super.buildTableIndexes(conn, catalog, schema, table);
		// remove duplicate PK index
		final PK pk = table.getPk();
		final String pkName = pk == null ? "" : pk.getName();
		table.removeIndex(pkName);
	}

	/**
	 * extract View's Columns
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param view View
	 * @throws SQLException e
	 */
	protected void buildViewColumns(final Connection conn, final Catalog catalog,
			final Schema schema, final View view) throws SQLException {
		super.buildViewColumns(conn, catalog, schema, view);
		MSSQLDataTypeHelper dtHelper = MSSQLDataTypeHelper.getInstance(null);
		for (Column column : view.getColumns()) {
			String shownDataType = dtHelper.getShownDataType(column);
			column.setShownDataType(shownDataType);
		}
	}

	/**
	 * return a list of table name. sysdiagrams was removed from list.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @return List<String> with schemaname.tablename
	 * @throws SQLException e
	 */
	protected List<String> getAllTableNames(Connection conn, Catalog catalog, Schema schema) throws SQLException {
		final List<String> result = super.getAllTableNames(conn, catalog, schema);
		result.remove("sysdiagrams");
		List<String> tables = new ArrayList<String>();
		for (String name : result) {
			tables.add(schema.getName() + "." + name);
		}
		return tables;
	}

	/**
	 * get SQL Server charset
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @throws SQLException e
	 * @return String
	 */
	private String getCatalogCharset(final Connection conn, final Catalog catalog) throws SQLException {
		ResultSet rs = null; // NOPMD
		PreparedStatement stmt = null; //NOPMD		
		try {
			stmt = conn.prepareStatement(SHOW_CHARSET.replace(CATALOG_NAME, catalog.getName()));
			stmt.setString(1, catalog.getName());
			rs = stmt.executeQuery();
			if (rs.next()) {
				String string = rs.getString(1);
				if ("iso_1".equals(string)) {
					string = "iso8859-1";
				}
				String result = CHARSET_MAPPING.get(string);
				return result == null ? string : result;
			}
			return null;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * Retrieves the export helper for migration
	 * 
	 * @return DBExportHelper
	 */
	protected DBExportHelper getExportHelper() {
		return new MSSQLExportHelper();
	}

	/**
	 * get TABLE DDL
	 * 
	 * @param conn Connection
	 * @param catalogName String
	 * @param schemaName String
	 * @param objectName String
	 * @param objectType String
	 * @return String
	 * @throws SQLException e
	 */
	public String getObjectDDL(final Connection conn, final String catalogName,
			final String schemaName, final String objectName, final String objectType) throws SQLException {
		if (StringUtils.isBlank(objectName)) {
			throw new IllegalArgumentException("The oracle object name is null!");
		}

		PreparedStatement preStmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			preStmt = conn.prepareStatement(SHOW_DDL.replace(CATALOG_NAME, catalogName));
			Integer schemaID = schemaNameIDMap.get(schemaName);
			preStmt.setInt(1, schemaID);
			preStmt.setString(2, objectName);
			preStmt.setString(3, objectType);

			rs = preStmt.executeQuery();

			String ddl = null;

			while (rs.next()) {
				ddl = rs.getString(1);
			}

			return ddl;
		} finally {
			Closer.close(rs);
			Closer.close(preStmt);
		}
	}

	/**
	 * get partition column
	 * 
	 * @param conn Connection
	 * @param tableID int
	 * @param indexID int
	 * @param ps PartitionSchemas
	 * @param table Table
	 * @return Column
	 * @throws SQLException e
	 */
	private Column getPartitionColumn(final Connection conn, int tableID, int indexID,
			MSSQLPartitionSchemas ps, Table table) throws SQLException {
		Column partitionColumn = null;
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD

		try {
			String sql2 = "SELECT COL_NAME([object_id],[column_id]) "
					+ "FROM [sys].[index_columns] " + "WHERE [object_id]=? AND [index_id]=?";

			stmt = conn.prepareStatement(sql2);
			stmt.setInt(1, tableID);
			stmt.setInt(2, indexID);
			rs = stmt.executeQuery();
			if (rs.next()) {
				String columnName = rs.getString(1);
				partitionColumn = table.getColumnByName(columnName);
			} else {
				for (Column column : table.getColumns()) {
					if (column.getDataType().equals(ps.getSystemType())) {
						partitionColumn = column;
						break;
					}
				}
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		return partitionColumn;
	}

	/**
	 * return schema names
	 * 
	 * @param conn Connection
	 * @param cp ConnParameters
	 * 
	 * @return List<String>
	 * @throws SQLException e
	 */
	protected List<String> getSchemaNames(Connection conn, ConnParameters cp) throws SQLException {

		ArrayList<String> schemaNames = new ArrayList<String>();
		PreparedStatement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD

		try {
			stmt = conn.prepareStatement(SHOW_SCHEMA_ID.replace(CATALOG_NAME, cp.getDbName()));
			rs = stmt.executeQuery();
			while (rs.next()) {
				String schemaName = rs.getString(2);
				Integer schemaID = rs.getInt(1);
				schemaNameIDMap.put(schemaName, schemaID);
				schemaNames.add(schemaName);
			}
			return schemaNames;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * get time zone
	 * 
	 * @param conn Connection
	 * @return String time zone
	 * @throws SQLException e
	 */
	private String getTimezone(final Connection conn) throws SQLException {

		Statement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			String timezone = "";
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT getUTCdate(), getdate()");

			if (rs.next()) {
				Timestamp utcTimestamp = rs.getTimestamp(1);
				Timestamp sysTimestamp = rs.getTimestamp(2);

				int rawOffset = (int) (sysTimestamp.getTime() - utcTimestamp.getTime());
				timezone = TimeZoneUtils.getGMTFormat(rawOffset);
				//				System.out.println("timezone: " + timezone);
				return timezone;
			}

			return timezone;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.MSSQL;
	}

	/**
	 * Get all sql data types
	 * 
	 * @param conn Connection
	 * @return String
	 * @throws SQLException e
	 */
	protected final Map<String, List<DataType>> getSupportedSqlTypes(Connection conn) throws SQLException {
		final Map<String, List<DataType>> supportedSqlTypes = super.getSupportedSqlTypes(conn);
		addHierarchyIDTypeToSupportedTypes(supportedSqlTypes);
		transforUserDefinedDataTypeToSysDataType(conn, supportedSqlTypes);
		return supportedSqlTypes;
	}

	/**
	 * JDBC driver retrieved supported types don't contain the HierarchyID
	 * 
	 * @param supportedSqlTypes result
	 */
	private void addHierarchyIDTypeToSupportedTypes(
			final Map<String, List<DataType>> supportedSqlTypes) {
		DataType dataTypeObj = factory.createDataType();
		dataTypeObj.setTypeName(MSSQL_TYPE_HIERARCHYID);
		dataTypeObj.setJdbcDataTypeID(MSSQL_TYPE_HIERARCHYID.hashCode());
		List<DataType> list = new ArrayList<DataType>();
		list.add(dataTypeObj);
		supportedSqlTypes.put(MSSQL_TYPE_HIERARCHYID, list);
	}

	/**
	 * Transform user defined type into database type.
	 * 
	 * @param conn JDBC connection
	 * @param supportedSqlTypes result
	 */
	private void transforUserDefinedDataTypeToSysDataType(Connection conn,
			final Map<String, List<DataType>> supportedSqlTypes) {
		try {
			Statement stmt = conn.createStatement();
			final ResultSet rs = stmt.executeQuery(USER_DEF_DATA_TYPE);
			try {
				while (rs.next()) {
					String userTypeName = rs.getString(1);
					String realTypeName = rs.getString(2);
					final List<DataType> userType = supportedSqlTypes.get(userTypeName);
					final List<DataType> realType = supportedSqlTypes.get(realTypeName);
					if (CollectionUtils.isEmpty(userType) || CollectionUtils.isEmpty(realType)) {
						continue;
					}
					supportedSqlTypes.put(userTypeName, realType);
				}
			} finally {
				rs.close();
				stmt.close();
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		}
	}

	/**
	 * extract Table's FK
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableFKs(final Connection conn, final Catalog catalog, final Schema schema,
			final Table table) throws SQLException {
		super.buildTableFKs(conn, catalog, schema, table);
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]MSSQL buildTableFKs()");
		}
		//TO fix the jdbc's error of reading FK information
		String sql = "select CONSTRAINT_NAME,DELETE_RULE,UPDATE_RULE from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS where CONSTRAINT_SCHEMA=? and CONSTRAINT_NAME=?";
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = conn.prepareStatement(sql);
		List<FK> fks = table.getFks();
		try {
			for (FK fk : fks) {
				try {
					stmt.setString(1, getSchemaName(schema));
					stmt.setString(2, fk.getName());
					rs = stmt.executeQuery();
					while (rs.next()) {
						if ("NO ACTION".equalsIgnoreCase(rs.getString(2))) {
							fk.setDeleteRule(FK.ON_DELETE_NO_ACTION);
						} else if ("SET NULL".equalsIgnoreCase(rs.getString(2))) {
							fk.setDeleteRule(FK.ON_DELETE_SET_NULL);
						}

						if ("NO ACTION".equalsIgnoreCase(rs.getString(3))) {
							fk.setUpdateRule(FK.ON_UPDATE_NO_ACTION);
						} else if ("SET NULL".equalsIgnoreCase(rs.getString(2))) {
							fk.setUpdateRule(FK.ON_UPDATE_SET_NULL);
						}
					}
				} finally {
					Closer.close(rs);
				}
			}
		} finally {
			Closer.close(stmt);
		}
	}
}