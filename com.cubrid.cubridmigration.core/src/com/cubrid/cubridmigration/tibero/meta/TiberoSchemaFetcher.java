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
package com.cubrid.cubridmigration.tibero.meta;

import java.io.Reader;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.CommonUtils;
import com.cubrid.cubridmigration.core.common.DBUtils;
import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IBuildSchemaFilter;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObjectFactory;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.PartitionTable;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.dbobject.Version;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.graph.GraphDataTypeHelper;
import com.cubrid.cubridmigration.tibero.TiberoDataTypeHelper;

public final class TiberoSchemaFetcher extends
		AbstractJDBCSchemaFetcher {
	private final static List<Object> COLUMNS_RESET1 = CommonUtils.createListWithArray(new Object[] {
			"CHAR", "NCHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2", "LONG" });

	private final static List<Object> COLUMNS_RESET2 = CommonUtils.createListWithArray(new Object[] {
			"RAW", "LONG RAW" });

	private final static Logger LOG = LogUtil.getLogger(TiberoSchemaFetcher.class);

	private static final String OBJECT_TYPE_FUNCTION = "FUNCTION";
	private static final String OBJECT_TYPE_PROCEDURE = "PROCEDURE";
	private static final String OBJECT_TYPE_SEQUENCE = "SEQUENCE";
	private static final String OBJECT_TYPE_TABLE = "TABLE";
	private static final String OBJECT_TYPE_TRIGGER = "TRIGGER";
	private static final String OBJECT_TYPE_VIEW = "VIEW";
	//	private static final String OBJECT_TYPE_INDEX = "INDEX";

	//Undefined columns will not be supported.
	private static final String SQL_GET_COLUMNS = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT, CHAR_LENGTH, CHAR_USED, COLUMN_ID "
			+ "FROM ALL_TAB_COLUMNS T WHERE T.OWNER=? AND T.TABLE_NAME=? " + "ORDER BY COLUMN_ID";

	private static final String SQL_GET_INDEX_COLUMNS = "SELECT A.COLUMN_NAME, A.DESCEND, B.COLUMN_EXPRESSION "
			+ "FROM ALL_IND_COLUMNS A LEFT JOIN ALL_IND_EXPRESSIONS B "
			+ "ON A.TABLE_OWNER=B.TABLE_OWNER AND A.TABLE_NAME=B.TABLE_NAME AND A.INDEX_NAME=B.INDEX_NAME AND A.COLUMN_POSITION=B.COLUMN_POSITION "
			+ " WHERE A.TABLE_OWNER=? AND A.TABLE_NAME=? "
			+ "AND A.INDEX_NAME=? ORDER BY A.COLUMN_POSITION";

	private static final String SQL_GET_PART_COLUMN = "SELECT * FROM ALL_PART_KEY_COLUMNS WHERE OBJECT_TYPE='TABLE' AND OWNER=? "
			+ " ORDER BY NAME, COLUMN_POSITION";

	private static final String SQL_GET_PART_TABLES = "SELECT T.* FROM ALL_PART_TABLES T WHERE T.OWNER=? ORDER BY TABLE_NAME";

	private static final String SQL_GET_PARTITIONS = "SELECT T.TABLE_NAME, T.PARTITION_NAME, T.HIGH_VALUE, T.PARTITION_POSITION "
			+ "FROM ALL_TAB_PARTITIONS T WHERE T.TABLE_OWNER=? "
			+ "ORDER BY TABLE_NAME, PARTITION_POSITION";

	private static final String SQL_GET_SUB_PART_TABLES = "SELECT TABLE_NAME, PARTITION_NAME, SUBPARTITION_NAME, HIGH_VALUE, SUBPARTITION_POSITION "
			+ " FROM ALL_TAB_SUBPARTITIONS WHERE TABLE_OWNER=? ORDER BY TABLE_NAME, SUBPARTITION_POSITION";

	private static final String SQL_GET_SUBPART_KEY_COLUMN = "SELECT * FROM ALL_SUBPART_KEY_COLUMNS WHERE OBJECT_TYPE='TABLE' AND OWNER=? "
			+ " ORDER BY NAME, COLUMN_POSITION";

	private static final String SQL_GET_TABLE_INDEX = "SELECT INDEX_NAME, INDEX_TYPE, UNIQUENESS FROM ALL_INDEXES A "
			+ " WHERE A.TABLE_OWNER=? AND A.TABLE_NAME=? "
			+ "AND A.INDEX_NAME NOT IN (SELECT C.CONSTRAINT_NAME FROM ALL_CONSTRAINTS C "
			+ "WHERE C.CONSTRAINT_TYPE='P' AND C.OWNER=A.TABLE_OWNER AND C.TABLE_NAME=A.TABLE_NAME) ORDER BY A.INDEX_NAME";

	private static final String SQL_SHOW_ALL_OBJECTS = "SELECT NAME FROM ALL_SOURCE S "
			+ "WHERE S.TYPE=? AND S.OWNER=? AND NOT S.NAME LIKE 'BIN$%'";

	private static final String SQL_SHOW_DDL = "SELECT DBMS_METADATA.GET_DDL(?, T.OBJECT_NAME, T.OWNER)\n"
			+ "FROM (\n"
			+ "	SELECT S.OBJECT_NAME, S.OWNER AS OWNER FROM ALL_OBJECTS S, ALL_TAB_PRIVS P\n"
			+ "	WHERE S.OBJECT_NAME=P.TABLE_NAME AND S.OBJECT_TYPE=? AND P.TABLE_SCHEMA=? AND P.PRIVILEGE='SELECT'\n"
			+ "	UNION\n"
			+ "	SELECT OBJECT_NAME, ? AS OWNER FROM USER_OBJECTS\n"
			+ "	WHERE OBJECT_TYPE=? AND NOT OBJECT_NAME LIKE 'BIN$%'\n" + ") T WHERE OBJECT_NAME=?";

	private static final String SQL_SHOW_SEQUENCES = "SELECT S.* FROM ALL_SEQUENCES S "
			+ "WHERE S.SEQUENCE_OWNER=? AND NOT S.SEQUENCE_NAME LIKE 'BIN$%' ";

	private static final String SQL_SHOW_VIEW_QUERYTEXT = "SELECT TEXT from ALL_VIEWS WHERE OWNER=? AND VIEW_NAME=?";

	//private static final String SHOW_SEQUENCE_MAXVAL = "SELECT ?.CURRVAL  FROM DUAL";
	private GraphDataTypeHelper graphDTHelper = GraphDataTypeHelper.getInstance(null);
	
	public TiberoSchemaFetcher() {
		factory = new DBObjectFactory() {

		};
	}

	/**
	 * Build Catalog
	 * 
	 * @param conn Connection
	 * @param cp ConnParameters
	 * @param filter IBuildSchemaFilter
	 * @return Catalog
	 * @throws SQLException e
	 */
	public Catalog buildCatalog(final Connection conn, ConnParameters cp, IBuildSchemaFilter filter) throws SQLException {
		final Catalog catalog = super.buildCatalog(conn, cp, filter);
		catalog.setDatabaseType(DatabaseType.TIBERO);
		setCharset(conn, catalog);
		setCatalogTimezone(catalog);
		final List<Schema> schemaList = new ArrayList<Schema>(catalog.getSchemas());
		for (Schema schema : schemaList) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]schema=" + schema.getName());
			}
			// get tables
			List<Table> tableList = schema.getTables();
			if (tableList == null) {
				tableList = new ArrayList<Table>();
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]tableList.count=" + tableList.size());
			}
			for (Table table : tableList) {
				String ddl = getObjectDDL(conn, schema.getName(), table.getName(),
						OBJECT_TYPE_TABLE);
				table.setDDL(ddl);
				
				setImportedKeysCount(conn, catalog, schema, table);
				setExportedKeysCount(conn, catalog, schema, table);
			}
			// get views
			List<View> viewList = schema.getViews();
			if (viewList == null) {
				viewList = new ArrayList<View>();
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]viewList.count=" + viewList.size());
			}
			for (View view : viewList) {
				String ddl = getObjectDDL(conn, schema.getName(), view.getName(), OBJECT_TYPE_VIEW);
				view.setDDL(ddl);
				view.setQuerySpec(getQueryText(conn, schema.getName(), view.getName()));
			}
			buildPartitions(conn, catalog, schema);
		}
		return catalog;
	}

	/**
	 * build Partitions
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 */
	protected void buildPartitions(final Connection conn, final Catalog catalog, final Schema schema) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildPartitions()");
		}
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD
		try {
			stmt = conn.prepareStatement(SQL_GET_PART_TABLES);
			stmt.setString(1, schema.getName());
			rs = stmt.executeQuery();

			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName);
				}
				Table table = schema.getTableByName(tableName);
				if (table == null) {
					continue;
				}

				String partitionMethod = rs.getString("PARTITIONING_TYPE");
				int partitionCount = rs.getInt("PARTITION_COUNT");
				int partitionColumnCount = rs.getInt("PARTITIONING_KEY_COUNT");

				String subPartitionMethod = rs.getString("SUBPARTITIONING_TYPE");
				int subPartitionCount = rs.getInt("DEF_SUBPARTITION_COUNT");
				int subPartitionColumnCount = rs.getInt("SUBPARTITIONING_KEY_COUNT");

				PartitionInfo partitionInfo = factory.createPartitionInfo();
				partitionInfo.setPartitionMethod(partitionMethod);
				partitionInfo.setPartitionCount(partitionCount);
				partitionInfo.setPartitionColumnCount(partitionColumnCount);
				partitionInfo.setPartitionExp(null);
				partitionInfo.setPartitionFunc(null);
				partitionInfo.setDDL(getSourcePartitionDDL(table));
				if ("NONE".equals(subPartitionMethod)) {
					subPartitionMethod = null;
				}
				partitionInfo.setSubPartitionMethod(subPartitionMethod);
				partitionInfo.setSubPartitionCount(subPartitionCount);
				partitionInfo.setSubPartitionColumnCount(subPartitionColumnCount);

				table.setPartitionInfo(partitionInfo);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]partitionInfo=" + partitionInfo);
				}
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		getPartitionColumn(conn, schema);
		getPartitionTables(conn, schema);
		getSubPartitionTables(conn, schema);
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
	protected void buildProcedures(Connection conn, Catalog catalog, Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildProcedures()");
		}
		List<Schema> schemaList = catalog.getSchemas();
		for (Schema sc : schemaList) {
			// get procedures
			List<Procedure> procList = this.getAllProcedures(conn, schema.getName(),
					schema.getName());
			sc.setProcedures(procList);

			// get functions
			List<Function> funcList = getAllFunctions(conn, schema.getName(), schema.getName());
			sc.setFunctions(funcList);
		}
	}

	/**
	 * Fetch all sequences of the given schemata. <br>
	 * SEQUENCE_NAME NOT NULL VARCHAR2(30) <br>
	 * MIN_VALUE NUMBER<br>
	 * MAX_VALUE NUMBER<br>
	 * INCREMENT_BY NOT NULL NUMBER<br>
	 * CYCLE_FLAG VARCHAR2(1)<br>
	 * ORDER_FLAG VARCHAR2(1)<br>
	 * CACHE_SIZE NOT NULL NUMBER<br>
	 * LAST_NUMBER NOT NULL NUMBER<br>
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	protected void buildSequence(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildSequence()");
		}
		PreparedStatement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD

		try {
			stmt = conn.prepareStatement(SQL_SHOW_SEQUENCES);
			stmt.setString(1, schema.getName());
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_SHOW_SEQUENCES + ", " + "1=" + schema.getName() + ", "
						+ "2=" + schema.getName());
			}

			rs = stmt.executeQuery();
			while (rs.next()) {
				String sequenceName = rs.getString("SEQUENCE_NAME");
				if (filter != null && filter.filter(schema.getName(), sequenceName)) {
					continue;
				}
				BigInteger minValue = new BigInteger(rs.getString("MIN_VALUE"));
				BigInteger maxValue = new BigInteger(rs.getString("MAX_VALUE"));
				BigInteger incrementBy = new BigInteger(rs.getString("INCREMENT_BY"));
				BigInteger currentValue = new BigInteger(rs.getString("LAST_NUMBER"));
				boolean cycleFlag = "N".equals(rs.getString("CYCLE_FLAG")) ? false : true;
				int cacheSize = rs.getInt("CACHE_SIZE");
				Sequence seq = factory.createSequence(sequenceName, minValue, maxValue,
						incrementBy, currentValue, cycleFlag, cacheSize);
				seq.setNoMaxValue(false);
				seq.setNoMinValue(false);
				seq.setNoCache(cacheSize <= 1);
				seq.setDDL(getObjectDDL(conn, schema.getName(), sequenceName, OBJECT_TYPE_SEQUENCE));
				seq.setOwner(schema.getName());
				schema.addSequence(seq);
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * Get metadata from SQLTable
	 * 
	 * @param resultSetMeta ResultSetMetaData
	 * @return SourceTable
	 * @throws SQLException e
	 */
	public Table buildSQLTable(ResultSetMetaData resultSetMeta) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildSQLTable()");
		}
		TiberoDataTypeHelper dtHelper = TiberoDataTypeHelper.getInstance(null);
		Table sourceTable = super.buildSQLTable(resultSetMeta);
		List<Column> columns = sourceTable.getColumns();
		for (Column column : columns) {
			if (isNULLType(column.getDataType())) {
				column.setDataType("VARCHAR2");
				column.setJdbcIDOfDataType(Types.VARCHAR);
			}
			column.setShownDataType(dtHelper.getShownDataType(column));
		}
		return sourceTable;
	}

	/**
	 * Extract Table's Columns
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableColumns(final Connection conn, final Catalog catalog,
			final Schema schema, final Table table) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTableColumns()");
		}
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD
		try {
			stmt = conn.prepareStatement(SQL_GET_COLUMNS);
			stmt.setString(1, schema.getName());
			stmt.setString(2, table.getName());
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_GET_COLUMNS + ", 1=" + table.getName() + ", 2="
						+ schema.getName() + ", 3=" + table.getName());
			}
			TiberoDataTypeHelper dtHelper = TiberoDataTypeHelper.getInstance(null);
			rs = stmt.executeQuery();
			while (rs.next()) {
				try {
					// create new column
					final Column column = factory.createColumn();
					String columnName = rs.getString("COLUMN_NAME");
					if (LOG.isDebugEnabled()) {
						LOG.debug("[VAR]columnName=" + columnName);
					}
					column.setName(columnName);
					column.setDataType(rs.getString("DATA_TYPE"));

					// DATA_LENGTH  Length of the column in bytes
					column.setByteLength(rs.getInt("DATA_LENGTH"));
					String precisionStr = rs.getString("DATA_PRECISION");

					column.setPrecision(precisionStr == null ? null : rs.getInt("DATA_PRECISION"));
					String scaleStr = rs.getString("DATA_SCALE");
					column.setScale(scaleStr == null ? null : rs.getInt("DATA_SCALE"));
					//Tibero Integer
					if (column.getDataType().equals("NUMBER") && precisionStr == null
							&& "0".equals(scaleStr)) {
						column.setDataType("INTEGER");
					}
					column.setJdbcIDOfDataType(dtHelper.getJdbcDataTypeID(catalog,
							column.getDataType(), column.getPrecision(), column.getScale()));

					column.setNullable(!"N".equalsIgnoreCase(rs.getString("NULLABLE")));

					// set column default value
					String defaultValue = rs.getString("DATA_DEFAULT");
					// if the data is last,default value add "\n" or "\r" automatically,so trim it
					if (defaultValue != null) {
						defaultValue = defaultValue.trim();
					}
					if ("NULL".equals(defaultValue)) {
						column.setDefaultValue(null);
					} else {
						column.setDefaultValue(defaultValue);
					}
					column.setCharLength(rs.getInt("CHAR_LENGTH"));
					//CHAR_USED: C=varchar2(xx char) B=varchar2(xx)
					column.setCharUsed(rs.getString("CHAR_USED"));
					resetTiberoColumnPrecision(column);

					String shownDataType = dtHelper.getShownDataType(column);
					column.setShownDataType(shownDataType);

					column.setGraphDataType(graphDTHelper.getGraphDataType(
							column.getDataType(), column.getPrecision(), column.getScale()));
					
					table.addColumn(column);
					
				} catch (Exception ex) {
					LOG.error("Read table column information error:" + table.getName(), ex);
				}
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	//	/**
	//	 * get time zone
	//	 * 
	//	 * @param conn Connection
	//	 * @return String time zone
	//	 * @throws SQLException e
	//	 */
	//	public String getTimezone(final Connection conn) throws SQLException {
	//
	//		Statement stmt = null; // NOPMD
	//		ResultSet rs = null; // NOPMD
	//		try {
	//			String timezone = "";
	//			stmt = conn.createStatement();
	//			rs = stmt.executeQuery("select dbtimezone from dual");
	//
	//			if (rs.next()) {
	//				timezone = rs.getString(1);
	//				timezone = "GMT" + timezone;
	//			}
	//
	//			return timezone;
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//		}
	//	}

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
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTableFKs()");
		}
		ResultSet rs = null; //NOPMD
		try {
			final String schemaName = schema == null ? null : schema.getName();
			rs = conn.getMetaData().getImportedKeys(catalog == null ? null : catalog.getName(),
					schemaName, table.getName());
			String fkName = "";
			FK foreignKey = null;

			while (rs.next()) {
				final String newFkName = rs.getString("FK_NAME");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]newFkName=" + newFkName);
				}
				if (fkName.compareToIgnoreCase(newFkName) != 0) {
					if (foreignKey != null) {
						table.addFK(foreignKey);
					}

					fkName = newFkName;
					foreignKey = factory.createFK(table);
					foreignKey.setName(fkName);
					foreignKey.setUpdateRule(FK.ON_UPDATE_NO_ACTION); //tibero doesn't have update rule
					//foreignKey.setDeferability(rs.getInt("DEFERRABILITY"));

					switch (rs.getShort("delete_rule")) {
					case 0:
						foreignKey.setDeleteRule(FK.ON_DELETE_CASCADE);
						break;
					case 1:
						foreignKey.setDeleteRule(FK.ON_DELETE_NO_ACTION);
						break;

					default:
						foreignKey.setDeleteRule(FK.ON_DELETE_SET_NULL);
						break;
					}

					//final String fkSchemaName = rs.getString("PKTABLE_SCHEM");
					//					if (rs.wasNull()) {
					//						foreignKey.setFkSchemaName(schemaName);
					//					} else {
					//						foreignKey.setFkSchemaName(fkSchemaName);
					//					}

					foreignKey.setReferencedTableName(rs.getString("PKTABLE_NAME"));
				}
				if (foreignKey != null) {
					// find reference table column
					final String colName = rs.getString("FKCOLUMN_NAME");
					Column column = table.getColumnByName(colName);
					if (column != null) {
						foreignKey.addRefColumnName(colName, rs.getString("PKCOLUMN_NAME"));
					}
					//					for (int j = 0; j < table.getColumns().size(); j++) {
					//						final Column column = (Column) (table.getColumns().get(j));
					//
					//						if (column.getName().compareToIgnoreCase(colName) == 0) {
					//							foreignKey.addColumn(column);
					//							break;
					//						}
					//					}
				}
			}

			if (foreignKey != null) {
				table.addFK(foreignKey);
			}
		} finally {
			Closer.close(rs);
		}
	}

	/**
	 * Build Table's indexes
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableIndexes(final Connection conn, final Catalog catalog,
			final Schema schema, final Table table) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTableIndexes()");
		}
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD
		try {
			stmt = conn.prepareStatement(SQL_GET_TABLE_INDEX);
			stmt.setString(1, schema.getName());
			stmt.setString(2, table.getName());
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_GET_TABLE_INDEX + ", 1=" + table.getName());
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				String indexName = rs.getString("INDEX_NAME");
				String indexType = rs.getString("INDEX_TYPE");

				Index idx = factory.createIndex(table);
				idx.setName(indexName);
				idx.setUnique("UNIQUE".equals(rs.getString("UNIQUENESS")));

				if ("NORMAL".equals(indexType)) {
					idx.setIndexType(DatabaseMetaData.tableIndexClustered);
				} else if ("NORMAL/REV".equals(indexType)) {
					idx.setReverse(true);
					idx.setIndexType(DatabaseMetaData.tableIndexClustered);
				} else {
					idx.setIndexType(DatabaseMetaData.tableIndexOther);
				}
				table.addIndex(idx);
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]indexes.count="
						+ (table.getIndexes() == null ? null : table.getIndexes()));
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		try {
			stmt = conn.prepareStatement(SQL_GET_INDEX_COLUMNS);
			for (Index idx : table.getIndexes()) {
				stmt.setString(1, schema.getName());
				stmt.setString(2, table.getName());
				stmt.setString(3, idx.getName());
				if (LOG.isDebugEnabled()) {
					LOG.debug("[SQL]" + SQL_GET_INDEX_COLUMNS + ", " + "1=" + table.getName()
							+ ", " + "2=" + idx.getName());
				}
				rs = stmt.executeQuery();
				while (rs.next()) {
					Column col = table.getColumnByName(rs.getString("COLUMN_NAME"));
					String name;
					if (col == null) {
						name = rs.getString("COLUMN_EXPRESSION");
						if (name == null) {
							continue;
						}
						//Some column name may be something like "test"
						if (name.matches("^\"(\\w|\\W|\\d|_)+\"$")) {
							name = name.substring(1, name.length() - 1);
						}
					} else {
						name = col.getName();
					}
					if (name == null) {
						continue;
					}
					String order = rs.getString("DESCEND");
					order = order == null ? "A" : order.toUpperCase(Locale.US);
					idx.addColumn(name, order.startsWith("A"));
				}
				rs.close();
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
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
	protected void buildTriggers(Connection conn, Catalog catalog, Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTriggers()");
		}
		Version version = catalog.getVersion();
		if (version.getDbMajorVersion() < 5) { // 5.0.2 support trigger
			return;
		}

		List<Schema> schemaList = catalog.getSchemas();
		for (Schema sc : schemaList) {
			// get triggers
			List<Trigger> trigList = this.getAllTriggers(conn, schema.getName(), schema.getName());
			sc.setTriggers(trigList);
		}
	}

	/**
	 * Extract View's Columns
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param view View
	 * @throws SQLException e
	 */
	protected void buildViewColumns(final Connection conn, final Catalog catalog,
			final Schema schema, final View view) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildViewColumns()");
		}
		super.buildViewColumns(conn, catalog, schema, view);
		TiberoDataTypeHelper dtHelper = TiberoDataTypeHelper.getInstance(null);
		for (Column column : view.getColumns()) {
			String shownDataType = dtHelper.getShownDataType(column);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]shownDataType=" + shownDataType + ", column=" + column);
			}
			column.setShownDataType(shownDataType);
		}
	}

	/**
	 * Get All Functions
	 * 
	 * @param conn Connection
	 * @param dbName String
	 * @param ownerName String
	 * @return all Functions
	 * @throws SQLException e
	 */
	private List<Function> getAllFunctions(final Connection conn, final String dbName,
			final String ownerName) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getAllFunctions()");
		}
		final List<String> list = this.getRountines(conn, OBJECT_TYPE_FUNCTION, ownerName);
		final List<Function> funcs = new ArrayList<Function>();

		for (String name : list) {
			final Function func = factory.createFunction();
			func.setName(name);
			final String funcDDL = getObjectDDL(conn, dbName, name, OBJECT_TYPE_FUNCTION);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]funcDDL=" + funcDDL);
			}
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
	 * @param ownerName == schema name
	 * @return List<Procedure>
	 * @throws SQLException e
	 */
	private List<Procedure> getAllProcedures(final Connection conn, final String dbName,
			final String ownerName) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getAllProcedures()");
		}
		final List<String> list = this.getRountines(conn, OBJECT_TYPE_PROCEDURE, ownerName);
		final List<Procedure> procs = new ArrayList<Procedure>();

		for (String name : list) {
			final Procedure proc = factory.createProcedure();
			proc.setName(name);
			final String procDDL = getObjectDDL(conn, dbName, name, OBJECT_TYPE_PROCEDURE);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]procDDL=" + procDDL);
			}
			proc.setProcedureDDL(procDDL);
			procs.add(proc);
		}

		return procs;
	}

	//	/**
	//	 * get sequence current value
	//	 * 
	//	 * @param conn Connection
	//	 * @param sequenceName String
	//	 * @return long
	//	 * @throws SQLException e
	//	 */
	//	public long getSequenceCurrentVal(final Connection conn,
	//			final String sequenceName) throws SQLException {
	//		long maxVal = -1;
	//		PreparedStatement stmt = null; // NOPMD
	//		ResultSet rs = null; // NOPMD
	//		try {
	//			stmt = conn.prepareStatement(SHOW_SEQUENCE_MAXVAL);
	//			stmt.setString(1, sequenceName);
	//			rs = stmt.executeQuery();
	//
	//			if (rs.next()) {
	//				maxVal = rs.getLong(1);
	//			}
	//
	//			return maxVal;
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//		}
	//
	//	}

	/**
	 * return a list of tibero table name.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @return List<String>
	 * @throws SQLException e
	 */
	protected List<String> getAllTableNames(final Connection conn, final Catalog catalog,
			final Schema schema) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getAllTableNames()");
		}
		final DatabaseMetaData metaData = conn.getMetaData();
		final ResultSet tables = metaData.getTables(catalog.getName(), schema.getName(), null,
				new String[] { OBJECT_TYPE_TABLE });
		try {
			final String owner = schema.getName();
			List<String> tableNameList = new ArrayList<String>();
			while (tables.next()) {
				tables.getString(1);
				tables.getString(2);
				String name = tables.getString(3);
				if (name.startsWith("BIN$%")) {
					continue;
				}
				tableNameList.add(owner + "." + name);
			}
			return tableNameList;
		} finally {
			Closer.close(tables);
		}
	}

	/**
	 * get All Triggers
	 * 
	 * @param conn Connection
	 * @param dbName the db name
	 * @param ownerName = schema name
	 * @return all triggers
	 * @throws SQLException e
	 */
	private List<Trigger> getAllTriggers(final Connection conn, final String dbName,
			final String ownerName) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getAllTriggers()");
		}
		final List<String> list = this.getRountines(conn, OBJECT_TYPE_TRIGGER, ownerName);
		final List<Trigger> triggers = new ArrayList<Trigger>();

		for (String name : list) {
			final Trigger trigger = factory.createTrigger();
			trigger.setName(name);
			final String trigDDL = getObjectDDL(conn, dbName, name, OBJECT_TYPE_TRIGGER);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]trigDDL=" + trigDDL);
			}
			trigger.setDDL(trigDDL);
			triggers.add(trigger);
		}

		return triggers;
	}

	/**
	 * return a list of view name. for different database, this method may be
	 * needed to override
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @return List<String>
	 * @throws SQLException e
	 */
	protected List<String> getAllViewNames(final Connection conn, final Catalog catalog,
			final Schema schema) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getAllViewNames()");
		}
		List<String> viewNameList = new ArrayList<String>();
		final String owner = schema.getName();
		final ResultSet rs = conn.getMetaData().getTables(catalog.getName(), schema.getName(),
				null, new String[] { OBJECT_TYPE_VIEW });
		try {
			while (rs.next()) {
				String name = rs.getString(3);
				if (name.startsWith("BIN$%") || "USER_SEQUENCES".equals(name)) {
					continue;
				}
				viewNameList.add(owner + "." + name);
			}
			return viewNameList;
		} finally {
			Closer.close(rs);
			//Closer.close(stmt);
		}
	}

	protected DBExportHelper getExportHelper() {
		return DatabaseType.TIBERO.getExportHelper();
	}

	/**
	 * Get TABLE DDL
	 * 
	 * @param conn Connection
	 * @param schemaName String
	 * @param objectName String
	 * @param objectType String
	 * @return String
	 * @throws SQLException e
	 */
	protected String getObjectDDL(final Connection conn, final String schemaName,
			final String objectName, final String objectType) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getObjectDDL()");
		}
		if (StringUtils.isBlank(objectName)) {
			throw new IllegalArgumentException("The Tibero object name is null!");
		}

		PreparedStatement preStmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			preStmt = conn.prepareStatement(SQL_SHOW_DDL);
			preStmt.setString(1, objectType);
			preStmt.setString(2, objectType);
			preStmt.setString(3, schemaName);
			preStmt.setString(4, schemaName);
			preStmt.setString(5, objectType);
			preStmt.setString(6, objectName);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_SHOW_DDL + ", " + "1=" + objectType + ", " + "2="
						+ objectType + ", " + "3=" + schemaName + ", " + "4=" + schemaName + ", "
						+ "5=" + objectType + ", " + "6=" + objectName);
			}
			rs = preStmt.executeQuery();

			String ddl = "";
			while (rs.next()) {
				ddl = rs.getString(1);
			}
			return ddl;
		} catch (Exception ex) {
			LOG.error("Get Tibero Object DDL error:" + objectName, ex);
			return "";
		} finally {
			Closer.close(rs);
			Closer.close(preStmt);
		}
	}

	/**
	 * get partition column information
	 * 
	 * @param conn Connection
	 * @param schema Schema
	 */
	private void getPartitionColumn(final Connection conn, final Schema schema) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getPartitionColumn()");
		}
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD

		try {
			stmt = conn.prepareStatement(SQL_GET_PART_COLUMN);
			stmt.setString(1, schema.getName());
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_GET_PART_COLUMN);
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString("NAME");
				String columnName = rs.getString("COLUMN_NAME");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName + ", columnName=" + columnName);
				}

				Table table = schema.getTableByName(tableName);
				if (table == null) {
					continue;
				}

				PartitionInfo partitionInfo = table.getPartitionInfo();
				partitionInfo.addPartitionColumn(table.getColumnByName(columnName));
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]partitionInfo=" + partitionInfo);
				}
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		try {
			stmt = conn.prepareStatement(SQL_GET_SUBPART_KEY_COLUMN);
			stmt.setString(1, schema.getName());
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_GET_SUBPART_KEY_COLUMN);
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString("NAME");
				String columnName = rs.getString("COLUMN_NAME");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName + ", columnName=" + columnName);
				}
				Table table = schema.getTableByName(tableName);
				if (table == null) {
					continue;
				}
				PartitionInfo partitionInfo = table.getPartitionInfo();
				partitionInfo.addSubPartitionColumn(table.getColumnByName(columnName));
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]partitionInfo=" + partitionInfo);
				}
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * get partition table information
	 * 
	 * @param conn Connection
	 * @param schema Schema
	 */
	private void getPartitionTables(final Connection conn, final Schema schema) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getPartitionTables()");
		}
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD

		try {
			stmt = conn.prepareStatement(SQL_GET_PARTITIONS);
			stmt.setString(1, schema.getName());
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_GET_PARTITIONS + ", 1=" + schema.getName() + ", 2="
						+ schema.getName());
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName);
				}
				Table table = schema.getTableByName(tableName);
				if (table == null) {
					continue;
				}

				String partitionName = rs.getString("PARTITION_NAME");
				Reader reader = rs.getCharacterStream("HIGH_VALUE");
				String partitionDesc = reader == null ? null : DBUtils.reader2String(reader);
				int partitionPosition = rs.getInt("PARTITION_POSITION");

				PartitionInfo partitionInfo = table.getPartitionInfo();
				partitionInfo.setPartitionExp(null);
				partitionInfo.setPartitionFunc(null);

				PartitionTable partition = factory.createPartitionTable();
				partition.setPartitionName(partitionName);
				partition.setPartitionDesc(partitionDesc);
				partition.setPartitionIdx(partitionPosition);

				partitionInfo.addPartition(partition);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]partition=" + partition);
				}
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * Return query text of a view
	 * 
	 * @param conn Connection
	 * @param schemaName schema name
	 * @param viewName String
	 * @return String
	 * @throws SQLException e
	 */
	private String getQueryText(final Connection conn, String schemaName, final String viewName) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getQueryText()");
		}
		ResultSet rs = null; //NOPMD		
		PreparedStatement stmt = null; //NOPMD		
		try {
			stmt = conn.prepareStatement(SQL_SHOW_VIEW_QUERYTEXT);
			stmt.setString(1, schemaName);
			stmt.setString(2, viewName);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_SHOW_VIEW_QUERYTEXT + ", 1=" + schemaName + ", 1="
						+ viewName);
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				return rs.getString("TEXT");
			}

			return null;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	//	/**
	//	 * getSQLTable
	//	 * 
	//	 * @param sql String
	//	 * @param resultSetMeta ResultSetMetaData
	//	 * @return SourceTable
	//	 * @throws SQLException e
	//	 */
	//	public Table getSQLTable(String sql, ResultSetMetaData resultSetMeta) throws SQLException {
	//		List<Column> columns = new ArrayList<Column>();
	//		Table sqlTable = factory.createTable();
	//		sqlTable.setName(sql);
	//		Set<String> columnNames = new HashSet<String>();
	//
	//		for (int i = 1; i < resultSetMeta.getColumnCount() + 1; i++) {
	//			Column column = factory.createColumn();
	//			String tableName = resultSetMeta.getTableName(i);
	//			String columnName = resultSetMeta.getColumnName(i);
	//
	//			if (StringUtils.isNotBlank(tableName)) {
	//				columnName = tableName + "." + columnName;
	//			}
	//
	//			if (columnNames.contains(columnName)) {
	//				columnName = columnName + "1";
	//			}
	//
	//			columnNames.add(columnName);
	//
	//			column.setName(columnName);
	//			column.setCharLength(resultSetMeta.getColumnDisplaySize(i));
	//
	//			column.setTableOrView(sqlTable);
	//			String dataType = resultSetMeta.getColumnTypeName(i);
	//
	//			column.setDataType(dataType);
	//			column.setJdbcIDOfDataType(resultSetMeta.getColumnType(i));
	//			int precision = resultSetMeta.getPrecision(i);
	//			column.setPrecision(precision);
	//
	//			int scale = resultSetMeta.getScale(i);
	//			column.setScale(scale);
	//			column.setAutoIncrement(resultSetMeta.isAutoIncrement(i));
	//			column.setNullable(resultSetMeta.isNullable(i) == ResultSetMetaData.columnNullable);
	//			columns.add(column);
	//
	//			String shownDataType = TiberoDataTypeHelper.getShownDataType(column);
	//			column.setShownDataType(shownDataType);
	//		}
	//
	//		sqlTable.setColumns(columns);
	//		return sqlTable;
	//	}

	/**
	 * get All Routines
	 * 
	 * @param conn Connection
	 * @param type procedure/function
	 * @param ownerName == schema name
	 * @return all Routines names
	 * @throws SQLException e
	 */
	private List<String> getRountines(final Connection conn, final String type,
			final String ownerName) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getRountines()");
		}
		PreparedStatement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			stmt = conn.prepareStatement(SQL_SHOW_ALL_OBJECTS);
			stmt.setString(1, type);
			stmt.setString(2, ownerName);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_SHOW_ALL_OBJECTS + ", " + "1=" + type + ", " + "2="
						+ ownerName + ", " + "3=" + type);
			}
			rs = stmt.executeQuery();
			final Set<String> list = new HashSet<String>();
			while (rs.next()) {
				list.add(rs.getString(1));
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]list=" + (list.size()));
			}
			return new ArrayList<String>(list);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * get sub partition table information
	 * 
	 * @param conn Connection
	 * @param schema Schema
	 */
	private void getSubPartitionTables(final Connection conn, final Schema schema) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getSubPartitionTables()");
		}
		ResultSet rs = null; //NOPMD
		PreparedStatement stmt = null; //NOPMD

		try {
			stmt = conn.prepareStatement(SQL_GET_SUB_PART_TABLES);
			stmt.setString(1, schema.getName());
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + SQL_GET_SUB_PART_TABLES);
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName);
				}
				Table table = schema.getTableByName(tableName);
				if (table == null) {
					continue;
				}

				String subPartitionName = rs.getString("SUBPARTITION_NAME");
				Reader reader = rs.getCharacterStream("HIGH_VALUE");
				String subPartitionDesc = reader == null ? null : DBUtils.reader2String(reader);
				int subPartitionPosition = rs.getInt("SUBPARTITION_POSITION");

				PartitionTable subPartition = factory.createPartitionTable();
				subPartition.setPartitionName(subPartitionName);
				subPartition.setPartitionDesc(subPartitionDesc);
				subPartition.setPartitionIdx(subPartitionPosition);

				table.getPartitionInfo().addSubPartition(subPartition);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]subPartition=" + subPartition);
				}
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * info: DECODE (t.data_precision, null, DECODE (t.data_type, 'CHAR',
	 * t.char_length, 'VARCHAR', t.char_length, 'VARCHAR2', t.char_length,
	 * t.data_length), t.data_precision)
	 * 
	 * @param column Column
	 */
	private void resetTiberoColumnPrecision(Column column) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]resetTiberoColumnPrecision()");
		}
		if (column.getPrecision() == null || column.getPrecision() == 0) {
			String dataType = column.getDataType();

			if (COLUMNS_RESET1.indexOf(dataType) >= 0) {
				column.setPrecision(column.getCharLength());
			} else if (COLUMNS_RESET2.indexOf(dataType) >= 0) {
				column.setPrecision(column.getByteLength());
			}
			//			else if (!"NUMBER".equals(dataType)
			//					&& !"BINARY_FLOAT".equals(dataType)
			//					&& !"BINARY_DOUBLE".equals(dataType)
			//					&& !"DATE".equals(dataType)) {
			//				column.setPrecision(column.getDataLength());
			//			}
		}
	}

	/**
	 * setCatalogTimezone
	 * 
	 * @param catalog Catalog
	 */
	private void setCatalogTimezone(final Catalog catalog) {
		try {
			catalog.setTimezone(TimeZoneUtils.getGMTFormat(TimeZone.getDefault().getID()));
		} catch (Exception ex) {
			LOG.error("", ex);
		}
	}

	/**
	 * get Tibero charset
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 */
	private void setCharset(final Connection conn, final Catalog catalog) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]setCharset()");
		}
		Statement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		try {
			final String sqlStr = "SELECT * FROM NLS_DATABASE_PARAMETERS";
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SQL]" + sqlStr);
			}
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sqlStr);
			while (rs.next()) {
				String key = rs.getString(1);
				String value = rs.getString(2);
				catalog.getAdditionalInfo().put(key, value);
			}
			catalog.setCharset(catalog.getAdditionalInfo().get("NLS_CHARACTERSET"));
		} catch (Exception ex) {
			LOG.error("", ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * 
	 * Tibero schemas; If default schema is specified, it will be returned
	 * directly.
	 * 
	 * @param conn Connection
	 * @param cp ConnParameters
	 * 
	 * @return schema names
	 * @throws SQLException ex;
	 */
	protected List<String> getSchemaNames(Connection conn, ConnParameters cp) throws SQLException {
		List<String> schemaNames = new ArrayList<String>();
		String sql = "SELECT OWNER FROM USER_TAB_PRIVS WHERE PRIVILEGE='SELECT' GROUP BY OWNER";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				schemaNames.add(rs.getString(1));
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		//		if (StringUtils.isNotBlank(cp.getSchema())
		//				&& !schemaNames.contains(cp.getSchema())) {
		//			schemaNames.add(cp.getSchema());
		//		}
		String defaultSchema = cp.getConUser().toUpperCase(Locale.US);
		if (!schemaNames.contains(defaultSchema)) {
			schemaNames.add(defaultSchema);
		}
		return schemaNames;
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.TIBERO;
	}
	//	protected void buildAllSchemas(Connection conn, Catalog catalog, Schema schema, Map<String, Table> tables)
	//			throws SQLException {
	//	}
}