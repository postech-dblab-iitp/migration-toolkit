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
package com.cubrid.cubridmigration.cubrid.meta;

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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.CommonUtils;
import com.cubrid.cubridmigration.core.common.DBUtils;
import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IBuildSchemaFilter;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObjectFactory;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.PartitionTable;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.TableOrView;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDSQLHelper;
import com.cubrid.cubridmigration.cubrid.dbobj.CUBRIDTrigger;

/**
 * 
 * ReverseEngineeringCUBRIDJdbc
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-15
 */
public final class CUBRIDSchemaFetcher extends
		AbstractJDBCSchemaFetcher {

	private static final String GET_ALLSERIALINFO_SQL = "select name,owner.name,current_val,"
			+ "increment_val,max_val,min_val,cyclic,started,class_name,att_name,cached_num "
			+ "from db_serial where class_name is NULL";

	private static final Map<String, String> STD_TYPE_MAPPING = new HashMap<String, String>();
	static {
		STD_TYPE_MAPPING.put("STRING", "varchar");
	};

	private static final String ALL_TABLE_COLUMN = "SELECT a.class_name, a.attr_name, a.attr_type, a.from_class_name,"
			+ " a.data_type, a.prec, a.scale, a.is_nullable,"
			+ " a.domain_class_name, a.default_value, a.def_order,c.is_reuse_oid_class"
			+ " FROM db_attribute a , db_class c"
			+ " WHERE c.class_name = a.class_name AND c.class_type='CLASS' AND c.is_system_class='NO' and from_class_name is NULL"
			+ " ORDER BY a.class_name, c.class_type, a.def_order";

	private CUBRIDDataTypeHelper cubDTHelper = CUBRIDDataTypeHelper.getInstance(null);

	/**
	 * Retrieves the lower case of type, and some type may be changed into stand
	 * format.
	 * 
	 * @param type String
	 * @return String
	 */
	private String getStdDataType(String type) {
		String newType = STD_TYPE_MAPPING.get(type);
		if (StringUtils.isNotEmpty(newType)) {
			return newType;
		}
		return cubDTHelper.getStdMainDataType(type);
	}

	public CUBRIDSchemaFetcher() {
		factory = new DBObjectFactory() {

			public Trigger createTrigger() {
				return new CUBRIDTrigger();
			}
		};
	}

	/**
	 * buildCatalog
	 * 
	 * @param conn Connection
	 * @param cp ConnParameters
	 * @param filter IBuildSchemaFilter
	 * 
	 * @return Catalog
	 * @throws SQLException e
	 */
	public Catalog buildCatalog(Connection conn, ConnParameters cp, IBuildSchemaFilter filter) throws SQLException {
		Catalog catalog = super.buildCatalog(conn, cp, filter);
		catalog.setDatabaseType(DatabaseType.CUBRID);
		catalog.setCreateSql(null);
		catalog.setTimezone(TimeZoneUtils.getDefaultID2GMT());
		List<Schema> schemaList = catalog.getSchemas();
		CUBRIDSQLHelper ddlUtil = CUBRIDSQLHelper.getInstance(null);

		for (Schema schema : schemaList) {

			List<Table> tableList = schema.getTables();

			for (Table table : tableList) {
				table.setDDL(ddlUtil.getTableDDL(table));
			}

			List<View> viewList = schema.getViews();

			for (View view : viewList) {
				view.setDDL(ddlUtil.getViewDDL(view));
			}
		}

		// get partitions
		buildPartitions(conn, catalog, catalog.getSchemas().get(0));

		return catalog;
	}

	/**
	 * Build CUBRID all tables' columns
	 * 
	 * @param conn Connection
	 * @param tables Map<String, Table>
	 * @throws SQLException ex
	 */
	private void buildCUBRIDTableColumns(Connection conn, Map<String, Table> tables) throws SQLException {
		// get set(object) type information from db_attr_setdomain_elm view
		//Fetch collection type's sub-data type informations
		ResultSet rs = null;
		Statement stmt = null;
		try {
			String sql = "SELECT a.class_name, a.attr_name, a.attr_type,"
					+ " a.data_type, a.prec, a.scale" + " FROM db_attr_setdomain_elm a, db_class c"
					+ " WHERE c.class_name = a.class_name AND c.class_type='CLASS' "
					+ " AND c.is_system_class='NO' " + " ORDER BY a.class_name ";

			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				String tableName = rs.getString("class_name");
				String attrName = rs.getString("attr_name");
				String dataType = rs.getString("data_type");

				//				String type = rs.getString("attr_type");
				Integer precision = rs.getInt("prec");
				Integer scale = rs.getInt("scale");
				dataType = getStdDataType(dataType);

				Table table = tables.get(tableName);
				if (table == null) {
					continue;
				}

				Column cubridColumn = table.getColumnByName(attrName);
				cubridColumn.setSubDataType(dataType);
				cubridColumn.setJdbcIDOfSubDataType(cubDTHelper.getCUBRIDDataTypeID(dataType));
				cubridColumn.setPrecision(precision);
				cubridColumn.setScale(scale);
				cubridColumn.setShownDataType(cubDTHelper.getShownDataType(cubridColumn));
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * Build all table's FKs
	 * 
	 * @param conn Connection
	 * @param tables Map<String, Table>
	 * @param schema Schema
	 * @param catalog Catalog
	 * @throws SQLException ex
	 */
	private void buildCUBRIDTableFKs(Connection conn, Map<String, Table> tables, Schema schema,
			Catalog catalog) throws SQLException {
		// FK
		ResultSet rs = null; //NOPMD
		Statement stmt = null;
		try {
			String sql = "SELECT i.class_name " + "FROM db_index i, db_class c "
					+ "WHERE i.class_name=c.class_name AND c.is_system_class='NO' "
					+ "AND c.class_type='CLASS' AND i.is_foreign_key='YES' "
					+ "GROUP BY i.class_name";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				String tableName = rs.getString("class_name");
				if (tableName == null) {
					continue;
				}

				Table table = tables.get(tableName);
				if (table == null) {
					continue;
				}
				buildTableFKs(conn, catalog, schema, table);
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * Build all tables' indexes
	 * 
	 * @param conn Connection
	 * @param tables Map<String, Table> tables
	 * @throws SQLException ex
	 */
	private void buildCUBRIDTableIndexes(Connection conn, Map<String, Table> tables) throws SQLException {
		// INDEX
		ResultSet rs = null; //NOPMD
		Statement stmt = null;
		try {
			//After CUBRID 9.1.0, function based index supported.
			DatabaseMetaData metaData = conn.getMetaData();
			String jdbcMajorVersion = metaData.getDatabaseProductVersion();
			final String sqlFuncCol;
			boolean supFuncIdx = jdbcMajorVersion.compareToIgnoreCase("9.1.0") >= 0;
			if (supFuncIdx) {
				sqlFuncCol = ", b.func";
			} else {
				sqlFuncCol = "";
			}

			String sql = "SELECT a.class_name, a.index_name, a.is_unique, b.key_attr_name, b.asc_desc "
					+ sqlFuncCol
					+ " FROM db_index a, db_index_key b, db_class c "
					+ "WHERE a.class_name=b.class_name AND c.class_type='CLASS' "
					+ "AND a.index_name=b.index_name AND a.class_name=c.class_name "
					+ "AND c.is_system_class='NO' AND is_primary_key='NO' AND is_foreign_key='NO' "
					+ "ORDER BY a.class_name, b.index_name, b.key_order";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			Map<String, Index> indexes = new HashMap<String, Index>();

			while (rs.next()) {
				String tableName = rs.getString("class_name");
				if (tableName == null) {
					continue;
				}

				Table table = tables.get(tableName);
				if (table == null) {
					continue;
				}

				String indexName = rs.getString("index_name");
				if (indexName == null) {
					continue;
				}

				boolean isUnique = isYes(rs.getString("is_unique"));

				String indexFindKey = tableName + "-" + indexName;
				Index index = indexes.get(indexFindKey);
				if (index == null) {
					index = factory.createIndex(table);
					index.setName(indexName);
					//index.setIndexType(indexType);
					index.setUnique(isUnique);
					table.addIndex(index);
					indexes.put(indexFindKey, index);
				}
				String columnName = rs.getString("key_attr_name");
				if (columnName == null && supFuncIdx) {
					columnName = rs.getString("func");
				}
				String orderRule = rs.getString("asc_desc");
				orderRule = orderRule == null ? "A" : orderRule.toUpperCase(Locale.US);

				index.addColumn(columnName, orderRule.startsWith("A"));
				index.setIndexType(DatabaseMetaData.tableIndexClustered);

				setUniquColumnByIndex(table);
			}
			//Set unique
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

	}

	/**
	 * Build all tables' PKs
	 * 
	 * @param conn Connection
	 * @param tables Map<String, Table>
	 * @throws SQLException ex
	 */
	private void buildCUBRIDTablePKs(Connection conn, Map<String, Table> tables) throws SQLException {
		// PK
		ResultSet rs = null; //NOPMD
		Statement stmt = null;
		try {
			String sql = "SELECT a.class_name, a.index_name, a.is_unique, b.key_attr_name, b.asc_desc "
					+ "FROM db_index a, db_index_key b, db_class c "
					+ "WHERE a.class_name=b.class_name AND a.index_name=b.index_name "
					+ "AND a.class_name=c.class_name AND c.is_system_class='NO' "
					+ "AND a.is_primary_key='YES' AND c.class_type='CLASS' "
					+ "ORDER BY a.class_name, b.key_order";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				String tableName = rs.getString("class_name");
				if (tableName == null) {
					continue;
				}

				Table table = tables.get(tableName);
				if (table == null) {
					continue;
				}

				PK primaryKey = table.getPk();
				if (primaryKey == null) {
					primaryKey = factory.createPK(table);
					table.setPk((PK) primaryKey);
				}

				String primaryKeyName = rs.getString("index_name");
				String columnName = rs.getString("key_attr_name");

				primaryKey.setName(primaryKeyName);

				// find reference table column
				final List<Column> columns = table.getColumns();
				for (int j = 0; j < columns.size(); j++) {
					Column column = columns.get(j);

					if (column.getName().compareToIgnoreCase(columnName) == 0) {
						//column.setUnique(isUnique);
						primaryKey.addColumn(column.getName());
						break;
					}
				}

				setUniquColumnByPK(table);
			}

		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

	}

	/**
	 * Build all tables
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @return Map<String, Table>
	 * @throws SQLException ex
	 */
	private Map<String, Table> buildCUBRIDTables(Connection conn, Catalog catalog, Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		Map<String, Table> tables = new HashMap<String, Table>();
		// get table information
		ResultSet rs = null;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(ALL_TABLE_COLUMN);

			while (rs.next()) {
				String tableName = rs.getString("class_name");
				if (tableName == null) {
					continue;
				}
				if (filter != null && filter.filter(null, tableName)) {
					//CUBRID is one DB one schema
					continue;
				}
				Table table = tables.get(tableName);
				if (table == null) {
					table = factory.createTable();
					table.setName(tableName);
					table.setReuseOID(isYes(rs.getString("is_reuse_oid_class")));
					schema.addTable(table);
					tables.put(tableName, table);
				}

				String attrName = rs.getString("attr_name");
				boolean isShared = "SHARED".equals(rs.getString("attr_type"));
				String dataTypeInView = rs.getString("data_type");
				String domainClassName = rs.getString("domain_class_name");
				Integer prec = rs.getInt("prec");
				Integer scale = rs.getInt("scale");

				Column column = factory.createColumn();
				column.setName(attrName);
				column.setShared(isShared);
				if (cubDTHelper.isObjectType(dataTypeInView)) {
					column.setDataType(domainClassName);
				} else {
					String standardDataType = getStdDataType(dataTypeInView);
					column.setDataType(standardDataType);
					column.setJdbcIDOfDataType(cubDTHelper.getCUBRIDDataTypeID(standardDataType));
				}
				column.setPrecision(prec);
				column.setScale(scale);

				String isNull = rs.getString("is_nullable");
				column.setNullable(isYes(isNull));

				String defaultValue = rs.getString("default_value");
				if (column.isShared()) {
					column.setSharedValue(defaultValue);
					column.setDefaultValue(null);
				} else {
					column.setSharedValue(null);
					column.setDefaultValue(defaultValue);
				}
				if (cubDTHelper.isEnum(dataTypeInView)) {
					String realDataType = fetchEnumType(conn, tableName, attrName);
					DataTypeInstance dti = cubDTHelper.parseDTInstance(realDataType);
					column.setDataTypeInstance(dti);
				}
				if (!cubDTHelper.isCollection(dataTypeInView)) {
					column.setShownDataType(cubDTHelper.getShownDataType(column));
				}
				table.addColumn(column);
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
		return tables;
	}

	/**
	 * Build all tables' columns' serials
	 * 
	 * @param conn Connection
	 * @param tables Map<String, Table>
	 * @throws SQLException ex
	 */
	private void buildCUBRIDTableSerials(Connection conn, Map<String, Table> tables) throws SQLException {
		ResultSet rs = null; //NOPMD
		Statement stmt = null;
		// SERIAL
		try {
			String sql = "SELECT class_name,name,owner.name,current_val,increment_val,"
					+ "max_val,min_val,cyclic,started,att_name " + "FROM db_serial "
					+ "WHERE class_name IS NOT NULL " + "ORDER BY class_name";

			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				String tableName = rs.getString("class_name");
				if (tableName == null) {
					continue;
				}

				String currentVal = rs.getString("current_val");
				String attrName = rs.getString("att_name");

				Table table = tables.get(tableName);
				if (table == null) {
					continue;
				}

				final Column column = table.getColumnByName(attrName);
				if (column == null) {
					continue;
				}
				column.setAutoIncrement(true);
				Long incrementVal = rs.getLong("increment_val");
				incrementVal = incrementVal == null ? 1 : incrementVal;
				column.setAutoIncIncrVal(incrementVal);
				//To avoid PK conflict if the column is auto increment.
				column.setAutoIncSeedVal(CommonUtils.str2Long(currentVal) + 1);
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
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
		String sql = "SELECT * FROM db_partition";
		ResultSet rs = null; //NOPMD
		Statement stmt = null; //NOPMD
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			List<Table> partitionTables = new ArrayList<Table>();
			while (rs.next()) {
				String tableName = rs.getString("class_name");
				Table table = schema.getTableByName(tableName);

				if (table == null) {
					continue;
				}

				String partitionMethod = rs.getString("partition_type");
				String partitionExpr = rs.getString("partition_expr");
				Object partitionValues = rs.getObject("partition_values");
				String partitionDesc = null;
				if ("RANGE".equals(partitionMethod)) {
					if (partitionValues == null) {
						continue;
					}
					Object[] array = (Object[]) partitionValues;

					partitionDesc = String.valueOf(array[array.length - 1]);
					if ("null".equals(partitionDesc)) {
						partitionDesc = "MAXVALUE";
					} else if (partitionDesc.matches("\\d\\d\\d\\d-\\d\\d-\\d\\d.*")) {
						//If value is data time type
						partitionDesc = "'" + partitionDesc + "'";
					}
				} else if ("LIST".equals(partitionMethod)) {
					Object[] array = (Object[]) partitionValues;
					StringBuffer bf = new StringBuffer();

					for (Object obj : array) {
						bf.append(",").append(String.valueOf(obj));
					}

					partitionDesc = bf.substring(1);
				} else if ("HASH".equals(partitionMethod)) {
					partitionDesc = null;
				}

				PartitionInfo partitionInfo = table.getPartitionInfo();

				if (partitionInfo == null) {
					partitionInfo = factory.createPartitionInfo();
					table.setPartitionInfo(partitionInfo);

					partitionTables.add(table);
				}

				partitionInfo.setPartitionMethod(partitionMethod);
				partitionInfo.setPartitionExp(partitionExpr);
				partitionInfo.setPartitionFunc(DBUtils.parsePartitionFunc(partitionExpr));
				partitionInfo.setPartitionColumns(DBUtils.parsePartitionColumns(table,
						partitionExpr));

				partitionInfo.setSubPartitionMethod(null);
				partitionInfo.setSubPartitionExp(null);
				partitionInfo.setSubPartitionFunc(null);

				String partitionName = rs.getString("partition_name");
				PartitionTable partitionTable = factory.createPartitionTable();
				partitionTable.setPartitionName(partitionName);
				partitionTable.setPartitionDesc(partitionDesc);
				int partitionIdx = partitionInfo.getPartitions().size() + 1;
				partitionTable.setPartitionIdx(partitionIdx);

				partitionInfo.setPartitionCount(partitionIdx);
				partitionInfo.addPartition(partitionTable);
			}

			CUBRIDSQLHelper util = CUBRIDSQLHelper.getInstance(null);
			for (Table tbl : partitionTables) {
				tbl.getPartitionInfo().setDDL(util.getTablePartitonDDL(tbl));
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
	protected void buildProcedures(Connection conn, Catalog catalog, Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		List<Schema> schemaList = catalog.getSchemas();

		for (Schema sc : schemaList) {
			// get procedures
			List<Procedure> procList = getAllProcedures(conn);
			sc.setProcedures(procList);

			// get functions
			List<Function> funcList = getAllFunctions(conn);
			sc.setFunctions(funcList);
		}
	}

	/**
	 * Fetch all sequences of the given schemata.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	protected void buildSequence(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		Statement stmt = null; //NOPMD
		ResultSet rs = null; //NOPMD
		List<Sequence> sequenceList = new ArrayList<Sequence>();

		try {

			stmt = conn.createStatement();
			rs = stmt.executeQuery(GET_ALLSERIALINFO_SQL);
			while (rs.next()) {
				String sequenceName = rs.getString("name");
				if (filter != null && filter.filter(schema.getName(), sequenceName)) {
					continue;
				}
				String currentVal = rs.getString("current_val");
				String incrementVal = rs.getString("increment_val");
				String maxVal = rs.getString("max_val");
				String minVal = rs.getString("min_val");
				String cyclic = rs.getString("cyclic");
				int cachedNum = rs.getInt("cached_num");

				boolean isCycle = "1".equals(cyclic);

				Sequence sequence = factory.createSequence(sequenceName, new BigInteger(minVal),
						new BigInteger(maxVal), new BigInteger(incrementVal), new BigInteger(
								currentVal), isCycle, cachedNum);
				String ddl = CUBRIDSQLHelper.getInstance(null).getSequenceDDL(sequence);
				sequence.setDDL(ddl);

				sequenceList.add(sequence);

			}
			schema.setSequenceList(sequenceList);

		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

	}

	// below are the mappings of data type which from querying system table to
	// standard data type

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
		for (Column column : columns) {
			if (isNULLType(column.getDataType())) {
				column.setDataType("varchar");
				column.setJdbcIDOfDataType(Types.VARCHAR);
			}
			column.setJdbcIDOfDataType(cubDTHelper.getCUBRIDDataTypeID(column.getDataType()));
			column.setShownDataType(cubDTHelper.getShownDataType(column));
		}

		return sourceTable;
	}

	/**
	 * get Table's Columns
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableColumns(Connection conn, Catalog catalog, Schema schema, Table table) throws SQLException {
		//buildTableOrViewColumns(conn, table);
	}

	//	/**
	//	 * extract Table's FK Notes: for no referenced table can not be accessed, so
	//	 * FKs are not built.
	//	 * 
	//	 * @param conn Connection
	//	 * @param catalog Catalog
	//	 * @param schema Schema
	//	 * @param table Table
	//	 * @throws SQLException e
	//	 */
	//	protected void buildTableFKs(Connection conn, Catalog catalog,
	//			Schema schema, Table table) throws SQLException {
	//		super.buildTableFKs(conn, catalog, schema, table);
	//	}

	/**
	 * extract Table's Indexes
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTableIndexes(Connection conn, Catalog catalog, Schema schema, Table table) throws SQLException {
		//		ResultSet rs = null; //NOPMD
		//		PreparedStatement stmt = null;
		//
		//		String sqlStr = "SELECT a.index_name,a.is_unique,b.key_attr_name,b.asc_desc "
		//				+ "FROM db_index a,db_index_key b WHERE a.class_name=b.class_name AND a.index_name=b.index_name "
		//				+ "AND a.class_name=? AND is_primary_key='NO' AND is_foreign_key='NO' ORDER BY b.index_name, b.key_order";
		//		String tableName = table.getName();
		//
		//		try {
		//
		//			stmt = conn.prepareStatement(sqlStr);
		//			stmt.setString(1, tableName);
		//			rs = stmt.executeQuery();
		//
		//			String indexName = "";
		//			Index index = null;
		//
		//			while (rs.next()) {
		//				String newIndexName = rs.getString("index_name");
		//				boolean isUnique = isYes(rs.getString("is_unique"));
		//
		//				if (newIndexName == null) {
		//					continue;
		//				}
		//
		//				if (!indexName.equalsIgnoreCase(newIndexName)) {
		//					if (index != null) {
		//						table.addIndex(index);
		//					}
		//
		//					indexName = newIndexName;
		//
		//					index = factory.createIndex(table);
		//					index.setName(indexName);
		//
		//					//index.setIndexType(indexType);
		//
		//					index.setUnique(isUnique);
		//				}
		//
		//				String columnName = rs.getString("key_attr_name");
		//				String orderRule = rs.getString("asc_desc");
		//
		//				if (index == null) {
		//					continue;
		//				}
		//				// find reference table column
		//				orderRule = orderRule == null ? "A"
		//						: orderRule.toUpperCase(Locale.US);
		//				index.addColumn(columnName, orderRule.startsWith("A"));
		//				index.setIndexType(DatabaseMetaData.tableIndexClustered);
		//			}
		//
		//			if (index != null) {
		//				table.addIndex(index);
		//			}
		//			//Set unique
		//		} finally {
		//			Closer.close(rs);
		//			Closer.close(stmt);
		//		}
		//		setUniquColumnByIndex(table);
	}

	/**
	 * get Columns
	 * 
	 * @param conn Connection
	 * @param table TableOrView
	 * @throws SQLException e
	 */
	private void buildTableOrViewColumns(Connection conn, TableOrView table) throws SQLException {
		ResultSet rs = null; //NOPMD
		PreparedStatement preStmt = null;
		String tableName = table.getName();
		try {
			// get table information
			String sql = "SELECT a.attr_name, a.attr_type, a.from_class_name,"
					+ " a.data_type, a.prec, a.scale, a.is_nullable, "
					+ " a.domain_class_name, a.default_value, a.def_order"
					+ " FROM db_attribute a WHERE a.class_name=? " + " order by a.def_order";

			preStmt = conn.prepareStatement(sql);
			preStmt.setString(1, tableName);
			rs = preStmt.executeQuery();
			while (rs.next()) {
				String attrName = rs.getString("attr_name");
				boolean isShared = "SHARED".equals(rs.getString("attr_type"));
				String dataTypeInView = rs.getString("data_type");
				String domainClassName = rs.getString("domain_class_name");
				Integer prec = rs.getInt("prec");
				Integer scale = rs.getInt("scale");

				String isNull = rs.getString("is_nullable");

				String defaultValue = rs.getString("default_value");

				Column column = factory.createColumn();
				column.setName(attrName);
				column.setShared(isShared);
				if ("OBJECT".equals(dataTypeInView)) {
					column.setDataType(domainClassName);
				} else {
					String standardDataType = getStdDataType(dataTypeInView);
					column.setDataType(standardDataType);
					column.setJdbcIDOfDataType(cubDTHelper.getCUBRIDDataTypeID(standardDataType));
				}

				column.setPrecision(prec);
				column.setScale(scale);
				table.addColumn(column);

				if (isYes(isNull)) { // null
					column.setNullable(true);
				} else {
					column.setNullable(false);
				}
				if (column.isShared()) {
					column.setSharedValue(defaultValue);
					column.setDefaultValue(null);
				} else {
					column.setSharedValue(null);
					column.setDefaultValue(defaultValue);
				}

				if (!cubDTHelper.isCollection(dataTypeInView)) {
					column.setShownDataType(cubDTHelper.getShownDataType(column));
				}

			}
		} finally {
			Closer.close(rs);
			Closer.close(preStmt);
		}

		try {
			// get set(object) type information from db_attr_setdomain_elm view
			String sql = "SELECT a.attr_name, a.attr_type," + " a.data_type, a.prec, a.scale"
					+ " FROM db_attr_setdomain_elm a" + " WHERE a.class_name=? ";

			preStmt = conn.prepareStatement(sql);
			preStmt.setString(1, tableName);
			rs = preStmt.executeQuery();
			while (rs.next()) {
				String attrName = rs.getString("attr_name");
				String dataType = rs.getString("data_type");

				//				String type = rs.getString("attr_type");
				Integer precision = rs.getInt("prec");
				Integer scale = rs.getInt("scale");
				dataType = getStdDataType(dataType);

				Column cubridColumn = table.getColumnByName(attrName);
				cubridColumn.setSubDataType(dataType);
				cubridColumn.setJdbcIDOfSubDataType(cubDTHelper.getCUBRIDDataTypeID(dataType));
				cubridColumn.setPrecision(precision);
				cubridColumn.setScale(scale);
				cubridColumn.setShownDataType(cubDTHelper.getShownDataType(cubridColumn));
			}
			// get auto increment information from db_serial table, which is a
			// system table accessed by all users

		} finally {
			Closer.close(rs);
			Closer.close(preStmt);
		}

		try {
			String sql = "select name,owner.name,current_val,increment_val,max_val,min_val,cyclic,started,class_name,att_name "
					+ "from db_serial where class_name =?";
			preStmt = conn.prepareStatement(sql);
			preStmt.setString(1, tableName);
			rs = preStmt.executeQuery();

			while (rs.next()) {

				String currentVal = rs.getString("current_val");

				String attrName = rs.getString("att_name");

				final Column column = table.getColumnByName(attrName);
				if (column == null) {
					continue;
				}
				column.setAutoIncrement(true);
				Long incrementVal = rs.getLong("increment_val");
				incrementVal = incrementVal == null ? 1 : incrementVal;
				column.setAutoIncIncrVal(incrementVal);
				//To avoid PK conflict if the column is auto increment.
				column.setAutoIncSeedVal(CommonUtils.str2Long(currentVal) + 1);
			}
		} finally {
			Closer.close(rs);
			Closer.close(preStmt);
		}

	}

	/**
	 * 
	 * extract Table's PK
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTablePK(Connection conn, Catalog catalog, Schema schema, Table table) throws SQLException {
		//		ResultSet rs = null; //NOPMD
		//		PreparedStatement stmt = null;
		//		try {
		//			String sqlStr = "SELECT a.index_name,a.is_unique,b.key_attr_name,b.asc_desc "
		//					+ "FROM db_index a,db_index_key b WHERE a.class_name=b.class_name AND a.index_name=b.index_name "
		//					+ "AND a.class_name=? AND is_primary_key='YES' ORDER BY b.key_order";
		//			String tableName = table.getName();
		//			stmt = conn.prepareStatement(sqlStr);
		//			stmt.setString(1, tableName);
		//			rs = stmt.executeQuery();
		//
		//			PK primaryKey = null;
		//			while (rs.next()) {
		//				if (primaryKey == null) {
		//					primaryKey = factory.createPK(table);
		//					String primaryKeyName = rs.getString("index_name");
		//					primaryKey.setName(primaryKeyName);
		//					table.setPk((PK) primaryKey);
		//				}
		//				String columnName = rs.getString("key_attr_name");
		//				// find reference table column
		//				Column column = table.getColumnWithNoCase(columnName);
		//				if (column != null) {
		//					primaryKey.addColumn(column.getName());
		//				}
		//			}
		//		} finally {
		//			Closer.close(rs);
		//			Closer.close(stmt);
		//		}
		//		setUniquColumnByPK(table);
	}

	/**
	 * 
	 * extract Tables, set table's reuseOID properties.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	protected void buildTables(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		// Fastest gathering schema meta data
		Map<String, Table> tables = buildCUBRIDTables(conn, catalog, schema, filter);
		buildCUBRIDTableColumns(conn, tables);
		buildCUBRIDTableSerials(conn, tables);
		buildCUBRIDTablePKs(conn, tables);
		buildCUBRIDTableFKs(conn, tables, schema, catalog);
		buildCUBRIDTableIndexes(conn, tables);
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
		String user = conn.getMetaData().getUserName();
		if (null != user && "DBA".equalsIgnoreCase(user)) {
			// get triggers
			List<Trigger> trigList = getAllTriggers(conn);
			schema.setTriggers(trigList);
		}
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
		buildTableOrViewColumns(conn, view);
	}

	/**
	 * Fetch all views of the given schemata.
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	protected void buildViews(Connection conn, Catalog catalog, Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		super.buildViews(conn, catalog, schema, filter);
		//Set view's DDL
		String sqlStr = "SELECT vclass_def FROM db_vclass WHERE vclass_name=?";
		PreparedStatement stmt = conn.prepareStatement(sqlStr);
		try {
			for (View view : schema.getViews()) {
				ResultSet rs = null; //NOPMD
				try {
					stmt.setString(1, view.getName());
					rs = stmt.executeQuery();
					while (rs.next()) {
						String querySpec = rs.getString(1);
						view.setQuerySpec(querySpec);
					}
				} finally {
					Closer.close(rs);
				}
			}
		} finally {
			Closer.close(stmt);
		}
	}

	/**
	 * creatSPDDL
	 * 
	 * @param map params
	 * @return ddl
	 * @throws SQLException e
	 */
	private String creatSPDDL(Map<String, Object> map) throws SQLException {
		StringBuffer buf = new StringBuffer(50);
		buf.append("CREATE ");
		buf.append(map.get("SP_TYPE"));
		buf.append(" \"").append(map.get("SP_NAME")).append("\" (");
		buf.append(map.get("PARAMS")).append(") ");

		if (((String) map.get("RETURN_TYPE")).equalsIgnoreCase("void")) {
			buf.append("RETURN ");
			buf.append(map.get("RETURN_TYPE")).append(" \r\n");
		} else {
			buf.append(" \r\n");
		}

		buf.append("AS LANGUAGE ").append(map.get("LANG")).append(" \r\n");
		buf.append("NAME \'").append(map.get("TARGET")).append("\'");

		return buf.toString();
	}

	/**
	 * Fetch enum data type with elements from real database.
	 * 
	 * @param conn Connection
	 * @param tableName String
	 * @param columnName String
	 * @return String enum data type with elements
	 */
	private String fetchEnumType(Connection conn, String tableName, String columnName) {
		StringBuilder sb = new StringBuilder();
		sb.append("SHOW COLUMNS FROM [").append(tableName).append("] WHERE FIELD='").append(
				columnName).append("'");
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sb.toString());
			if (rs.next()) {
				return rs.getString("Type");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
		return null;
	}

	/**
	 * get All Functions
	 * 
	 * @param conn Connection
	 * @return all Functions
	 * @throws SQLException e
	 */
	private List<Function> getAllFunctions(Connection conn) throws SQLException {
		Map<String, String> map = this.getRountines(conn, "FUNCTION");
		List<Function> funcs = new ArrayList<Function>();

		for (Entry<String, String> entry : map.entrySet()) {
			Function func = factory.createFunction();
			func.setName(entry.getKey());
			func.setFuncDDL(entry.getValue());
			funcs.add(func);
		}

		return funcs;
	}

	/**
	 * get All Procedures
	 * 
	 * @param conn Connection
	 * @return all Procedures
	 * @throws SQLException e
	 */
	private List<Procedure> getAllProcedures(Connection conn) throws SQLException {
		Map<String, String> map = this.getRountines(conn, "PROCEDURE");
		List<Procedure> procedures = new ArrayList<Procedure>();

		for (Entry<String, String> entry : map.entrySet()) {
			Procedure procedure = factory.createProcedure();
			procedure.setName(entry.getKey());
			procedure.setProcedureDDL(entry.getValue());
			procedures.add(procedure);
		}

		return procedures;
	}

	/**
	 * return a list of table name. for different database, this method may be
	 * needed to override
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @return List<String>
	 * @throws SQLException e
	 */
	protected List<String> getAllTableNames(final Connection conn, final Catalog catalog,
			final Schema schema) throws SQLException {
		PreparedStatement stmt = null;
		ResultSet rs = null; //NOPMD

		List<String> tableNames = new ArrayList<String>();
		try {
			String sqlStr = "SELECT CLASS_NAME FROM db_class "
					+ "WHERE is_system_class = 'NO' and class_type = 'CLASS' order by CLASS_NAME";
			stmt = conn.prepareStatement(sqlStr);
			rs = stmt.executeQuery();

			while (rs.next()) {
				tableNames.add(rs.getString("CLASS_NAME"));
			}

		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}

		try {
			String sqlStr = "SELECT  partition_class_name  FROM db_partition";
			stmt = conn.prepareStatement(sqlStr);
			rs = stmt.executeQuery();

			while (rs.next()) {
				tableNames.remove(rs.getString("partition_class_name").toLowerCase(Locale.ENGLISH));
			}

			return tableNames;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

	/**
	 * get All Triggers
	 * 
	 * @param conn Connection
	 * @return all triggers
	 * @throws SQLException e
	 */
	private List<Trigger> getAllTriggers(Connection conn) throws SQLException {
		PreparedStatement stmt = null;
		ResultSet rs = null; //NOPMD
		try {
			String sqlStr = "SELECT T.TARGET_CLASS_NAME,NAME,STATUS,PRIORITY,EVENT,"
					+ "TARGET_CLASS,TARGET_ATTRIBUTE,CONDITION_TYPE,CONDITION,CONDITION_TIME,"
					+ "TRIG.ACTION_TYPE,ACTION_DEFINITION,TRIG.ACTION_TIME "
					+ "FROM DB_CLASS C,DB_TRIGGER TRIG,DB_TRIG T "
					+ "WHERE TRIG.NAME=T.TRIGGER_NAME AND T.TARGET_CLASS_NAME=C.CLASS_NAME(+) "
					+ "AND C.IS_SYSTEM_CLASS='NO' ORDER BY NAME";
			stmt = conn.prepareStatement(sqlStr);
			rs = stmt.executeQuery();
			List<Trigger> triggers = new ArrayList<Trigger>();

			while (rs.next()) {
				CUBRIDTrigger trigger = (CUBRIDTrigger) factory.createTrigger();
				trigger.setTargetClass(rs.getString("TARGET_CLASS_NAME"));
				trigger.setName(rs.getString("NAME"));
				trigger.setStatus(rs.getString("STATUS"));
				trigger.setPriority(rs.getString("PRIORITY"));
				trigger.setEventType(rs.getString("EVENT"));
				trigger.setTargetAttribute(rs.getString("TARGET_ATTRIBUTE"));
				trigger.setCondition(rs.getString("CONDITION"));
				trigger.setConditionTime(rs.getString("CONDITION_TIME"));
				trigger.setActionType(rs.getString("ACTION_TYPE"));
				trigger.setActionDefintion(rs.getString("ACTION_DEFINITION"));
				trigger.setActionTime(rs.getString("ACTION_TIME"));

				triggers.add(trigger);
			}

			return triggers;
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
		return DatabaseType.CUBRID;
	}

	protected DBExportHelper getExportHelper() {
		return DatabaseType.CUBRID.getExportHelper();
	}

	/**
	 * get All Rountines
	 * 
	 * @param conn Connection
	 * @param spType procedure/function
	 * @return all Rountines
	 * @throws SQLException e
	 */
	private Map<String, String> getRountines(Connection conn, String spType) throws SQLException {
		PreparedStatement stmt = null;
		ResultSet rs = null; //NOPMD
		try {
			String sqlStr = "SELECT SP.SP_NAME,SP.SP_TYPE,SP.RETURN_TYPE,"
					+ "SP.ARG_COUNT,SP.LANG,SP.TARGET,SP.OWNER,SPARGS.INDEX_OF,"
					+ "SPARGS.ARG_NAME,SPARGS.DATA_TYPE,SPARGS.MODE "
					+ "FROM DB_STORED_PROCEDURE SP "
					+ "LEFT OUTER JOIN DB_STORED_PROCEDURE_ARGS SPARGS "
					+ "ON SP.SP_NAME=SPARGS.SP_NAME WHERE SP.SP_TYPE=? "
					+ "ORDER BY SP.SP_NAME ASC,SPARGS.INDEX_OF ASC";
			stmt = conn.prepareStatement(sqlStr);
			stmt.setString(1, spType);
			rs = stmt.executeQuery();

			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

			Map<String, Object> map = new HashMap<String, Object>();
			String spName = "";

			while (rs.next()) {
				String str = "";

				if (rs.getString("ARG_NAME") != null) {
					str = "\"" + rs.getString("ARG_NAME") + "\" ";

					if (!rs.getString("MODE").equalsIgnoreCase("IN")) { // IN,OUT,IN OUT,INOUT
						str += rs.getString("MODE") + " ";
					}

					str += rs.getString("DATA_TYPE");
				}

				if (spName.equals(rs.getString("SP_NAME"))) {
					String tmp = (String) map.get("PARAMS");

					if (str != null) {
						map.put("PARAMS", tmp + "," + str);
					}
				} else {
					if (!map.isEmpty()) {
						Map<String, Object> tmpMap = new HashMap<String, Object>();
						tmpMap.putAll(map);
						list.add(tmpMap);
					}

					map = new HashMap<String, Object>();
					spName = rs.getString("SP_NAME");
					map.put("SP_NAME", rs.getString("SP_NAME"));
					map.put("SP_TYPE", rs.getString("SP_TYPE"));
					map.put("RETURN_TYPE", rs.getString("RETURN_TYPE"));
					map.put("ARG_COUNT", rs.getInt("ARG_COUNT"));
					map.put("LANG", rs.getString("LANG"));
					map.put("TARGET", rs.getString("TARGET"));
					map.put("OWNER", rs.getString("OWNER"));
					map.put("INDEX_OF", rs.getInt("INDEX_OF"));
					map.put("DATA_TYPE", rs.getString("DATA_TYPE"));
					map.put("PARAMS", str);

				}
			}

			if (!map.isEmpty()) {
				Map<String, Object> tmpMap = new HashMap<String, Object>();
				tmpMap.putAll(map);
				list.add(tmpMap);
			}

			Map<String, String> spMap = new LinkedHashMap<String, String>();

			for (Map<String, Object> tmpMap : list) {
				String ddl = this.creatSPDDL(tmpMap);
				spMap.put((String) tmpMap.get("SP_NAME"), ddl);
			}

			return spMap;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
		}
	}

}