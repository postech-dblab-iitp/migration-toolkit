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
package com.cubrid.cubridmigration.core.dbmetadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObjectFactory;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Version;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.dbtype.IDependOnDatabaseType;
import com.cubrid.cubridmigration.core.engine.exception.JDBCConnectErrorException;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.jdbc.proxy.driver.CUBRIDResultSetProxy;

/**
 * AbstractJDBCSchemaFetcher
 * 
 * @author moulinwang Jessie Huang caoyilin
 * @version 1.0 - 2009-9-15
 */
public abstract class AbstractJDBCSchemaFetcher implements
		IDependOnDatabaseType {
	private final static Logger LOG = LogUtil.getLogger(AbstractJDBCSchemaFetcher.class);

	protected DBObjectFactory factory = null;

	/**
	 * buildCatalog
	 * 
	 * @param conn Connection
	 * @param cp connection parameter
	 * @param filter IBuildSchemaFilter
	 * @return Catalog
	 * @throws SQLException e
	 * 
	 */
	public Catalog buildCatalog(final Connection conn, ConnParameters cp, IBuildSchemaFilter filter) throws SQLException {
		String dbName = cp.getDbName();
		String catalogName;

		DatabaseType databaseType = cp.getDatabaseType();
		if (DatabaseType.ORACLE == databaseType || DatabaseType.ORACLE == databaseType) {
			//If DB name is SID/schemaName pattern
			if (dbName.startsWith("/")) {
				dbName = dbName.substring(1, dbName.length());
			}
			String[] strs = dbName.toUpperCase(Locale.ENGLISH).split("/");
			catalogName = strs[0];
		} else {
			catalogName = cp.getDbName();
		}

		final Catalog catalog = factory.createCatalog();
		catalog.setDatabaseType(databaseType);
		catalog.setName(catalogName);
		catalog.setHost(cp.getHost());
		catalog.setPort(cp.getPort());
		catalog.setConnectionParameters(cp);
		catalog.setVersion(getVersion(conn));
		catalog.setSupportedDataType(getSupportedSqlTypes(conn));
		//Build schema
		List<String> schemas = getSchemaNames(conn, cp);
		if (schemas.isEmpty()) {
			throw new IllegalArgumentException("Invalid schema or no schema specified.");
		}
		for (String schema : schemas) {
			buildSchema(conn, catalog, schema, filter);
		}
		return catalog;

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
	protected List<String> getSchemaNames(final Connection conn, ConnParameters cp) throws SQLException {
		List<String> result = new ArrayList<String>();
		//		if (StringUtils.isNotBlank(cp.getSchema())) {
		//			result.add(cp.getSchema());
		//		} else {
		//			result.add(cp.getDbName());
		//		}
		result.add(cp.getDbName());
		return result;
	}

	/**
	 * return schema names
	 * 
	 * @param cp ConnParameters
	 * 
	 * @return List<String>
	 */
	public List<String> getAllSchemaNames(ConnParameters cp) {
		List<String> result = new ArrayList<String>();
		try {
			ConnParameters cp2 = cp.clone();
			Connection conn = cp2.createConnection();
			try {
				return this.getSchemaNames(conn, cp2);
			} finally {
				conn.close();
			}
		} catch (SQLException e) {
			LOG.error("Get Schema name error", e);
		}
		return result;
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
	protected void buildProcedures(final Connection conn, final Catalog catalog,
			final Schema schema, IBuildSchemaFilter filter) throws SQLException {
		//do nothing
	}

	/**
	 * Build all schemas
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schemaName String
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	private void buildSchema(final Connection conn, final Catalog catalog, String schemaName,
			IBuildSchemaFilter filter) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildSchema()");
		}
		final Schema schema = factory.createSchema();
		schema.setName(schemaName);
		catalog.addSchema(schema);

		// Get Tables
		try {
			buildTables(conn, catalog, schema, filter);
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			LOG.error("buildTables", e);
		}

		try {
			buildViews(conn, catalog, schema, filter);
		} catch (Exception e) {
			LOG.error("buildViews", e);
		}

		// get procedures
		try {
			buildProcedures(conn, catalog, schema, filter);
		} catch (Exception e) {
			LOG.error("buildProcedures", e);
		}

		// get triggers
		try {
			buildTriggers(conn, catalog, schema, filter);
		} catch (Exception e) {
			LOG.error("buildTriggers", e);
		}

		try {
			buildSequence(conn, catalog, schema, filter);
		} catch (Exception e) {
			LOG.error("buildSequence", e);
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
		//do nothing
	}

	/**
	 * Build SQL table's schema by ResultSetMetaData
	 * 
	 * @param resultSetMeta ResultSetMetaData
	 * @return Table schema
	 * @throws SQLException if SQL error
	 */
	public Table buildSQLTable(ResultSetMetaData resultSetMeta) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildSQLTable()");
		}
		List<Column> columns = new ArrayList<Column>();
		Table sqlTable = factory.createTable();

		for (int i = 1; i < resultSetMeta.getColumnCount() + 1; i++) {
			Column column = factory.createColumn();
			column.setTableOrView(sqlTable);
			String columnName = resultSetMeta.getColumnLabel(i); // if it has column alias
			if (StringUtils.isEmpty(columnName)) {
				columnName = resultSetMeta.getColumnName(i);
			}
			column.setName(columnName);
			//			int charLength = resultSetMeta.getColumnDisplaySize(i);
			//			if (charLength <= 0) {
			//				charLength = 1;
			//			}
			column.setJdbcIDOfDataType(resultSetMeta.getColumnType(i));

			int precision = resultSetMeta.getPrecision(i);
			column.setDataType(resultSetMeta.getColumnTypeName(i));
			if (precision <= 0) {
				column.setPrecision(1);
			} else {
				column.setPrecision(precision);
			}
			column.setScale(resultSetMeta.getScale(i));

			column.setNullable(true);
			columns.add(column);
		}

		sqlTable.setColumns(columns);
		return sqlTable;
	}

	/**
	 * Build SQL table's schema;Don't forget to set the returned table's name
	 * 
	 * @param cp source database connection configuration
	 * @param sql the SQL statement
	 * @return Table schema
	 * @throws Exception
	 */
	public Table buildSQLTableSchema(ConnParameters cp, final String sql) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildSQLTableSchema()");
		}
		if (cp == null) {
			throw new RuntimeException("Connnection parameters can't be null!");
		}
		Connection conn = null; // NOPMD
		PreparedStatement stmt = null; // NOPMD
		ResultSet rs = null; // NOPMD
		//Create JDBC connection
		try {
			conn = cp.createConnection(); // NOPMD
		} catch (Exception ex) {
			throw new JDBCConnectErrorException(ex);
		}
		try {
			String cleanSQL = sql == null ? "" : sql;
			if (cleanSQL.endsWith(";")) {
				cleanSQL = cleanSQL.substring(0, cleanSQL.length() - 1);
			}
			final SQLHelper sqlHelper = cp.getDatabaseType().getSQLHelper(null);
			String appendLimit0SQL = sqlHelper.getTestSelectSQL(cleanSQL);
			LOG.info("execute sql:" + appendLimit0SQL);
			stmt = conn.prepareStatement(appendLimit0SQL);
			getExportHelper().configStatement(stmt);
			stmt.setFetchSize(1);
			rs = stmt.executeQuery();
			ResultSetMetaData resultSetMeta = rs.getMetaData();
			return buildSQLTable(resultSetMeta);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
			Closer.close(conn);
		}
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
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTableColumns()");
		}
		ResultSet rs = null; //NOPMD
		try {
			rs = conn.getMetaData().getColumns(getCatalogName(catalog), getSchemaName(schema),
					table.getName(), null);

			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName);
				}
				//Sometimes, the columns may not be the columns we want to get.
				if (!StringUtils.equals(tableName, table.getName())) {
					continue;
				}
				try {
					// create new column
					final Column column = factory.createColumn();
					column.setName(rs.getString("COLUMN_NAME"));
					column.setDataType(rs.getString("TYPE_NAME"));
					column.setJdbcIDOfDataType(rs.getInt("DATA_TYPE"));

					//COLUMN_SIZE int => column size. 
					//For char or date types this is the maximum number of characters, 
					//for numeric or decimal types this is precision. 				
					column.setCharLength(rs.getInt("COLUMN_SIZE"));

					//CHAR_OCTET_LENGTH int => 
					//for char types the maximum number of bytes in the column 
					column.setByteLength(rs.getInt("CHAR_OCTET_LENGTH"));

					column.setPrecision(column.getCharLength());

					//DECIMAL_DIGITS int => 
					//the number of fractional digits
					column.setScale(rs.getInt("DECIMAL_DIGITS"));

					// make sure precision is greater than scale
					if (column.getScale() != null && column.getPrecision() < column.getScale()) {
						column.setPrecision(16);

						if (column.getPrecision() < column.getScale()) {
							column.setPrecision(column.getScale() + 1);
						}
					}
					// prevent VARCHAR(0) columns
					if (column.getDataType().equalsIgnoreCase("VARCHAR")
							&& column.getByteLength() == 0) {
						column.setByteLength(255);
					}

					column.setNullable(rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable);

					// set column default value
					column.setDefaultValue(rs.getString("COLUMN_DEF"));
					column.setAutoIncrement(isYes(rs.getString("IS_AUTOINCREMENT")));

					table.addColumn(column);
				} catch (Exception ex) {
					LOG.error("Read table column information error:" + table.getName(), ex);
				}
			}
		} finally {
			Closer.close(rs);
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
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTableFKs()");
		}
		ResultSet rs = null; //NOPMD
		try {
			rs = conn.getMetaData().getImportedKeys(getCatalogName(catalog), getSchemaName(schema),
					table.getName());
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
					final String fkTableName = rs.getString("PKTABLE_NAME");
					//Ignore invalid foreign key.
					if (StringUtils.isEmpty(fkTableName)) {
						continue;
					}
					
					if (catalog.getDatabaseType().getID() == 1) {
						int major = catalog.getVersion().getDbMajorVersion();
						int miner = catalog.getVersion().getDbMinorVersion();
						
						if ((major * 10 + miner) >= 112) {
							foreignKey.setReferencedTableName(fkTableName.split("\\.")[1]);
						} else {
							foreignKey.setReferencedTableName(fkTableName);
						}
					}
					
					//foreignKey.setDeferability(rs.getInt("DEFERRABILITY"));

					switch (rs.getShort("DELETE_RULE")) {
					case DatabaseMetaData.importedKeyCascade:
						foreignKey.setDeleteRule(DatabaseMetaData.importedKeyCascade);
						break;

					case DatabaseMetaData.importedKeyRestrict:
						foreignKey.setDeleteRule(DatabaseMetaData.importedKeyRestrict);
						break;

					case DatabaseMetaData.importedKeySetNull:
						foreignKey.setDeleteRule(DatabaseMetaData.importedKeySetNull);
						break;

					default:
						foreignKey.setDeleteRule(FK.ON_DELETE_NO_ACTION);
						break;
					}

					switch (rs.getShort("UPDATE_RULE")) {
					case DatabaseMetaData.importedKeyCascade:
						foreignKey.setUpdateRule(DatabaseMetaData.importedKeyCascade);
						break;

					case DatabaseMetaData.importedKeyRestrict:
						foreignKey.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
						break;

					case DatabaseMetaData.importedKeySetNull:
						foreignKey.setUpdateRule(DatabaseMetaData.importedKeySetNull);
						break;

					default:
						foreignKey.setUpdateRule(FK.ON_UPDATE_NO_ACTION);
						break;
					}
				}
				if (foreignKey == null) {
					continue;
				}
				// find reference table column
				final String colName = rs.getString("FKCOLUMN_NAME");
				final Column column = table.getColumnByName(colName);
				if (column != null) {
					foreignKey.addRefColumnName(colName, rs.getString("PKCOLUMN_NAME"));
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
	 * Extract Table's Indexes
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
		try {
			String indexName = "";
			Index index = null;

			rs = conn.getMetaData().getIndexInfo(getCatalogName(catalog), getSchemaName(schema),
					table.getName(), false, true);

			while (rs.next()) {
				final String newIndexName = rs.getString("INDEX_NAME");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]newIndexName=" + newIndexName);
				}

				final String sindexType = rs.getString("TYPE");
				int indexType;
				try {
					indexType = Integer.parseInt(sindexType);
				} catch (Exception ex) {
					indexType = DatabaseMetaData.tableIndexHashed;
				}
				//tableIndexStatistic can't be supported
				if (indexType == DatabaseMetaData.tableIndexStatistic) {
					continue;
				}
				if (newIndexName == null) {
					continue;
				}
				final String columnName = rs.getString("COLUMN_NAME");
				if (columnName == null) {
					continue;
				}
				Column columnByName = table.getColumnByName(columnName);
				if (columnByName == null) {
					continue;
				}
				if (!indexName.equalsIgnoreCase(newIndexName)) {
					if (index != null) {
						table.addIndex(index);
					}
					indexName = newIndexName;

					index = factory.createIndex(table);
					index.setName(indexName);
					index.setIndexType(indexType);
					index.setUnique(!rs.getBoolean("NON_UNIQUE"));
				}
				if (index == null) {
					continue;
				}
				String columnSortRule = rs.getString("ASC_OR_DESC");
				if (StringUtils.isEmpty(columnSortRule)) {
					columnSortRule = "A";
				} else {
					columnSortRule = columnSortRule.toUpperCase(Locale.US);
				}
				index.addColumn(columnByName.getName(), columnSortRule.startsWith("A"));
			}
			if (index != null) {
				table.addIndex(index);
			}

		} finally {
			Closer.close(rs);
		}
	}

	/**
	 * Extract Table's PK
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param table Table
	 * @throws SQLException e
	 */
	protected void buildTablePK(final Connection conn, final Catalog catalog, final Schema schema,
			final Table table) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTablePK()");
		}
		ResultSet rs = null; //NOPMD
		try {

			rs = conn.getMetaData().getPrimaryKeys(getCatalogName(catalog), getSchemaName(schema),
					table.getName());

			PK primaryKey = null;
			String primaryKeyName = null;

			Map<Integer, String> pkColumns = new HashMap<Integer, String>();
			List<Integer> keySeqs = new ArrayList<Integer>();
			while (rs.next()) {
				if (primaryKey == null) {
					primaryKey = factory.createPK(table);
					// add PK
					table.setPk(primaryKey);
					primaryKeyName = rs.getString("PK_NAME");
					primaryKey.setName(primaryKeyName);
				}
				final String columnName = rs.getString("COLUMN_NAME");
				final int idx = rs.getInt("KEY_SEQ");
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]primaryKeyName=" + primaryKeyName + "  columnName="
							+ columnName);
				}
				pkColumns.put(idx, columnName);
				keySeqs.add(idx);
			}
			if (primaryKey != null) {
				Collections.sort(keySeqs);
				for (Integer key : keySeqs) {
					Column col = table.getColumnWithNoCase(pkColumns.get(key));
					if (col == null) {
						continue;
					}
					primaryKey.addColumn(col.getName());
				}
			}
			// remove primary key from list of indices
			if (primaryKeyName != null) {
				final List<Index> indexes = table.getIndexes();
				for (int i = 0; i < indexes.size(); i++) {
					final String indexName = indexes.get(i).getName();
					if (primaryKeyName.compareToIgnoreCase(indexName) == 0) {
						indexes.remove(i);
						break;
					}
				}
			}
		} finally {
			Closer.close(rs);
		}
		setUniquColumnByPK(table);
	}

	/**
	 * 
	 * extract Tables
	 * 
	 * @param conn Connection
	 * @param catalog Catalog
	 * @param schema Schema
	 * @param filter IBuildSchemaFilter
	 * @throws SQLException e
	 */
	protected void buildTables(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildTables()");
		}
		List<String> tableNameList = getAllTableNames(conn, catalog, schema);
		for (String tableName : tableNameList) {
			Table table = null;
			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + tableName);
				}
				//If names format like xxx.xxx means schema name prefixed
				String tableOwnerName = null;
				String tablePureName = null;
				if (tableName != null && tableName.indexOf(".") != -1) {
					String[] arr = tableName.split("\\.");
					tableOwnerName = arr[0];
					tablePureName = arr[1];
				} else {
					tableOwnerName = null;
					tablePureName = tableName;
				}
				if (filter != null && filter.filter(schema.getName(), tablePureName)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("[VAR]tableName=" + tableName + ", skipped object");
					}
					continue;
				}
				table = factory.createTable();
				table.setOwner(tableOwnerName);
				table.setName(tablePureName);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]tableName=" + table.getName() + ", owner=" + table.getOwner());
				}
				table.setSchema(schema);

				buildTableColumns(conn, catalog, schema, table);
				buildTablePK(conn, catalog, schema, table);
				buildTableFKs(conn, catalog, schema, table);
				buildTableIndexes(conn, catalog, schema, table);
				
			} catch (Exception ex) {
				LOG.error("", ex);
			}
			if (table != null) {
				schema.addTable(table);
			}
		}
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
	protected void buildTriggers(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		//do nothing
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
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildViewColumns()");
		}
		ResultSet rs = null; //NOPMD
		try {
			rs = conn.getMetaData().getColumns(getCatalogName(catalog), getSchemaName(schema),
					view.getName(), null);
			while (rs.next()) {
				// create new column
				final Column column = factory.createColumn();
				view.addColumn(column);
				column.setTableOrView(view);
				column.setName(rs.getString("COLUMN_NAME"));
				column.setDataType(rs.getString("TYPE_NAME"));
				column.setJdbcIDOfDataType(rs.getInt("DATA_TYPE"));
				column.setCharLength(rs.getInt("COLUMN_SIZE"));
				column.setPrecision(column.getCharLength());
				column.setScale(rs.getInt("DECIMAL_DIGITS"));
				// make sure precision is greater than scale
				//				if (column.getScale() != null
				//						&& column.getPrecision() < column.getScale()) {
				//					column.setPrecision(16);
				//
				//					if (column.getPrecision() < column.getScale()) {
				//						column.setPrecision(column.getScale() + 1);
				//					}
				//				}
				column.setNullable(rs.getInt("NULLABLE") == java.sql.DatabaseMetaData.columnNullable);
				// prevent VARCHAR(0) columns
				//				if (column.getDataType().equalsIgnoreCase("VARCHAR")
				//						&& column.getCharLength() == 0) {
				//					column.setCharLength(255);
				//				}
				// set column default value
				column.setDefaultValue(rs.getString("COLUMN_DEF"));
				//				LOG.debug("Column Name:" + column.getName() + "  Column Type "
				//						+ column.getDataType() + "  Column Length:"
				//						+ column.getByteLength());
			}
		} finally {
			Closer.close(rs);
		}
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
	protected void buildViews(final Connection conn, final Catalog catalog, final Schema schema,
			IBuildSchemaFilter filter) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]buildViews()");
		}
		List<String> viewNameList = getAllViewNames(conn, catalog, schema);
		for (String viewName : viewNameList) {
			String viewOwnerName = null;
			String viewPureName = null;
			if (viewName != null && viewName.indexOf(".") != -1) {
				String[] arr = viewName.split("\\.");
				viewOwnerName = arr[0];
				viewPureName = arr[1];
			} else {
				viewOwnerName = null;
				viewPureName = viewName;
			}
			if (filter != null && filter.filter(schema.getName(), viewPureName)) {
				continue;
			}
			if (!isViewNameAccepted(viewName)) {
				continue;
			}

			final View view = factory.createView();
			view.setOwner(viewOwnerName);
			view.setName(viewPureName);
			view.setSchema(schema);
			schema.addView(view);
			buildViewColumns(conn, catalog, schema, view);
		}
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
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getAllTableNames()");
		}
		ResultSet rs = null; //NOPMD
		List<String> tableNameList = new ArrayList<String>();
		try {
			rs = conn.getMetaData().getTables(getCatalogName(catalog), getSchemaName(schema), null,
					new String[] {"TABLE"});
			while (rs.next()) {
				tableNameList.add(rs.getString("TABLE_NAME"));
			}
			return tableNameList;
		} finally {
			Closer.close(rs);
		}
	}

	/**
	 * Return a list of view name. for different database, this method may be
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
		ResultSet rs = null; //NOPMD
		List<String> viewNameList = new ArrayList<String>();
		try {
			String schemaName = getSchemaName(schema);
			rs = conn.getMetaData().getTables(getCatalogName(catalog), schemaName, null,
					new String[] {"VIEW"});
			String prefix = StringUtils.isBlank(schemaName) ? "" : schemaName + ".";
			while (rs.next()) {
				viewNameList.add(prefix + rs.getString("TABLE_NAME"));
			}
			return viewNameList;
		} finally {
			Closer.close(rs);
		}
	}

	/**
	 * Get Catalog name
	 * 
	 * @param catalog to be got
	 * @return CatalogName
	 */
	protected String getCatalogName(final Catalog catalog) {
		return catalog == null ? null : catalog.getName();
	}

	//	/**
	//	 * Get Catalogs
	//	 * 
	//	 * @param conn Connection
	//	 * @return List<String> @ e
	//	 */
	//	public List<String> getCatalogs(final Connection conn) {
	//		if (LOG.isDebugEnabled()) {
	//			LOG.debug("[IN]getCatalogs()");
	//		}
	//		final List<String> list = new ArrayList<String>();
	//		ResultSet rs = null; //NOPMD
	//		try {
	//			final DatabaseMetaData metadata = conn.getMetaData();
	//			rs = metadata.getCatalogs();
	//			if (rs != null) {
	//				while (rs.next()) {
	//					list.add(rs.getString("TABLE_CAT"));
	//				}
	//			}
	//			return list;
	//		} catch (SQLException e) {
	//			LOG.error("getCatalogs err:", e);
	//			throw new RuntimeException(e);
	//		} finally {
	//			Closer.close(rs);
	//		}
	//	}

	/**
	 * ORACLE ----------select name,value$ from props$ where name like
	 * 'NLS_CHAR%'; MYSQL ----------SHOW VARIABLES where
	 * variable_name='character_set_database'
	 * 
	 * @param conn Connection
	 * @return String @ e
	 */
	protected String getCharSet(final Connection conn) {
		return null;
	}

	/**
	 * Retrieves the export helper of builder
	 * 
	 * @return DBExportHelper
	 */
	protected abstract DBExportHelper getExportHelper();

	/**
	 * getSchemaName
	 * 
	 * @param schema to be got name
	 * @return getSchemaName
	 */
	protected String getSchemaName(final Schema schema) {
		return schema == null ? null : schema.getName();
	}

	//	/**
	//	 * Returns a list of all schemata from the given JDBC connection
	//	 * 
	//	 * @param conn Connection
	//	 * @return List<String> @ e
	//	 */
	//	public List<String> getSchemata(final Connection conn) {
	//		if (LOG.isDebugEnabled()) {
	//			LOG.debug("[IN]getSchemata()");
	//		}
	//		ResultSet rs = null; //NOPMD
	//		try {
	//			final List<String> schemataList = new ArrayList<String>();
	//
	//			rs = conn.getMetaData().getSchemas();
	//			while (rs.next()) {
	//				final String schemaName = rs.getString("TABLE_SCHEM");
	//				if (schemaName != null) {
	//					schemataList.add(schemaName);
	//				}
	//			}
	//			if (schemataList.isEmpty()) {
	//				schemataList.add("DEFAULT");
	//			}
	//
	//			return schemataList;
	//		} catch (SQLException e) {
	//			LOG.error("getSchemata err:", e);
	//			throw new RuntimeException(e);
	//		} finally {
	//			Closer.close(rs);
	//		}
	//	}

	/**
	 * Get Source Partition DDL
	 * 
	 * @param sourceTable Table
	 * @return String
	 */
	protected String getSourcePartitionDDL(Table sourceTable) {
		String ddl = sourceTable.getDDL();
		if (ddl.indexOf("PARTITION BY") > -1) {
			return ddl.substring(ddl.indexOf("PARTITION BY"), ddl.length());
		}
		return "";
	}

	/**
	 * Get all sql data types
	 * 
	 * @param conn Connection
	 * @return String
	 * @throws SQLException e
	 */
	protected Map<String, List<DataType>> getSupportedSqlTypes(Connection conn) throws SQLException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]getSupportedSqlTypes()");
		}
		ResultSet rs = null; //NOPMD
		try {
			Map<String, List<DataType>> supportedDataType = new HashMap<String, List<DataType>>();
			// Get database metadata
			DatabaseMetaData meta = conn.getMetaData();
			if (meta == null) {
				return supportedDataType;
			}
			// Get type infornmation
			rs = meta.getTypeInfo();
			// Retrieve type info from the result set
			while (rs.next()) {
				String typeName = rs.getString("TYPE_NAME");
				Integer dataType = rs.getInt("DATA_TYPE");
				Long precision = rs.getLong("PRECISION");
				String prefix = rs.getString("LITERAL_PREFIX");
				String suffix = rs.getString("LITERAL_SUFFIX");
				String createParams = rs.getString("CREATE_PARAMS");
				Boolean nullable = rs.getBoolean("NULLABLE");
				Boolean caseSensitive = rs.getBoolean("CASE_SENSITIVE");
				Integer searchable = rs.getInt("SEARCHABLE");
				Boolean unsigned = rs.getBoolean("UNSIGNED_ATTRIBUTE");
				Boolean fixedPrecisionScale = rs.getBoolean("FIXED_PREC_SCALE");
				Boolean autoIncrement = rs.getBoolean("AUTO_INCREMENT");
				Integer minimumScale = rs.getInt("MINIMUM_SCALE");
				Integer maximumScale = rs.getInt("MAXIMUM_SCALE");

				// Get the name of the java.sql.Types value.
				DataType dataTypeObj = factory.createDataType();
				dataTypeObj.setTypeName(typeName);
				dataTypeObj.setJdbcDataTypeID(dataType);
				dataTypeObj.setPrecision(precision);
				dataTypeObj.setPrefix(prefix);
				dataTypeObj.setSuffix(suffix);
				dataTypeObj.setCreateParams(createParams);
				dataTypeObj.setNullable(nullable);
				dataTypeObj.setCaseSensitive(caseSensitive);
				dataTypeObj.setSearchable(searchable);
				dataTypeObj.setUnsigned(unsigned);
				dataTypeObj.setFixedPrecisionScale(fixedPrecisionScale);
				dataTypeObj.setAutoIncrement(autoIncrement);
				dataTypeObj.setMinimumScale(minimumScale);
				dataTypeObj.setMaximumScale(maximumScale);

				if (supportedDataType.containsKey(typeName)) {
					supportedDataType.get(typeName).add(dataTypeObj);
				} else {
					List<DataType> list = new ArrayList<DataType>();
					list.add(dataTypeObj);
					//temp
					if (typeName.equals("VARCHAR2")) {
						supportedDataType.put("VARCHAR", list);
					}
					supportedDataType.put(typeName, list);
				}
			}
			return supportedDataType;
		} finally {
			Closer.close(rs);
		}
	}

	/**
	 * Get version
	 * 
	 * @param conn Connection
	 * @return Version
	 * @throws SQLException e
	 */
	protected Version getVersion(final Connection conn) throws SQLException {
		final DatabaseMetaData metaData = conn.getMetaData();
		final Version version = new Version();
		version.setDbProductName(metaData.getDatabaseProductName());
		version.setDbProductVersion(metaData.getDatabaseProductVersion());
		version.setDbMajorVersion(metaData.getDatabaseMajorVersion());
		version.setDbMinorVersion(metaData.getDatabaseMinorVersion());
		version.setDriverName(metaData.getDriverName());
		version.setDriverVersion(metaData.getDriverVersion());
		version.setDriverMajorVersion(metaData.getDriverMajorVersion());
		version.setDriverMinorVersion(metaData.getDriverMinorVersion());
		return version;
	}

	/**
	 * Retrieves if the column's data type is NULL such as select NULL as ...
	 * from ...
	 * 
	 * @param dataType String
	 * @return true if the data type is NULL
	 */
	protected boolean isNULLType(String dataType) {
		return StringUtils.isBlank(dataType) || "NULL".equalsIgnoreCase(dataType)
				|| "UNKNOWN".equalsIgnoreCase(dataType);
	}

	//	/**
	//	 * build Partitions
	//	 * 
	//	 * @param conn Connection
	//	 * @param catalog Catalog
	//	 * @param schema Schema
	//	 * @throws SQLException e
	//	 */
	//	protected void buildPartitions(final Connection conn,
	//			final Catalog catalog, final Schema schema) throws SQLException {
	//		//do nothing
	//	}

	/**
	 * Return whether a view name is accepted.
	 * 
	 * @param viewName String
	 * @return boolean
	 */
	protected boolean isViewNameAccepted(String viewName) {
		return true;
	}

	/**
	 * Set unique property of column in unique index
	 * 
	 * @param table to be set
	 */
	protected void setUniquColumnByIndex(final Table table) {
		//Set unique
		for (Index idx : table.getIndexes()) {
			if (!idx.isUnique() || idx.getColumnNames().size() != 1) {
				continue;
			}
			Column col = table.getColumnByName(idx.getColumnNames().get(0));
			col.setUnique(true);
		}
	}

	/**
	 * Set unique property of column in unique index
	 * 
	 * @param table to be set
	 */
	protected void setUniquColumnByPK(final Table table) {
		if (table.getPk() == null || table.getPk().getPkColumns().size() != 1) {
			return;
		}
		Column col = table.getColumnByName(table.getPk().getPkColumns().get(0));
		if (col != null) {
			col.setUnique(true);
		}
	}

	/**
	 * Retrieves if the input value is YES or yes
	 * 
	 * @param value column value
	 * @return true if yes.
	 */
	public static final boolean isYes(String value) {
		return "YES".equalsIgnoreCase(value);
	}
}
