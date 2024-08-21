package com.cubrid.cubridmigration.graph.meta;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IBuildSchemaFilter;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObjectFactory;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;

public class TurboSchemaFetcher extends AbstractJDBCSchemaFetcher {
	
	private static final int CUBRID_VARCHAR_MAX_LENGHT = 1073741823;
	
	public TurboSchemaFetcher() {
		factory = new DBObjectFactory();
	}
	
	public DatabaseType getDBType() {
		return DatabaseType.TURBO;
	}

	@Override
	protected DBExportHelper getExportHelper() {
		return DatabaseType.TURBO.getExportHelper();
	}
	
	
	//GDB graphdb build catalog. does GraphDB need catalog?
	@Override
	public Catalog buildCatalog(final Connection conn, ConnParameters cp, IBuildSchemaFilter filter) throws SQLException {
		String dbName = cp.getDbName();
		String catalogName;

		DatabaseType databaseType = cp.getDatabaseType();
		if (DatabaseType.ORACLE == databaseType || DatabaseType.TIBERO == databaseType) {
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
	
	private void buildSchema(final Connection conn, final Catalog catalog, String schemaName,
			IBuildSchemaFilter filter) throws SQLException {
//		if (LOG.isDebugEnabled()) {
//			LOG.debug("[IN]buildSchema()");
//		}
		final Schema schema = factory.createSchema();
		schema.setName(schemaName);
		catalog.addSchema(schema);

		// Get Tables
//		try {
//			buildTables(conn, catalog, schema, filter);
//		} catch (SQLException e) {
//			throw e;
//		} catch (Exception e) {
//			LOG.error("buildTables", e);
//		}
//
//		try {
//			buildViews(conn, catalog, schema, filter);
//		} catch (Exception e) {
//			LOG.error("buildViews", e);
//		}
//
//		// get procedures
//		try {
//			buildProcedures(conn, catalog, schema, filter);
//		} catch (Exception e) {
//			LOG.error("buildProcedures", e);
//		}
//
//		// get triggers
//		try {
//			buildTriggers(conn, catalog, schema, filter);
//		} catch (Exception e) {
//			LOG.error("buildTriggers", e);
//		}
//
//		try {
//			buildSequence(conn, catalog, schema, filter);
//		} catch (Exception e) {
//			LOG.error("buildSequence", e);
//		}
	}
	
	@Override
	public Table buildSQLTable(ResultSetMetaData resultSetMeta) throws SQLException {
//		if (LOG.isDebugEnabled()) {
//			LOG.debug("[IN]buildSQLTable()");
//		}
		List<Column> columns = new ArrayList<Column>();
		Table sqlTable = factory.createTable();

		for (int i = 1; i < resultSetMeta.getColumnCount() + 1; i++) {
			Column column = factory.createColumn();
			column.setTableOrView(sqlTable);
			String columnName = resultSetMeta.getColumnLabel(i); // if it has column alias
			
			String columnType = resultSetMeta.getColumnTypeName(i);
			int columnSize = resultSetMeta.getColumnDisplaySize(i);
			int columnTypeNum = resultSetMeta.getColumnType(i);
			int columnPrecision = resultSetMeta.getPrecision(i);
			
			System.out.println("column type log : " + columnType);
			System.out.println("column size log : " + columnSize);
			System.out.println("column type number log : " + columnTypeNum);
			System.out.println("column precision log : " + columnPrecision);
			
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
				column.setPrecision(CUBRID_VARCHAR_MAX_LENGHT);
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
}
