package com.cubrid.cubridmigration.graph.meta;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IBuildSchemaFilter;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.DBObjectFactory;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;

public class GraphSchemaFetcher extends AbstractJDBCSchemaFetcher {
	
	public GraphSchemaFetcher() {
		factory = new DBObjectFactory();
	}

	//GDB schemafetcher buildVertexes
	private GraphDictionary buildVertexes() {
		return null;
	}
	
	//GDB schemafetcher
	private GraphDictionary buildEdges(){
		return null;
	}
	
	public DatabaseType getDBType() {
		return DatabaseType.GRAPH;
	}

	@Override
	protected DBExportHelper getExportHelper() {
		return DatabaseType.GRAPH.getExportHelper();
	}
	
	
	//GDB graphdb build catalog. does GraphDB need catalog?
	@Override
	public Catalog buildCatalog(final Connection conn, ConnParameters cp, IBuildSchemaFilter filter) throws SQLException {
		String dbName = cp.getDbName();
		String catalogName;

		DatabaseType databaseType = cp.getDatabaseType();
		if (DatabaseType.ORACLE == databaseType) {
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
//		for (String schema : schemas) {
//			buildSchema(conn, catalog, schema, filter);
//		}
		return catalog;
	}

}
