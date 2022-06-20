package com.cubrid.cubridmigration.graph.meta;

import java.sql.Connection;
import java.sql.SQLException;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IBuildSchemaFilter;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;

public class GraphSchemaFetcher extends AbstractJDBCSchemaFetcher {

	//GDB schemafetcher buildNodes
	private GraphDictionary buildNodes() {
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
//		return super.buildCatalog(conn, cp, filter);
		return null;
	}

}
