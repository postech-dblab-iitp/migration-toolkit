package com.cubrid.cubridmigration.graph;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.connection.IConnHelper;
import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.dbtype.DBConstant;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.graph.export.GraphExportHelper;
import com.cubrid.cubridmigration.graph.meta.GraphSchemaFetcher;

public class GraphDatabase extends DatabaseType {

	public GraphDatabase() {
		super(DBConstant.DBTYPE_TURBO,
				DBConstant.DB_NAMES[5],
				new String[] { DBConstant.JDBC_CLASS_NEO4J },
				null, null,	null, null, false);
		//GDB GraphDatabase constructor
	}

	@Override
	public SQLHelper getSQLHelper(String version) {
		//GDB GraphDatabase getSQLHelper
		return GraphSQLHelper.getInstance(version);
	}

	@Override
	public DBDataTypeHelper getDataTypeHelper(String version) {
		//GDB GraphDatabase data type helper
		return null;
	}

	private static class GraphConnHelper implements IConnHelper {

		public String makeUrl(ConnParameters connParameters) {
			//GDB GraphDatabase make URL
			return null;
		}

		public Connection createConnection(ConnParameters conParam)
				throws SQLException {
			return null; 
		}
		
		private void checkDatabase(Connection conn) 
		        throws SQLException {
		}
	}
}
