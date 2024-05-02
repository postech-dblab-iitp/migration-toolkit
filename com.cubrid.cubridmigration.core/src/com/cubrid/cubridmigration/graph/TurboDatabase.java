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

public class TurboDatabase extends DatabaseType {

	public TurboDatabase() {
		super(DBConstant.DBTYPE_GRAPH,
				DBConstant.DB_NAMES[4],
				new String[] { DBConstant.JDBC_CLASS_NEO4J },
				DBConstant.DEF_PORT_GRAPH, new GraphSchemaFetcher(),
				new GraphExportHelper(), new GraphConnHelper(), false);
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
			String neo4jUrlPattern = "jdbc:neo4j:bolt://%s:%s/?database=%s";
			String url = String.format(neo4jUrlPattern,
					connParameters.getHost(), connParameters.getPort(), connParameters.getDbName());
			return url;
		}

		public Connection createConnection(ConnParameters conParam)
				throws SQLException {
			//GDB GraphDatabase create connection
			try {
				Driver driver = conParam.getDriver();
				if (driver == null) {
					throw new RuntimeException("JDBC driver can't be null.");
				}
				Properties props = new Properties();
				props.put("username", conParam.getConUser());
				props.put("password", conParam.getConPassword());
				
				Connection conn;
				if (StringUtils.isBlank(conParam.getUserJDBCURL())) {
					conn = driver.connect(makeUrl(conParam), props);
				} else {
					conn = driver.connect(conParam.getUserJDBCURL(), props);
				}
				
				if (conn == null) {
					throw new SQLException("Can't connect database server");
				}
				
                checkDatabase(conn);
			    
				return conn;
			} catch (SQLException e) {
				throw e;
			} catch (Exception e) { 
				throw new RuntimeException(e);
			} 
		}
		
		private void checkDatabase(Connection conn) 
		        throws SQLException {
            try {
                DatabaseMetaData metaData = conn.getMetaData();
                if (metaData != null) {
                    String projectVersion= metaData.getDatabaseProductVersion();
                    if (projectVersion.equals("Unknown")) {
                        throw new SQLException("Unable to read database information., Please check name or all setting of database");
                    }
                }
            } catch (SQLException e) {
                throw e;
            }
		}
	}
}
