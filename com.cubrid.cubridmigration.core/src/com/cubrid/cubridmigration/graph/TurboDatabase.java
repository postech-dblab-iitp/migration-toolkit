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
import com.cubrid.cubridmigration.graph.export.TurboExportHelper;
import com.cubrid.cubridmigration.graph.meta.TurboSchemaFetcher;

public class TurboDatabase extends DatabaseType {

	public TurboDatabase() {
		super(DBConstant.DBTYPE_TURBO,
				DBConstant.DB_NAMES[6],	new String[] { DBConstant.JDBC_CLASS_TURBO }, DBConstant.DEF_PORT_TURBO, 
				new TurboSchemaFetcher(), new TurboExportHelper(), new TurboConnHelper(), false);
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

	private static class TurboConnHelper implements IConnHelper {

		public String makeUrl(ConnParameters connParameters) {
			//GDB GraphDatabase make URL
			String neo4jUrlPattern = "jdbc:turbograph:%s:%s:%s:::";
			String url = String.format(neo4jUrlPattern,
					connParameters.getHost(), connParameters.getPort(), connParameters.getDbName());
			return url;
		}

		public Connection createConnection(ConnParameters conParam)
				throws SQLException {
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
		}
	}
}
