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
package com.cubrid.cubridmigration.tibero;

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
import com.cubrid.cubridmigration.tibero.export.TiberoExportHelper;
import com.cubrid.cubridmigration.tibero.meta.TiberoSchemaFetcher;

/**
 * CUBRID Database Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-2-2 created by Kevin Cao
 */
public class TiberoDatabase extends
		DatabaseType {

	public TiberoDatabase() {
		super(DBConstant.DBTYPE_TIBERO,
				DBConstant.DB_NAMES[DBConstant.DBTYPE_TIBERO],
				new String[]{DBConstant.JDBC_CLASS_TIBERO},
				DBConstant.DEF_PORT_TIBERO, new TiberoSchemaFetcher(),
				new TiberoExportHelper(), new TiberoConnHelper(), false);
	}

	/**
	 * 
	 * OracleConnHelper
	 * 
	 */
	private static class TiberoConnHelper implements
			IConnHelper {
		/**
		 * return the jdbc url to connect the database
		 * 
		 * @param connParameters ConnParameters
		 * @return String
		 */
		public String makeUrl(ConnParameters connParameters) {
			String dbName = connParameters.getDbName();
			if (dbName == null) {
				throw new IllegalArgumentException("DB name can't be NULL.");
			}
			String tiberoJdbcURLPattern = "jdbc:tibero:thin:@%s:%s:%s";
			//Oracle cluster connecting mode
			if (dbName.startsWith("/")) {
				tiberoJdbcURLPattern = "jdbc:tibero:thin:@%s:%s/%s";
				dbName = dbName.substring(1, dbName.length());
			}
			//If the DB name contains schema name, for example: XE/migrationdev
			//Not support XE/migrationdev any more
			//			if (dbName.indexOf('/') > 0) {
			//				dbName = dbName.split("/")[0];
			//			}
			String url = String.format(tiberoJdbcURLPattern,
					connParameters.getHost(), connParameters.getPort(), dbName);
			return url;
		}

		/**
		 * get a Connection
		 * 
		 * @param conParam ConnParameters
		 * @return Connection
		 * @throws SQLException e
		 */
		public Connection createConnection(ConnParameters conParam) throws SQLException {
			try {
				Driver driver = conParam.getDriver();
				if (driver == null) {
					throw new RuntimeException("JDBC driver can't be null.");
				}
				Properties props = new Properties();
				props.put("user", conParam.getConUser());
				props.put("password", conParam.getConPassword());
				props.put("characterencoding", conParam.getCharset());

				Connection conn;
				if (StringUtils.isBlank(conParam.getUserJDBCURL())) {
					conn = driver.connect(makeUrl(conParam), props);
				} else {
					conn = driver.connect(conParam.getUserJDBCURL(), props);
				}
				
				checkDatabase(conn);
				
				return conn;
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

	/**
	 * Retrieves the databases SQL helper
	 * 
	 * @param version Database version
	 * @return SQLHelper
	 */
	public SQLHelper getSQLHelper(String version) {
		return TiberoSQLHelper.getInstance(version);
	}

	/**
	 * Retrieves the databases data type helper
	 * 
	 * @param version Database version
	 * @return DBDataTypeHelper
	 */
	public DBDataTypeHelper getDataTypeHelper(String version) {
		return TiberoDataTypeHelper.getInstance(version);
	};

	/**
	 * The database type is supporting multi-schema.
	 * 
	 * @return true if supporting.
	 */
	public boolean isSupportMultiSchema() {
		return true;
	}
}
