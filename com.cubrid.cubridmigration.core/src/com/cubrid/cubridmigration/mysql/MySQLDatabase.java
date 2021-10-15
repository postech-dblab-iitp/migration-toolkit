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
package com.cubrid.cubridmigration.mysql;

import java.sql.Connection;
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
import com.cubrid.cubridmigration.mysql.export.MySQLExportHelper;
import com.cubrid.cubridmigration.mysql.meta.MySQLSchemaFetcher;

/**
 * CUBRID Database Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-2-2 created by Kevin Cao
 */
public class MySQLDatabase extends
		DatabaseType {

	public MySQLDatabase() {
		super(DBConstant.DBTYPE_MYSQL,
				DBConstant.DB_NAMES[DBConstant.DBTYPE_MYSQL],
				new String[]{DBConstant.JDBC_CLASS_MYSQL },
				DBConstant.DEF_PORT_MYSQL, new MySQLSchemaFetcher(),
				new MySQLExportHelper(), new MysqlConnHelper(), false);
	}

	/**
	 * Retrieves the databases SQL helper
	 * 
	 * @param version Database version
	 * @return SQLHelper
	 */
	public SQLHelper getSQLHelper(String version) {
		return MySQLSQLHelper.getInstance(version);
	}

	/**
	 * 
	 * MysqlConnHelper
	 * 
	 */
	private static class MysqlConnHelper implements
			IConnHelper {
		/**
		 * return the jdbc url to connect the database
		 * 
		 * @param connParameters ConnParameters
		 * @return String
		 */
		public String makeUrl(ConnParameters connParameters) {
			String url;
			if (connParameters.getPort() == 0) {
				String mysqlJdbcURLPattern = "jdbc:mysql://%s/%s";
				url = String.format(mysqlJdbcURLPattern,
						connParameters.getHost(), connParameters.getDbName());
			} else {
				String mysqlJdbcURLPattern = "jdbc:mysql://%s:%s/%s";
				url = String.format(mysqlJdbcURLPattern,
						connParameters.getHost(), connParameters.getPort(),
						connParameters.getDbName());
			}

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
				//can't get connection throw DriverManger
				Properties props = new Properties();
				props.put("user", conParam.getConUser());
				props.put("password", conParam.getConPassword());
				props.put("characterencoding", conParam.getCharset());

				//Connection conn = driver.connect(makeUrl(conParam), props);
				Connection conn;
				if (StringUtils.isBlank(conParam.getUserJDBCURL())) {
					conn = driver.connect(makeUrl(conParam), props);
				} else {
					conn = driver.connect(conParam.getUserJDBCURL(), props);
				}
				//0000-00-00 will be turned into 0001-01-01
				//				Method method = conn.getClass().getMethod(
				//						"setZeroDateTimeBehavior", String.class);
				//				method.invoke(conn, "round");
				conn.setAutoCommit(false);
				return conn;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Get Database data type helper
	 * 
	 * @param version of database
	 * @return helper.
	 */
	public DBDataTypeHelper getDataTypeHelper(String version) {
		return MySQLDataTypeHelper.getInstance(version);
	};
}
