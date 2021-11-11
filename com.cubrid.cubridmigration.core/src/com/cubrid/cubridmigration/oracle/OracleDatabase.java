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
package com.cubrid.cubridmigration.oracle;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.connection.IConnHelper;
import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.dbtype.DBConstant;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.oracle.export.OracleExportHelper;
import com.cubrid.cubridmigration.oracle.meta.OracleSchemaFetcher;

/**
 * CUBRID Database Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-2-2 created by Kevin Cao
 */
public class OracleDatabase extends
		DatabaseType {

	public OracleDatabase() {
		super(DBConstant.DBTYPE_ORACLE,
				DBConstant.DB_NAMES[DBConstant.DBTYPE_ORACLE],
				new String[]{DBConstant.JDBC_CLASS_ORACLE },
				DBConstant.DEF_PORT_ORACLE, new OracleSchemaFetcher(),
				new OracleExportHelper(), new OracleConnHelper(), false);
	}

	/**
	 * 
	 * OracleConnHelper
	 * 
	 */
	private static class OracleConnHelper implements
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
			String oracleJdbcURLPattern = "jdbc:oracle:thin:@%s:%s:%s";
			//Oracle cluster connecting mode
			if (dbName.startsWith("/")) {
				oracleJdbcURLPattern = "jdbc:oracle:thin:@%s:%s/%s";
				dbName = dbName.substring(1, dbName.length());
			}
			//If the DB name contains schema name, for example: XE/migrationdev
			//Not support XE/migrationdev any more
			//			if (dbName.indexOf('/') > 0) {
			//				dbName = dbName.split("/")[0];
			//			}
			String url = String.format(oracleJdbcURLPattern,
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
				//				String tzName = connParameters.getTimeZone();
				//				String timeZone = connParameters.getTimeZone();
				//				// "Etc/GMT+0"
				//				if (tzName == null) {
				//					timeZone = TimeZoneUtil.getOracleTZID(TimeZoneUtil.getGMTFormat(TimeZone.getDefault().getID()));
				//				} else {
				//					String tzID = TimeZoneUtil.getGMTByDisplay(tzName);
				//					if (tzID == null) {
				//						timeZone = TimeZoneUtil.getOracleTZID(TimeZoneUtil.getGMTFormat(TimeZone.getDefault().getID()));
				//					} else {
				//						timeZone = TimeZoneUtil.getOracleTZID(tzID);
				//					}
				//				}
				Method method = Class.forName("oracle.jdbc.OracleConnection",
						false, driver.getClass().getClassLoader()).getMethod(
						"setSessionTimeZone", new Class[]{String.class });
				String timeZone = TimeZoneUtils.getOracleTZID(TimeZoneUtils.getGMTFormat(TimeZone.getDefault().getID()));
				method.invoke(conn, timeZone);
				//				((OracleConnection) conn).setSessionTimeZone(timeZone == null ? "Etc/GMT+0"
				//						: TimeUtil.getOracleTZID(timeZone));
				return conn;
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			} catch (Exception e) {
				throw new RuntimeException(e);
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
		return OracleSQLHelper.getInstance(version);
	}

	/**
	 * Retrieves the databases data type helper
	 * 
	 * @param version Database version
	 * @return DBDataTypeHelper
	 */
	public DBDataTypeHelper getDataTypeHelper(String version) {
		return OracleDataTypeHelper.getInstance(version);
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
