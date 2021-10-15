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
package com.cubrid.cubridmigration.cubrid.export;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.core.export.handler.BytesTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.CharTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.ClobTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.DateTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.DefaultHandler;
import com.cubrid.cubridmigration.core.export.handler.NumberTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.TimeTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.TimestampTypeHandler;
import com.cubrid.cubridmigration.cubrid.export.handler.CUBRIDSetTypeHandler;

/**
 * a class help to export CUBRID data and verify CUBRID sql statement
 * 
 * @author Kevin Cao
 * @version 1.0 - 2010-9-15
 */
public class CUBRIDExportHelper extends
		DBExportHelper {
	//private static final Logger LOG = LogUtil.getLogger(CUBRIDExportHelper.class);

	public CUBRIDExportHelper() {
		super();
		handlerMap1.put(DataTypeConstant.CUBRID_DT_BIT, new BytesTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_VARBIT, new BytesTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_BLOB, new BytesTypeHandler());

		handlerMap1.put(DataTypeConstant.CUBRID_DT_CHAR, new CharTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_VARCHAR, new CharTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_NCHAR, new CharTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_NVARCHAR, new CharTypeHandler());

		handlerMap1.put(DataTypeConstant.CUBRID_DT_CLOB, new ClobTypeHandler());

		handlerMap1.put(DataTypeConstant.CUBRID_DT_SMALLINT, new DefaultHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_INTEGER, new DefaultHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_BIGINT, new DefaultHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_FLOAT, new DefaultHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_DOUBLE, new DefaultHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_MONETARY, new DefaultHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_NUMERIC, new NumberTypeHandler());

		handlerMap1.put(DataTypeConstant.CUBRID_DT_DATE, new DateTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_DATETIME, new TimestampTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_TIME, new TimeTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_TIMESTAMP, new TimestampTypeHandler());

		handlerMap1.put(DataTypeConstant.CUBRID_DT_SET, new CUBRIDSetTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_MULTISET, new CUBRIDSetTypeHandler());
		handlerMap1.put(DataTypeConstant.CUBRID_DT_SEQUENCE, new CUBRIDSetTypeHandler());
	}

	/**
	 * return db object name
	 * 
	 * @param objectName String
	 * @return String
	 */
	public String getQuotedObjName(String objectName) {
		return DatabaseType.CUBRID.getSQLHelper(null).getQuotedObjName(objectName);
	}

	/**
	 * Retrieves the sql with page condition
	 * 
	 * @param sql to be change
	 * @param rows per-page
	 * @param exportedRecords start position
	 * @param pk table's primary key
	 * @return SQL
	 */
	public String getPagedSelectSQL(String sql, long rows, long exportedRecords, PK pk) {
		StringBuilder buf = new StringBuilder(sql.trim());
		String cleanSql = sql.toUpperCase().trim();

		Pattern pattern = Pattern.compile("GROUP\\s+BY", Pattern.MULTILINE
				| Pattern.CASE_INSENSITIVE);
		Pattern pattern2 = Pattern.compile("ORDER\\s+BY", Pattern.MULTILINE
				| Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(cleanSql);
		Matcher matcher2 = pattern2.matcher(cleanSql);
		if (matcher.find()) {
			//End with group by 
			if (cleanSql.indexOf("HAVING") < 0) {
				buf.append(" HAVING ");
			} else {
				buf.append(" AND ");
			}
			buf.append(" GROUPBY_NUM() ");
		} else if (matcher2.find()) {
			//End with order by 
			buf.append(" FOR ORDERBY_NUM() ");
		} else {
			StringBuilder orderby = new StringBuilder();
			if (pk != null) {
				// if it has a pk, a pk scan is better than full range scan
				for (String pkCol : pk.getPkColumns()) {
					if (orderby.length() > 0) {
						orderby.append(", ");
					}
					orderby.append("\"").append(pkCol).append("\"");
				}
			}
			if (orderby.length() > 0) {
				buf.append(" ORDER BY ");
				buf.append(orderby);
				buf.append(" FOR ORDERBY_NUM() ");
			} else {
				if (cleanSql.indexOf("WHERE") < 0) {
					buf.append(" WHERE");
				} else {
					buf.append(" AND");
				}
				buf.append(" ROWNUM ");
			}
		}

		buf.append(" BETWEEN ").append(exportedRecords + 1L);
		buf.append(" AND ").append(exportedRecords + rows);

		return buf.toString();
	}

	/**
	 * Is support fast search with PK.
	 * 
	 * @param conn Connection
	 * @return true or false
	 */
	public boolean supportFastSearchWithPK(Connection conn) {
		try {
			String databaseProductName = conn.getMetaData().getDatabaseProductName();
			if ("CUBRID".equals(databaseProductName)) {
				String databaseVersion = conn.getMetaData().getDatabaseProductVersion();
				if (databaseVersion.startsWith("8.2.") || databaseVersion.startsWith("8.3.")) {
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.CUBRID;
	}

	/**
	 * If add a schema prefix before the table name.
	 * 
	 * @param setc SourceEntryTableConfig
	 * @param buf StringBuffer
	 */
	protected void addSchemaPrefix(SourceEntryTableConfig setc, StringBuffer buf) {
		//CUBRID will do nothing here
	}

	private static final String SERIAL_CURRENT_VALUE_SQL = "select current_val from db_serial where name=?";

	/**
	 * Retrieves the current value of input serial.
	 * 
	 * @param sourceConParams JDBC connection configuration
	 * @param sq sequence to be synchronized.
	 * @return The current value of the input SQ
	 */
	public BigInteger getSerialStartValue(ConnParameters sourceConParams, SourceSequenceConfig sq) {
		Connection conn = null;
		try {
			conn = sourceConParams.createConnection();
			PreparedStatement stmt = conn.prepareStatement(SERIAL_CURRENT_VALUE_SQL);
			stmt.setString(1, sq.getName());
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return new BigInteger(rs.getString(1));
			}
		} catch (SQLException e) {
			Closer.close(conn);
		}
		return null;
	}
}
