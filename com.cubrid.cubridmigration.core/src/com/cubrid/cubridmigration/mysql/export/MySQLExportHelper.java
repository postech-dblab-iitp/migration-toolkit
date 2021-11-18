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
package com.cubrid.cubridmigration.mysql.export;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.core.export.IExportDataHandler;
import com.cubrid.cubridmigration.core.export.handler.DateTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.NumberTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.TimeTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.TimestampTypeHandler;
import com.cubrid.cubridmigration.mysql.export.handler.MySQLYearTypeHandler;

/**
 * a class help to export MySQL data and verify MySQL sql statement
 * 
 * @author moulinwang
 * 
 */
public class MySQLExportHelper extends
		DBExportHelper {

	private static final Logger LOG = LogUtil.getLogger(MySQLExportHelper.class);

	/**
	 * constructor
	 */
	public MySQLExportHelper() {
		super();
		handlerMap2.put("YEAR", new MySQLYearTypeHandler());
		handlerMap2.put("BIGINT UNSIGNED", new NumberTypeHandler());
		handlerMap2.put("DATE", new DateTypeHandler());
		handlerMap2.put("TIME", new TimeTypeHandler());
		handlerMap2.put("TIMESTAMP", new TimestampTypeHandler());
		handlerMap2.put("DATETIME", new TimestampTypeHandler());
	}

	/**
	 * get JDBC Object
	 * 
	 * @param rs ResultSet
	 * @param column Column
	 * 
	 * @return Object
	 * @throws SQLException e
	 */
	public Object getJdbcObject(final ResultSet rs, final Column column) throws SQLException {
		IExportDataHandler edh = handlerMap2.get(column.getDataType());
		if (edh != null) {
			return edh.getJdbcObject(rs, column);
		}
		return super.getJdbcObject(rs, column);
	}

	/**
	 * return whether input string match Mysql limit syntex [LIMIT [offset,]
	 * rows | rows OFFSET offset]
	 * 
	 * @param limitPart String
	 * @return boolean
	 */
	public boolean matchMySQLLimit(String limitPart) {
		String regex1 = "\\s*LIMIT\\s*(\\d+\\s*,)?\\s*\\d+\\s*+(\\D*)";
		Pattern pattern1 = Pattern.compile(regex1);
		Matcher matcher1 = pattern1.matcher(limitPart);

		String regex2 = "\\s*LIMIT\\s*\\d+\\s*OFFSET\\s*\\d+\\s*(\\D*)";
		Pattern pattern2 = Pattern.compile(regex2);
		Matcher matcher2 = pattern2.matcher(limitPart);

		return matcher1.matches() || matcher2.matches();
	}

	/**
	 * replace limit part with "limit 0"
	 * 
	 * @param limitPart String
	 * @return String
	 */
	public String replaceWithMySQLLimit0(String limitPart) {
		String regex1 = "\\s*LIMIT\\s*(\\d+\\s*,)?\\s*\\d+\\s*+(\\D*)";
		Pattern pattern1 = Pattern.compile(regex1);
		Matcher matcher1 = pattern1.matcher(limitPart);

		if (matcher1.matches()) {
			int start = matcher1.start(2);
			int end = matcher1.end(2);
			return " LIMIT 0 " + limitPart.substring(start, end);
		}

		String regex2 = "\\s*LIMIT\\s*\\d+\\s*OFFSET\\s*\\d+\\s*(\\D*)";
		Pattern pattern2 = Pattern.compile(regex2);
		Matcher matcher2 = pattern2.matcher(limitPart);

		if (matcher2.matches()) {
			int start = matcher2.start(1);
			int end = matcher2.end(1);
			return " LIMIT 0 " + limitPart.substring(start, end);
		}

		return limitPart;
	}

	/**
	 * return database object name
	 * 
	 * @param objectName String
	 * @return String
	 */
	protected String getQuotedObjName(String objectName) {
		return DatabaseType.MYSQL.getSQLHelper(null).getQuotedObjName(objectName);
	}

	/**
	 * config Statement Object
	 * 
	 * @param stmt Statement
	 */
	public void configStatement(Statement stmt) {
		try {
			super.configStatement(stmt);
			//Call the method "enableStreamingResults" of mysql statement to make fetch size work
			Method method = stmt.getClass().getMethod("enableStreamingResults");
			if (method != null) {
				method.invoke(stmt);
			}
		} catch (Exception e) {
			LOG.error("", e);
		}
	}

	//	/**
	//	 * to return Paged SELECT SQL
	//	 * 
	//	 * @param sourceTable SourceTable
	//	 * @param columnList List<Column>
	//	 * @param rows
	//	 * @param exportedRecords
	//	 * @return String
	//	 */
	//	public String getPagedSelectSQL(final Table sourceTable,
	//			final List<SourceColumnConfig> columnList, int rows,
	//			long exportedRecords) {
	//
	//		StringBuffer buf = new StringBuffer(256);
	//		buf.append("SELECT ");
	//
	//		for (int i = 0; i < columnList.size(); i++) {
	//			if (i > 0) {
	//				buf.append(',');
	//			}
	//
	//			buf.append(getQuoteStr(columnList.get(i).getName()));
	//		}
	//
	//		buf.append(" FROM ").append(getQuoteStr(sourceTable.getName()));
	//		buf.append(" LIMIT ").append(exportedRecords);
	//		buf.append(" , ").append(rows);
	//
	//		return buf.toString();
	//	}

	/**
	 * Retrieves the SQL with page condition
	 * 
	 * @param sql to be change
	 * @param rows record count per-page
	 * @param exportedRecords record count to be skipped.
	 * @param pk table's primary key
	 * @return SQL
	 */
	public String getPagedSelectSQL(String sql, long rows, long exportedRecords, PK pk) {
		return sql;
		//TODO: MySQL page selecting SQL
		//		StringBuilder buf = new StringBuilder(sql);
		//		buf.append(" LIMIT ").append(
		//				(exportedRecords <= 0 ? 0 : (exportedRecords - 1))).append(", ").append(
		//				rows);
		//
		//		return buf.toString();
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.MYSQL;
	}
}
