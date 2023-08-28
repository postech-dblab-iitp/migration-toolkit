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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.dbobject.Column;
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
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

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
	
	public String getPagedSelectSQL(Vertex v, String sql, long rows, long exportedRecords, PK pk) {
		String cleanSql = sql.toUpperCase().trim();
		
		String editedQuery = editQueryForVertexCSV(v, sql);
		
		StringBuilder buf = new StringBuilder(editedQuery.trim());
		
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
	
	private String editQueryForVertexCSV(Vertex v, String sql) {
		Pattern selectPattern = Pattern.compile("SELECT\\s", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher selectMatcher = selectPattern.matcher(sql);
		
		if (selectMatcher.find()) {
			StringBuffer buffer = new StringBuffer("SELECT ROWNUM as ");
			
			sql = selectMatcher.replaceFirst(buffer.toString());
		}
		
		return sql;
	}
	
	public String getPagedSelectSQL(Edge e, String sql, long rows, long exportedRecords, PK pk) {
		String cleanSql = sql.toUpperCase().trim();
		
		String editedQuery = editQueryForJoinTableEdge(e, cleanSql);
		
		StringBuilder buf = new StringBuilder(editedQuery.trim());

		Pattern pattern = Pattern.compile("GROUP\\s+BY", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(editedQuery); 
		
		Pattern pattern2 = Pattern.compile("ORDER\\s+BY", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher matcher2 = pattern2.matcher(editedQuery);
		
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
	
	public String getPagedFkRecords(Edge e, String sql, long rows, long exportedRecords) {
		String cleanSql = sql.toUpperCase().trim();
		
		String editedQuery = editQueryForFk(e, cleanSql);
		
		StringBuilder buf = new StringBuilder(editedQuery.trim());

		Pattern pattern = Pattern.compile("GROUP\\s+BY", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(editedQuery); 
		
		Pattern pattern2 = Pattern.compile("ORDER\\s+BY", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher matcher2 = pattern2.matcher(editedQuery);
		
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
//			if (pk != null) {
//				// if it has a pk, a pk scan is better than full range scan
//				for (String pkCol : pk.getPkColumns()) {
//					if (orderby.length() > 0) {
//						orderby.append(", ");
//					}
//					orderby.append("\"").append(pkCol).append("\"");
//				}
//			}
//			if (orderby.length() > 0) {
//				buf.append(" ORDER BY ");
//				buf.append(orderby);
//				buf.append(" FOR ORDERBY_NUM() ");
//			} else {
//				if (cleanSql.indexOf("WHERE") < 0) {
//					buf.append(" WHERE");
//				} else {
//					buf.append(" AND");
//				}
//				buf.append(" ROWNUM ");
//			}
		}

		buf.append(" BETWEEN ").append(exportedRecords + 1L);
		buf.append(" AND ").append(exportedRecords + rows);

		return buf.toString();
	}
	
	private String editQueryForFk(Edge e, String sql) {
		Map<String, String> fkMapping = e.getfkCol2RefMapping();
		List<String> keySet = e.getFKColumnNames();
		
		String startVertexName;
		String endVertexName;
		
		if (e.getStartVertexName().equals(e.getEndVertexName())) {
			startVertexName = e.getStartVertexName() + "_1";
			endVertexName = e.getEndVertexName() + "_2";
		} else {
			startVertexName = e.getStartVertexName();
			endVertexName = e.getEndVertexName();
		}
		
		String fkCol = keySet.get(0);
		String refCol = fkMapping.get(keySet.get(0));
		
		StringBuffer buffer = new StringBuffer();
		buffer.append("SELECT ");
		buffer.append(startVertexName + ".\"START_ID(" + e.getStartVertexName() + ")\"");
		buffer.append(", ");
		buffer.append(endVertexName + ".\"END_ID(" + e.getEndVertexName() + ")\"");
		
		buffer.append(" FROM ");
		
		buffer.append("(SELECT ROWNUM as \"START_ID(" + e.getStartVertexName() + ")\", ");
		buffer.append(fkCol);
		buffer.append(" FROM ");
		buffer.append(e.getStartVertexName());
		buffer.append(" order by ");
		buffer.append(fkCol);
		buffer.append(" for orderby_num()) as ");
		buffer.append(startVertexName);
		buffer.append(", ");
		
		buffer.append("(SELECT ROWNUM as \"END_ID(" + e.getEndVertexName() + ")\", ");
		buffer.append(refCol);
		buffer.append(" FROM ");
		buffer.append(e.getEndVertexName());
		buffer.append(" order by ");
		buffer.append(refCol);
		buffer.append(" for orderby_num()) as ");
		buffer.append(endVertexName);
		
		buffer.append(" where ");
		buffer.append(startVertexName + "." + fkCol);
		buffer.append(" = ");
		buffer.append(endVertexName + "." + refCol);
		
		buffer.append(" order by " + startVertexName + "." + fkCol);
		
		return buffer.toString();
	}
	
	private String editQueryForJoinTableEdge(Edge e, String sql) {
		String editString = new String(sql);
		
		List<String> refColList = e.getFKColumnNames();
		
		String startVertexName = addDoubleQuote(e.getStartVertexName().toUpperCase());
		String endVertexName = addDoubleQuote(e.getEndVertexName().toUpperCase());
		
		String startIdCol = "\"START_ID(" + e.getStartVertexName().toUpperCase() + ")\"";
		String endIdCol = "\"END_ID(" + e.getEndVertexName().toUpperCase() + ")\"";
		
		String dupStartVertexName = null;
		String dupEndVertexName = null;
		
		if (e.getStartVertexName().equalsIgnoreCase(e.getEndVertexName())) {
			dupStartVertexName = addDoubleQuote(e.getStartVertexName() + "_1");
			dupEndVertexName = addDoubleQuote(e.getEndVertexName() + "_2");
		} else {
			dupStartVertexName = startVertexName;
			dupEndVertexName = endVertexName;
		}
		
		String edgeLabel = addDoubleQuote(e.getEdgeLabel().toUpperCase());
		
		Pattern startVertexIdPattern = Pattern.compile(addDoubleQuote(refColList.get(0)), Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher startVertexIdMatcher = startVertexIdPattern.matcher(editString);
		
		if (startVertexIdMatcher.find()) {
			String column = "\"main\"." + addDoubleQuote(refColList.get(0));
			
			editString = startVertexIdMatcher.replaceFirst(column);
		}
		
		Pattern startIdPattern = Pattern.compile("\"START_ID", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher startIdMatcher = startIdPattern.matcher(editString);
		
		if (startIdMatcher.find()) {
			String startId = dupStartVertexName + ".\"START_ID";
			
			editString = startIdMatcher.replaceFirst(startId);
		}
		
		Pattern endIdPattern = Pattern.compile("\"END_ID", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);;
		Matcher endIdMatcher = endIdPattern.matcher(editString);
		
		if (endIdMatcher.find()) {
			String endId = dupEndVertexName + ".\"END_ID";
			
			editString = endIdMatcher.replaceFirst(endId);
		}
		
		Pattern endVertexIdPattern = Pattern.compile(addDoubleQuote(refColList.get(1)), Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher endVertexIdMatcher = endVertexIdPattern.matcher(editString);
		
		if (endVertexIdMatcher.find()) {
			
			String column = "\"main\"." + addDoubleQuote(refColList.get(1));
			
			editString = endVertexIdMatcher.replaceFirst(column);
		}
		
		Pattern fromPattern = Pattern.compile("FROM\\s.*$", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher fromMatcher = fromPattern.matcher(editString);
		
		if (fromMatcher.find()) {
			StringBuffer buffer = new StringBuffer();
			
			startVertexIdMatcher.reset();
			endVertexIdMatcher.reset();
			
			if (startVertexIdMatcher.find() || endVertexIdMatcher.find()) {
				buffer.append("FROM " + edgeLabel + " as main, " + "(SELECT ROWNUM as " + startIdCol + ", ");
				
				edgeLabel = new String("\"main\"");
				
			} else {
				buffer.append("FROM " + edgeLabel + ", " + "(SELECT ROWNUM as " + startIdCol + ", ");
			}
			
			buffer.append(addDoubleQuote(e.getREFColumnNames(refColList.get(0))));
			
			buffer.append(" from " + startVertexName + " order by ");
			
			buffer.append(addDoubleQuote(e.getREFColumnNames(refColList.get(0))));
			
			buffer.append(" for orderby_num()) as " + dupStartVertexName + ", (SELECT ROWNUM as " + endIdCol + ", ");
			
			buffer.append(addDoubleQuote(e.getREFColumnNames(refColList.get(1))));
			
			buffer.append(" from " + endVertexName + " order by ");
			
			buffer.append(addDoubleQuote(e.getREFColumnNames(refColList.get(1))));
			
			buffer.append(" for orderby_num()) as " + dupEndVertexName);
			
			editString = fromMatcher.replaceFirst(buffer.toString());
		}
		
		StringBuffer originalString = new StringBuffer(editString);
		StringBuffer whereBuffer = new StringBuffer();
		
		whereBuffer.append(" where " + dupStartVertexName + ".");
		
		whereBuffer.append(addDoubleQuote(e.getfkCol2RefMapping().get(refColList.get(0))));
		
		whereBuffer.append(" = " + edgeLabel + ".");
		
		whereBuffer.append(addDoubleQuote(refColList.get(0)));
		
		whereBuffer.append(" and " + dupEndVertexName + ".");
		
		whereBuffer.append(addDoubleQuote(e.getfkCol2RefMapping().get(refColList.get(1))));
		
		whereBuffer.append(" = " + edgeLabel + ".");
		
		whereBuffer.append(addDoubleQuote(refColList.get(1)));
		
		whereBuffer.append(" order by " + edgeLabel + ".");
		
		whereBuffer.append(addDoubleQuote(refColList.get(0)));
		
		whereBuffer.append(", " + edgeLabel + ".");
		
		whereBuffer.append(addDoubleQuote(refColList.get(1)));
		
		originalString.append(whereBuffer.toString());
		
		editString = originalString.toString();
		
		return editString;
	}
	
	private String addDoubleQuote(String str) {
		return "\"" + str + "\"";
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
	
	public String getGraphPagedSelectSQL(String sql, long pageSize,
			long exportedRecords, PK pk) {
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
		buf.append(" AND ").append(exportedRecords + pageSize);

		return buf.toString();
	}
	
	public String getGraphSelectSQL(Vertex v, boolean isTargetCSV) {
		StringBuffer buf = new StringBuffer(256);
		buf.append("SELECT ");

		final List<Column> columnList = v.getColumnList();
		for (int i = 0; i < columnList.size(); i++) {
			if (i > 0) {
				buf.append(',');
			}
			buf.append(getQuotedObjName(columnList.get(i).getName()));
		}
		buf.append(" FROM ");
		// it will make a query with a schema and table name 
		// if it required a schema name when there create sql such as SCOTT.EMP
		addGraphSchemaPrefix(v, buf);
		buf.append(getQuotedObjName(v.getTableName()));

//		String condition = setc.getCondition();
//		if (StringUtils.isNotBlank(condition)) {
//			condition = condition.trim();
//			if (!condition.toLowerCase(Locale.US).startsWith("where")) {
//				buf.append(" WHERE ");
//			}
//			if (condition.trim().endsWith(";")) {
//				condition = condition.substring(0, condition.length() - 1);
//			}
//			buf.append(" ").append(condition);
//		}
		return buf.toString();
		
	}
	
	public String getGraphSelectSQL(Edge e) {
		StringBuffer buf = new StringBuffer(256);
		buf.append("SELECT ");
		final List<Column> columnList = e.getColumnList();
		for (int i = 0; i < columnList.size(); i++) {
			if (i > 0) {
				buf.append(',');
			}
			buf.append(getQuotedObjName(columnList.get(i).getName()));
		}
		buf.append(" FROM ");
		// it will make a query with a schema and table name 
		// if it required a schema name when there create sql such as SCOTT.EMP
		addGraphSchemaPrefix(e, buf);
		buf.append(getQuotedObjName(e.getEdgeLabel()));

//		String condition = setc.getCondition();
//		if (StringUtils.isNotBlank(condition)) {
//			condition = condition.trim();
//			if (!condition.toLowerCase(Locale.US).startsWith("where")) {
//				buf.append(" WHERE ");
//			}
//			if (condition.trim().endsWith(";")) {
//				condition = condition.substring(0, condition.length() - 1);
//			}
//			buf.append(" ").append(condition);
//		}
		return buf.toString();
		
	}
	
	public String getGraphSelectCountSQL(final Vertex v) {
		StringBuffer buf = new StringBuffer(256);
		buf.append("SELECT COUNT(1) ");
		buf.append(" FROM ");
		// it will make a query with a schema and table name 
		// if it required a schema name when there create sql such as SCOTT.EMP
		addGraphSchemaPrefix(v, buf);
		buf.append(getQuotedObjName(v.getVertexLabel()));

//		String condition = v.getCondition();
//		if (StringUtils.isNotBlank(condition)) {
//			if (!condition.trim().toLowerCase(Locale.US).startsWith("where")) {
//				buf.append(" WHERE ");
//			}
//			condition = condition.trim();
//			if (condition.trim().endsWith(";")) {
//				condition = condition.substring(0, condition.length() - 1);
//			}
//			buf.append(" ").append(condition);
//		}
		return buf.toString();
	}
	
	protected void addGraphSchemaPrefix(Vertex v, StringBuffer buf) {
		if (StringUtils.isNotBlank(v.getOwner())) {
			buf.append(getQuotedObjName(v.getOwner())).append(".");
		}
	}
	
	protected void addGraphSchemaPrefix(Edge e, StringBuffer buf) {
		if (StringUtils.isNotBlank(e.getOwner())) {
			buf.append(getQuotedObjName(e.getOwner())).append(".");
		}
	}
}
