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
package com.cubrid.cubridmigration.tibero.export;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.core.export.IExportDataHandler;
import com.cubrid.cubridmigration.core.export.handler.CharTypeHandler;
import com.cubrid.cubridmigration.core.export.handler.TimestampTypeHandler;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.tibero.TiberoDataTypeHelper;
import com.cubrid.cubridmigration.tibero.export.handler.TiberoBFileTypeHandler;
import com.cubrid.cubridmigration.tibero.export.handler.TiberoIntervalDSTypeHandler;
import com.cubrid.cubridmigration.tibero.export.handler.TiberoIntervalYMTypeHandler;

public class TiberoExportHelper extends
		DBExportHelper {
	//private static final Logger LOG = LogUtil.getLogger(TiberoExportHelper.class);

	//private static final String ORACAL_ROW_NUMBER = "TiberoRowNumber";

	/**
	 * constructor
	 */
	public TiberoExportHelper() {
		super();
		handlerMap1.put(Types.DATE, new TimestampTypeHandler());
		handlerMap2.put("INTERVALDS", new TiberoIntervalDSTypeHandler());
		handlerMap2.put("INTERVALYM", new TiberoIntervalYMTypeHandler());
		handlerMap2.put("TIMESTAMP WITH LOCAL TIME ZONE", new TimestampTypeHandler());
		handlerMap2.put("TIMESTAMP WITH TIME ZONE", new CharTypeHandler());
		handlerMap2.put("ROWID", new CharTypeHandler());
		handlerMap2.put("UROWID", new CharTypeHandler());
		handlerMap2.put("BFILE", new TiberoBFileTypeHandler());
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
		String oraType = TiberoDataTypeHelper.getTiberoDataTypeKey(column.getDataType());
		IExportDataHandler edh = handlerMap2.get(oraType);
		if (edh != null) {
			return edh.getJdbcObject(rs, column);
		}
		return super.getJdbcObject(rs, column);
	}

	/**
	 * return database object name
	 * 
	 * @param objectName String
	 * @return String
	 */
	protected String getQuotedObjName(String objectName) {
		return DatabaseType.TIBERO.getSQLHelper(null).getQuotedObjName(objectName);
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
	//		buf.append(" WHERE ROWNUM BETWEEN ").append(exportedRecords + 1L);
	//		buf.append(" AND ").append(rows + exportedRecords);
	//
	//		return buf.toString();
	//	}
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
		//		StringBuilder buf = new StringBuilder(sql);
		//		String cleanSql = sql.toUpperCase().trim();
		//		if (cleanSql.indexOf("WHERE") != -1) {
		//			buf.append(" AND ROWNUM BETWEEN ").append(exportedRecords + 1L).append(" AND ").append(
		//					rows + exportedRecords);
		//		} else {
		//			buf.append(" WHERE ROWNUM BETWEEN ").append(exportedRecords + 1L).append(" AND ").append(
		//					rows + exportedRecords);
		//		}
		//
		//		return buf.toString();
		//TODO: Tibero page selection SQL
		return sql;
	}
	
	@Override
	public String getPagedSelectSQL(Edge e, String sql, long rows, long exportedRecords, PK pk) {
		String cleanSql = sql.toUpperCase().trim();
		
		String editedQuery = editQueryForJoinTableEdge(e, cleanSql, rows, exportedRecords);
		
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
//			buf.append(" FOR ORDERBY_NUM() ");
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
//				buf.append(" FOR ORDERBY_NUM() ");
			} else {
				if (cleanSql.indexOf("WHERE") < 0) {
					buf.append(" WHERE");
				} else {
					buf.append(" AND");
				}
				buf.append(" ROWNUM ");
			}
		}



		return buf.toString();
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.TIBERO;
	}

	private static final String SERIAL_CURRENT_VALUE_SQL = "SELECT S.LAST_NUMBER,S.SEQUENCE_OWNER FROM ALL_SEQUENCES S "
			+ "WHERE S.SEQUENCE_NAME=? ORDER BY S.SEQUENCE_OWNER";

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
			while (rs.next()) {
				if (sq.getOwner() == null) {
					return new BigInteger(rs.getString(1));
				}
				if (rs.getString(2).equalsIgnoreCase(sq.getOwner())) {
					return new BigInteger(rs.getString(1));
				}
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
//			buf.append(" FOR ORDERBY_NUM() ");
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
//				buf.append(" FOR ORDERBY_NUM() ");
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
	
	public String getPagedFkRecords(Edge e, String sql, long rows, long exportedRecords) {
		String cleanSql = sql.toUpperCase().trim();
		
		String editedQuery = editQueryForFk(e, cleanSql, rows, exportedRecords);
		
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
//			buf.append(" FOR ORDERBY_NUM() ");
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


//		buf.append(" order by " + e.getStartVertexName() + "." + fkCol);

		return buf.toString();
	}
	
	public String editQueryForFk(Edge e, String sql, long rows, long exportedRecords) {
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
		buffer.append(startVertexName + ".\":START_ID(" + e.getStartVertexName() + ")\"");
		buffer.append(", ");
		buffer.append(endVertexName + ".\":END_ID(" + e.getEndVertexName() + ")\"");
		
		buffer.append(" FROM ");
		
		buffer.append("(SELECT ROWNUM as \":START_ID(" + e.getStartVertexName() + ")\", ");
		buffer.append(fkCol);
		buffer.append(" FROM ");
		buffer.append(e.getStartVertexName());
		buffer.append(" order by ");
		buffer.append(fkCol);
		buffer.append(") as ");
		buffer.append(startVertexName);
		buffer.append(", ");
		
		buffer.append("(SELECT ROWNUM as \":END_ID(" + e.getEndVertexName() + ")\", ");
		buffer.append(refCol);
		buffer.append(" FROM ");
		buffer.append(e.getEndVertexName());
		buffer.append(" ORDER BY ");
		buffer.append(refCol);
		buffer.append(") AS ");
		buffer.append(endVertexName);
		
		buffer.append(" WHERE ");
		buffer.append(startVertexName + "." + fkCol);
		buffer.append(" = ");
		buffer.append(endVertexName + "." + refCol);
		buffer.append(" AND ROWNUM ");
		buffer.append(" BETWEEN ").append(exportedRecords + 1L);
		buffer.append(" AND ").append(exportedRecords + rows);
		
		buffer.append(" ORDER BY " + startVertexName + "." + fkCol);
		
		return buffer.toString();
	}
	
	public String getFkRecordCounterSql(Edge e, Map<String, String> fkMapping) {
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
		buffer.append("SELECT COUNT(1) FROM ");
		
		buffer.append("(SELECT ROWNUM as \"START_ID\", ");
		buffer.append(fkCol);
		buffer.append(" FROM ");
		buffer.append(e.getStartVertexName());
		buffer.append(" order by ");
		buffer.append(fkCol);
		buffer.append(")");
		buffer.append(startVertexName);
		buffer.append(", ");
		
		buffer.append("(SELECT ROWNUM as \"END_ID\", ");
		buffer.append(refCol);
		buffer.append(" FROM ");
		buffer.append(e.getEndVertexName());
		buffer.append(" order by ");
		buffer.append(refCol);
		buffer.append(")");
		buffer.append(endVertexName);
		
		buffer.append(" where ");
		buffer.append(startVertexName + "." + fkCol);
		buffer.append(" = ");
		buffer.append(endVertexName + "." + refCol);
		
		return buffer.toString();
	}

	@Override
	public String getGraphSelectSQL(Edge e, boolean targetIsCSV) {
		// TODO Auto-generated method stub
		return null;
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
	
	private String editQueryForJoinTableEdge(Edge e, String sql, long rows, long exportedRecords) {
		String editString = new String(sql);
		List<String> refColList = e.getFKColumnNames();
		
		String startVertexName = addDoubleQuote(e.getStartVertexName().toUpperCase());
		String endVertexName = addDoubleQuote(e.getEndVertexName().toUpperCase());
		
		String startIdCol = "\":START_ID(" + e.getStartVertexName().toUpperCase() + ")\"";
		String endIdCol = "\":END_ID(" + e.getEndVertexName().toUpperCase() + ")\"";
		
		String dupStartVertexName = null;
		String dupEndVertexName = null;
		
		if (e.getStartVertexName().equalsIgnoreCase(e.getEndVertexName())) {
			dupStartVertexName = addDoubleQuote(e.getStartVertexName() + "_1");
			dupEndVertexName = addDoubleQuote(e.getEndVertexName() + "_2");
		} else {
			dupStartVertexName = startVertexName;
			dupEndVertexName = endVertexName;
		}
		
//		String edgeLabel = addDoubleQuote(e.getEdgeLabel().toUpperCase());
		String edgeLabel = e.getEdgeLabel().toUpperCase();
		
		Pattern startVertexIdPattern = Pattern.compile(addDoubleQuote(refColList.get(0)), Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher startVertexIdMatcher = startVertexIdPattern.matcher(editString);
		
		if (startVertexIdMatcher.find()) {
			String column = "\"" + edgeLabel + "\"." + addDoubleQuote(refColList.get(0));
			
			editString = startVertexIdMatcher.replaceFirst(column);
		}
		
		Pattern startIdPattern = Pattern.compile("\":START_ID", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher startIdMatcher = startIdPattern.matcher(editString);
		
		if (startIdMatcher.find()) {
			String startId = dupStartVertexName + ".\":START_ID";
			
			editString = startIdMatcher.replaceFirst(startId);
		}
		
		Pattern endIdPattern = Pattern.compile("\":END_ID", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);;
		Matcher endIdMatcher = endIdPattern.matcher(editString);
		
		if (endIdMatcher.find()) {
			String endId = dupEndVertexName + ".\":END_ID";
			
			editString = endIdMatcher.replaceFirst(endId);
		}
		
		Pattern endVertexIdPattern = Pattern.compile(addDoubleQuote(refColList.get(1)), Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher endVertexIdMatcher = endVertexIdPattern.matcher(editString);
		
		if (endVertexIdMatcher.find()) {
			
			String column = "\"" + edgeLabel + "\"." + addDoubleQuote(refColList.get(1));
			
			editString = endVertexIdMatcher.replaceFirst(column);
		}
		
		Pattern fromPattern = Pattern.compile("FROM\\s.*$", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher fromMatcher = fromPattern.matcher(editString);
		
		if (fromMatcher.find()) {
			StringBuffer buffer = new StringBuffer();
			
			startVertexIdMatcher.reset();
			endVertexIdMatcher.reset();
			
//			if (startVertexIdMatcher.find() || endVertexIdMatcher.find()) {
//				buffer.append("FROM " + edgeLabel + " as main, " + "(SELECT ROWNUM as " + startIdCol + ", ");
//				
//				edgeLabel = new String("\"main\"");
//				
//			} else {
//				buffer.append("FROM " + edgeLabel + ", " + "(SELECT ROWNUM as " + startIdCol + ", ");
//			}
			
			buffer.append("FROM " + edgeLabel + ", " + "(SELECT ROWNUM as " + startIdCol + ", ");
			
			buffer.append(addDoubleQuote(e.getREFColumnNames(refColList.get(0))));
			
			buffer.append(" from " + startVertexName + " order by ");
			
			buffer.append(addDoubleQuote(e.getREFColumnNames(refColList.get(0))));
			
			buffer.append(") as " + dupStartVertexName + ", (SELECT ROWNUM as " + endIdCol + ", ");
			
			buffer.append(addDoubleQuote(e.getREFColumnNames(refColList.get(1))));
			
			buffer.append(" from " + endVertexName + " order by ");
			
			buffer.append(addDoubleQuote(e.getREFColumnNames(refColList.get(1))));
			
			buffer.append(") as " + dupEndVertexName);
			
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
		
		whereBuffer.append(" and ROWNUM ");
		
		whereBuffer.append(" BETWEEN ").append(exportedRecords + 1L);
		
		whereBuffer.append(" AND ").append(exportedRecords + rows);
		
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
	
}
