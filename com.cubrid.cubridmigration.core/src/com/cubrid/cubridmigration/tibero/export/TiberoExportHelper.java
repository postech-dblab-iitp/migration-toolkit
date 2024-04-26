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
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
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
	@Override
	public String getPagedSelectSQL(String sql, long rows, long exportedRecords, PK pk) {
		return sql;
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
	
	@Override
	public String getPagedFkRecords(Edge e, String sql, long rows, long exportedRecords, boolean hasMultiSchema) {
		
		StringBuffer buffer = new StringBuffer(sql);
		
//		buffer.append(" WHERE rnum BETWEEN " + (exportedRecords + 1L));
//		buffer.append(" AND " + (exportedRecords + rows));
		
		return buffer.toString();
	}
	
	@Override
	public String getPagedSelectSQLForEdgeCSV(Edge e, String sql, long rows, long exportedRecords, PK pk, boolean hasMultiSchema) {
		String cleanSql = sql.toUpperCase().trim();
		StringBuilder buf = new StringBuilder();

//		buf.append(" WHERE \"RNUM\"");
//
//		buf.append(" BETWEEN ").append(exportedRecords + 1L);
//		buf.append(" AND ").append(exportedRecords + rows);

		return cleanSql + buf.toString();
	}
	
	@Override
	public String getInnerQuery(Edge e, String sql, Connection conn, long exportedCount, long rows) {
		StringBuffer buffer = new StringBuffer();
		
		Map<String, String> fkMapping = e.getfkCol2RefMapping();
		List<String> keySet = e.getFKColumnNames();
		
		String innerTableName = getInnerTable(conn, e);
		
		String startVertexName;
		String endVertexName;
		
		if (e.getStartVertexName().equals(e.getEndVertexName())) {
			startVertexName = e.getStartVertexName() + "_1";
			endVertexName = e.getEndVertexName() + "_2";
		} else {
			startVertexName = e.getStartVertexName();
			endVertexName = e.getEndVertexName();
		}
		
		String startIdWithVertexName = "\"" + startVertexName + "\"" + ".\":START_ID(" + startVertexName + ")\"";
		String endIdWithVertexName = "\"" + endVertexName + "\"" + ".\":END_ID(" + endVertexName + ")\"";
		
		String startId = "\":START_ID(" + startVertexName + ")\"";
		String endId = "\":END_ID(" + endVertexName + ")\"";
		
		String fkCol = keySet.get(0);
		String refCol = fkMapping.get(keySet.get(0));
		
		StringBuilder sVertexOrderby = new StringBuilder();
		StringBuilder eVertexOrderby = new StringBuilder();
		
		if (e.getStartVertex() != null) {
			PK pk = e.getStartVertex().getPK();
			if (pk != null) {
				// if it has a pk, a pk scan is better than full range scan
				for (String pkCol : pk.getPkColumns()) {
					if (sVertexOrderby.length() > 0) {
						sVertexOrderby.append(", ");
					}
					sVertexOrderby.append(pkCol);
				}
			} else {
				Vertex startVertex = e.getStartVertex();
				
				for (Column col : startVertex.getColumnList()) {
					if (col.getDataType().equalsIgnoreCase("ID"))
						continue;
					
					if (sVertexOrderby.length() > 0) {
						sVertexOrderby.append(", ");
					}
					sVertexOrderby.append(col.getName());
				}
			}
		}
		
		if (e.getEndVertex() != null) {
			PK pk = e.getEndVertex().getPK();
			if (pk != null) {
				// if it has a pk, a pk scan is better than full range scan
				for (String pkCol : pk.getPkColumns()) {
					if (eVertexOrderby.length() > 0) {
						eVertexOrderby.append(", ");
					}
					eVertexOrderby.append(pkCol);
				}
			} else {
				Vertex endVertex = e.getEndVertex();
				
				for (Column col : endVertex.getColumnList()) {
					if (col.getDataType().equalsIgnoreCase("ID")) 
						continue;
					
					if (eVertexOrderby.length() > 0) {
						eVertexOrderby.append(", ");
					}
					eVertexOrderby.append(col.getName());
				}
			}
		}
		
		String innerTableWhere = null;
		
		buffer.append("SELECT * /*+ use_merge */FROM (SELECT rownum as rnum, ");
		buffer.append(startIdWithVertexName);		
		buffer.append(", ");
		buffer.append(endIdWithVertexName);
		
		buffer.append(" FROM ");
		
		if (innerTableName.equals(e.getStartVertexName())) {
			buffer.append("(SELECT ROWNUM as ");
			buffer.append(startId);
			
			innerTableWhere = startId;
			
		} else {
			buffer.append("(SELECT ROWNUM as ");
			buffer.append(startId);
		}
		
		if (sVertexOrderby.toString().contains(fkCol)) {
			buffer.append(", " + sVertexOrderby);
		} else {
			buffer.append(", " + sVertexOrderby + ", " + fkCol);						
		}
		
		buffer.append(" FROM " + startVertexName + " ORDER BY " + sVertexOrderby + ")");
		buffer.append(" as " + startVertexName + ", ");
		
		if (innerTableName.equals(e.getEndVertexName())) {
			buffer.append("(SELECT ROWNUM as ");
			buffer.append(endId);
			
			innerTableWhere = endId;
		} else {
			buffer.append("(SELECT ROWNUM as ");
			buffer.append(endId);
		}
		
		if (eVertexOrderby.toString().contains(refCol)) {
			buffer.append(", " + eVertexOrderby);
		} else {
			buffer.append(", " + eVertexOrderby + ", " + refCol);
		}
		
		buffer.append(" FROM " + endVertexName + " ORDER BY " + eVertexOrderby + ")");
		buffer.append(" as " + endVertexName);
		
		buffer.append(" WHERE ");
		
		buffer.append(startVertexName + "." + fkCol);
		buffer.append(" = ");
		buffer.append(endVertexName + "." + refCol);
		
		buffer.append(")");
		
		return buffer.toString();
	}
	
	@Override
	public String getJoinTableInnerQuery(Edge e, String sql, Connection conn, long innerTotalExported, long realPageCount) {
		
		sql = sql.replaceFirst("SELECT", "SELECT ROWNUM AS rnum, ");
		
		String buffer = new String(sql);
		
		Map<String, String> fkMapping = e.getfkCol2RefMapping();
		List<String> keySet = e.getFKColumnNames();
		
		String innerTableName = getInnerTable(conn, e);
		
		String startVertexName;
		String endVertexName;
		
		if (e.getStartVertexName().equals(e.getEndVertexName())) {
			startVertexName = e.getStartVertexName() + "_1";
			endVertexName = e.getEndVertexName() + "_2";
		} else {
			startVertexName = e.getStartVertexName();
			endVertexName = e.getEndVertexName();
		}
		
		String startFkCol = addDoubleQuote(keySet.get(0));
		String startRefCol = addDoubleQuote(fkMapping.get(keySet.get(0)));
		
		String endFkCol = addDoubleQuote(keySet.get(1));
		String endRefCol = addDoubleQuote(fkMapping.get(keySet.get(1)));
		
		StringBuilder sVertexOrderby = new StringBuilder();
		StringBuilder eVertexOrderby = new StringBuilder();
		
		String startIdWithVertexName = startVertexName + ".\":START_ID(" + startVertexName + ")\"";
		String endIdWithVertexName = endVertexName + ".\":END_ID(" + endVertexName + ")\"";

		String startId = "\":START_ID(" + startVertexName + ")\"";
		String endId = "\":END_ID(" + endVertexName + ")\"";
		
		if (e.getStartVertex() != null) {
			PK pk = e.getStartVertex().getPK();
			if (pk != null) {
				// if it has a pk, a pk scan is better than full range scan
				for (String pkCol : pk.getPkColumns()) {
					if (sVertexOrderby.length() > 0) {
						sVertexOrderby.append(", ");
					}
					sVertexOrderby.append(pkCol);
				}
			}
		}
		
		if (e.getEndVertex() != null) {
		
		PK pk = e.getEndVertex().getPK();
		if (pk != null) {
				// if it has a pk, a pk scan is better than full range scan
				for (String pkCol : pk.getPkColumns()) {
					if (eVertexOrderby.length() > 0) {
						eVertexOrderby.append(", ");
					}
					eVertexOrderby.append(pkCol);
				}
			}
		}
		
		String innerTableWhere = new String();
		
		Pattern startIdPattern = Pattern.compile("\":START_ID(\\([^)]*\\))\"", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher startIdMatcher = startIdPattern.matcher(buffer);
		
		if (startIdMatcher.find()) {
			innerTableWhere = startId;
			
			buffer = startIdMatcher.replaceFirst(startIdWithVertexName);
		}
		
		Pattern endIdPattern = Pattern.compile("\":END_ID(\\([^)]*\\))\"", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher endIdMatcher = endIdPattern.matcher(buffer);
		
		if (endIdMatcher.find()) {
			innerTableWhere = endId;
			
			buffer = endIdMatcher.replaceFirst(endIdWithVertexName);
		}
		
		buffer = buffer += (" AS MAIN, ");
		
		StringBuffer conditionBuffer = new StringBuffer(buffer);
		
		conditionBuffer.insert(0, "SELECT * FROM (");
		
		conditionBuffer.append("(SELECT ");
		
		conditionBuffer.append("ROWNUM");
		
		conditionBuffer.append(" AS ");
		conditionBuffer.append(startId);
		conditionBuffer.append(", ");
		conditionBuffer.append(sVertexOrderby);
		conditionBuffer.append(" FROM ");
		conditionBuffer.append(startVertexName);
		conditionBuffer.append(")");
		conditionBuffer.append(" AS ");
		conditionBuffer.append(startVertexName);
		conditionBuffer.append(", ");
		
		conditionBuffer.append("(SELECT ");
		
		conditionBuffer.append("ROWNUM");
		
		conditionBuffer.append(" AS ");
		conditionBuffer.append(endId);
		conditionBuffer.append(", ");
		conditionBuffer.append(eVertexOrderby);
		conditionBuffer.append(" FROM ");
		conditionBuffer.append(endVertexName);
		conditionBuffer.append(")");
		conditionBuffer.append(" AS ");
		conditionBuffer.append(endVertexName);
		
		//append where query
		
		conditionBuffer.append(" WHERE ");
		conditionBuffer.append(startVertexName + "." + startRefCol);
		conditionBuffer.append(" = ");
		conditionBuffer.append("MAIN." + startFkCol);
		
		conditionBuffer.append(" AND ");
		
		conditionBuffer.append(endVertexName + "." + endRefCol);
		conditionBuffer.append(" = ");
		conditionBuffer.append("MAIN." + endFkCol);
		
		conditionBuffer.append(")");
		
		return conditionBuffer.toString();
	}
	
	@Override
	public String getPagedSelectSQLForVertexCSV(Vertex v, String sql, long rows, long exportedRecords, PK pk) {
		StringBuilder orderby = new StringBuilder();
		int pkCount = 0;
		
		if (pk != null) {
			pkCount = pk.getPkColumns().size();			
		} else {
			pkCount = 0;
		}
		
		if (pkCount != 0) {
			// if it has a pk, a pk scan is better than full range scan
			for (String pkCol : pk.getPkColumns()) {
				if (orderby.length() > 0) {
					orderby.append(", ");
				}
				orderby.append("\"").append(pkCol).append("\"");
			}
			
		} else {
			System.out.println("Vertex [" + v.getVertexLabel() + "] : pk is null");
		}
	
		String editedQuery = editQueryForVertexCSV(v, sql, orderby.toString(), pkCount);
		
		StringBuilder buf = new StringBuilder();
		
		if (pkCount >= 2) {
			buf.append("SELECT * FROM (");
			buf.append(editedQuery.trim());
			buf.append(")");
		} else if (pkCount == 1) {
			buf.append("SELECT ");
			buf.append(editedQuery.trim());
			buf.append(" ORDER BY " + orderby);
		} else {
			buf.append(editedQuery.trim());
			
			StringBuffer allColumn = new StringBuffer();
			
			List<Column> columnList = v.getColumnList();
			
			for (int i = 1; i < columnList.size(); i++) {
				if (i > 1) {
					allColumn.append(',');
				}
				allColumn.append(getQuotedObjName(columnList.get(i).getName()));
			}
			
			buf.append(" ORDER BY " + allColumn);
		}
		
		return buf.toString(); 
	}
	
	private String editQueryForVertexCSV(Vertex v, String sql, String orderby, int pkCount) {
		Pattern selectPattern = Pattern.compile("SELECT\\s", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		Matcher selectMatcher = selectPattern.matcher(sql);
		
		if (selectMatcher.find()) {
			if (orderby != null && !orderby.isEmpty()) {
				if (pkCount < 2) {
					StringBuffer buffer = new StringBuffer("ROWNUM");
					buffer.append(" AS ");
					sql = selectMatcher.replaceFirst(buffer.toString());
				} else {
					StringBuffer buffer = new StringBuffer("SELECT ROW_NUMBER() OVER (ORDER BY ");
					buffer.append(orderby);
					buffer.append(") AS ");
					sql = selectMatcher.replaceFirst(buffer.toString());
				}
			} else {
				StringBuffer buffer = new StringBuffer("SELECT ROWNUM as ");
				sql = selectMatcher.replaceFirst(buffer.toString());
			}
		}
		
		return sql;
	}
	
	@Override
	public boolean supportFastSearchWithPK(Connection conn) {
		return true;
	}

	@Override
	public String getGraphSelectSQL(Edge e, boolean targetIsCSV) {
		// TODO Auto-generated method stub
		return null;
	}
}
