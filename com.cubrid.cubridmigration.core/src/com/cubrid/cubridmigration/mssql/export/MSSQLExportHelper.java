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
package com.cubrid.cubridmigration.mssql.export;

import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.core.export.handler.CharTypeHandler;
import com.cubrid.cubridmigration.mssql.MSSQLDataTypeHelper;
import com.cubrid.cubridmigration.mssql.MSSQLSQLHelper;

/**
 * a class help to export MSSQL data and verify MSSQL sql statement
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-11-11
 */
public class MSSQLExportHelper extends
		DBExportHelper {
	//private static boolean isJVM16 = isJVM16();
	//private static final Logger LOG = LogUtil.getLogger(MSSQLExportHelper.class);

	//private static final String SQL_SERVER_ROW_NUMBER = "SQLServerRowNumber";

	//	/**
	//	 * JVM is 1.6 or upper.
	//	 * 
	//	 * @return true if 1.6 or upper
	//	 */
	//	private static boolean isJVM16() {
	//		final String version = System.getProperty("java.version", "");
	//		return "1.6".compareTo(version) <= 0;
	//	}

	/**
	 * constructor
	 */
	public MSSQLExportHelper() {
		super();
		handlerMap1.put(MSSQLDataTypeHelper.MSSQL_DT_DATETIMEOFFSET, new CharTypeHandler());
		handlerMap1.put(MSSQLDataTypeHelper.MSSQL_DT_SQL_VARIANT, new CharTypeHandler());
	}

	//	/** 
	//	 * to return some column of table for row number statistic
	//	 * 
	//	 * @param table Table
	//	 * @return String
	//	 */
	//	public String getRowNumberColumns(Table table) {
	//		String firstColumnName = getKeyColumns(table);
	//
	//		if (null == firstColumnName) {
	//			List<Column> columnList = table.getColumns();
	//
	//			for (Column column : columnList) {
	//				if ("text".equals(column.getDataType())
	//						|| "ntext".equals(column.getDataType())
	//						|| "image".equals(column.getDataType())) {
	//					continue;
	//				}
	//
	//				return getQuoteStr(column.getName());
	//			}
	//		}
	//
	//		return firstColumnName;
	//	}

	//	/**
	//	 * get column name string
	//	 * 
	//	 * @param sourceTable Table
	//	 * @param columnList List<Column>
	//	 * @return String
	//	 */
	//	protected String innerGetColumnNames(final Table sourceTable,
	//			final List<Column> columnList) {
	//		if (columnList.isEmpty()) {
	//			return "'" + SQL_SERVER_ROW_NUMBER + "'";
	//		} else {
	//			StringBuffer buf = new StringBuffer(256);
	//			String firstColumnName = getRowNumberColumns(sourceTable);
	//
	//			if (firstColumnName == null) {
	//				throw new IllegalArgumentException(
	//						"all table columns are text, ntext or image data type, "
	//								+ "so failed to get row number column");
	//			}
	//
	//			for (Column column : columnList) {
	//				String columnName = getQuoteStr(column.getName());
	//				if ("sql_variant".equalsIgnoreCase(column.getDataType())) {
	//					buf.append(" cast (" + columnName + " as varchar(8000)) "
	//							+ columnName);
	//				} else {
	//					buf.append(columnName);
	//				}
	//
	//				buf.append(',');
	//			}
	//
	//			buf.append("ROW_NUMBER() OVER (ORDER BY ").append(
	//					firstColumnName + ") AS '" + SQL_SERVER_ROW_NUMBER + "'");
	//
	//			return buf.toString();
	//		}
	//
	//	}

	//	/**
	//	 * return database special table name
	//	 * 
	//	 * @param sourceTable Table
	//	 * @return String
	//	 */
	//	public String getTableNameKey(Table sourceTable) {
	//		String schemaName = sourceTable.getName();
	//
	//		if (schemaName == null) {
	//			return sourceTable.getName();
	//		} else {
	//			return schemaName + "_" + sourceTable.getName();
	//		}
	//	}

	//	/**
	//	 * return database special table name
	//	 * 
	//	 * @param sourceTable Table
	//	 * @return String
	//	 */
	//	public String getTableName(Table sourceTable) {
	//		if (sourceTable.getTableType() == TabelConstant.SOURCE_TABLE_TYPE_TABLE) {
	//			return getQuoteStr(sourceTable.getCatalogName()) + "."
	//					+ getQuoteStr(sourceTable.getSchemaName()) + "."
	//					+ getQuoteStr(sourceTable.getName());
	//		} else {
	//			return "(" + sourceTable.getSql() + ") Temp";
	//		}
	//	}

	//	/**
	//	 * get JDBC Object
	//	 * 
	//	 * @param rs ResultSet
	//	 * @param column Column
	//	 * @param getValueByName boolean
	//	 * @param columnIndex int
	//	 * @param dbClassLoader ClassLoader
	//	 * 
	//	 * @return Object
	//	 * @throws IOException e
	//	 * @throws SQLException e
	//	 */
	//	public Object getJdbcObject(final ResultSet rs, final Column column,
	//			final boolean getValueByName, final int columnIndex,
	//			ClassLoader dbClassLoader) throws IOException, SQLException {
	//
	//		if (getValueByName) {
	//			return getJDBCValueByName(rs, column, dbClassLoader);
	//		} else {
	//			return getJDBCValueByIndex(rs, column, columnIndex, dbClassLoader);
	//		}
	//
	//	}

	//	/**
	//	 * get JDBC Object
	//	 * 
	//	 * @param rs ResultSet
	//	 * @param column Column
	//	 * @param columnIndex int
	//	 * @param dbClassLoader ClassLoader
	//	 * 
	//	 * @return Object
	//	 * @throws IOException e
	//	 * @throws SQLException e
	//	 */
	//	private Object getJDBCValueByIndex(final ResultSet rs, final Column column,
	//			final int columnIndex, ClassLoader dbClassLoader) throws SQLException,
	//			IOException {
	//		Integer dataTypeID = column.getJdbcIDOfDataType();
	//		String dataType = column.getDataType();
	//
	//		if ("sql_variant".equalsIgnoreCase(dataType)) {
	//			return rs.getString(columnIndex);
	//		}
	//
	//		if (isJVM16) {
	//			if (dataTypeID == NCHAR || dataTypeID == NVARCHAR) {
	//				try {
	//					Method method = Class.forName(
	//							"com.microsoft.sqlserver.jdbc.SQLServerResultSet",
	//							false, dbClassLoader).getMethod("getNString",
	//							new Class[]{int.class });
	//					return method.invoke(rs, columnIndex);
	//				} catch (Exception e) {
	//					LOG.error(LogUtil.getExceptionString(e));
	//					return null;
	//				}
	//
	//				//				return ((com.microsoft.sqlserver.jdbc.SQLServerResultSet)rs).getNString(columnName);
	//			} else if (dataTypeID == LONGNVARCHAR) {
	//				try {
	//					Method method = Class.forName(
	//							"com.microsoft.sqlserver.jdbc.SQLServerResultSet",
	//							false, dbClassLoader).getMethod(
	//							"getNCharacterStream", new Class[]{int.class });
	//					Reader reader = (Reader) method.invoke(rs, columnIndex);
	//					return getCharObject(reader);
	//				} catch (Exception e) {
	//					LOG.error(LogUtil.getExceptionString(e));
	//					return null;
	//				}
	//
	//				//				Reader reader = ((com.microsoft.sqlserver.jdbc.SQLServerResultSet)rs).getNCharacterStream(column.getName());
	//				//				return getCharObject(reader);
	//			} else if (dataTypeID == NCLOB) {
	//				try {
	//					Method method = Class.forName(
	//							"com.microsoft.sqlserver.jdbc.SQLServerResultSet",
	//							false, dbClassLoader).getMethod("getNClob",
	//							new Class[]{int.class });
	//					return method.invoke(rs, columnIndex);
	//				} catch (Exception e) {
	//					LOG.error(LogUtil.getExceptionString(e));
	//					return null;
	//				}
	//
	//				//				return ((com.microsoft.sqlserver.jdbc.SQLServerResultSet)rs).getNClob(columnName);
	//			}
	//		}
	//
	//		if (dataTypeID != null) {
	//			return super.getJdbcObject(rs, column);
	//		}
	//
	//		LOG.error("Unknown SQL Server data type:" + column.getDataType()
	//				+ "(Column name=" + column.getName() + ")");
	//		return null;
	//	}

	//	private Object getCharObject(Reader reader) throws IOException {
	//		StringBuffer sb = new StringBuffer();
	//		char[] read = new char[1024];
	//		int count = reader.read(read);
	//		while (count >= 0) {
	//			sb.append(read, 0, count);
	//			count = reader.read(read);
	//		}
	//		return sb.toString();
	//	}

	//	/**
	//	 * get JDBC Object
	//	 * 
	//	 * @param rs ResultSet
	//	 * @param column Column
	//	 * @param dbClassLoader ClassLoader
	//	 * 
	//	 * @return Object
	//	 * @throws IOException e
	//	 * @throws SQLException e
	//	 */
	//	private Object getJDBCValueByName(final ResultSet rs, final Column column,
	//			ClassLoader dbClassLoader) throws SQLException, IOException {
	//		Integer dataTypeID = column.getJdbcIDOfDataType();
	//		String columnName = column.getName();
	//
	//		String dataType = column.getDataType();
	//
	//		if ("sql_variant".equalsIgnoreCase(dataType)) {
	//			return rs.getString(columnName);
	//		}
	//
	//		if (isJVM16) {
	//			if (dataTypeID == NCHAR || dataTypeID == NVARCHAR) {
	//				try {
	//					Method method = Class.forName(
	//							"com.microsoft.sqlserver.jdbc.SQLServerResultSet",
	//							false, dbClassLoader).getMethod("getNString",
	//							new Class[]{String.class });
	//					return method.invoke(rs, columnName);
	//				} catch (Exception e) {
	//					LOG.error(LogUtil.getExceptionString(e));
	//					return null;
	//				}
	//
	//				//				return ((com.microsoft.sqlserver.jdbc.SQLServerResultSet)rs).getNString(columnName);
	//			} else if (dataTypeID == LONGNVARCHAR) {
	//				try {
	//					Method method = Class.forName(
	//							"com.microsoft.sqlserver.jdbc.SQLServerResultSet",
	//							false, dbClassLoader).getMethod(
	//							"getNCharacterStream", new Class[]{String.class });
	//					Reader reader = (Reader) method.invoke(rs, columnName);
	//					return getCharObject(reader);
	//				} catch (Exception e) {
	//					LOG.error(LogUtil.getExceptionString(e));
	//					return null;
	//				}
	//
	//				//				Reader reader = ((com.microsoft.sqlserver.jdbc.SQLServerResultSet)rs).getNCharacterStream(column.getName());
	//				//				return getCharObject(reader);
	//			} else if (dataTypeID == NCLOB) {
	//				try {
	//					Method method = Class.forName(
	//							"com.microsoft.sqlserver.jdbc.SQLServerResultSet",
	//							false, dbClassLoader).getMethod("getNClob",
	//							new Class[]{String.class });
	//					return method.invoke(rs, columnName);
	//				} catch (Exception e) {
	//					LOG.error(LogUtil.getExceptionString(e));
	//					return null;
	//				}
	//
	//				//				return ((com.microsoft.sqlserver.jdbc.SQLServerResultSet)rs).getNClob(columnName);
	//			}
	//		}
	//
	//		if (dataTypeID != null) {
	//			return super.getJdbcObject(rs, column);
	//		}
	//
	//		LOG.error("Unknown SQL Server data type:" + column.getDataType()
	//				+ "(Column name=" + column.getName() + ")");
	//		return null;
	//	}

	//	/**
	//	 * append record range information to sql statment
	//	 * 
	//	 * @param sql String
	//	 * @param sourceTable Table
	//	 * @return String
	//	 */
	//	public String getSQLAppendLimit(final String sql, final Table sourceTable) {
	//		long start = sourceTable.getStart();
	//		long splitSize = sourceTable.getSplitSize();
	//
	//		if (start == -1 || splitSize == -1) {
	//			return sql;
	//		} else {
	//			return "SELECT * FROM (" + sql + ") Temp" + start + " WHERE "
	//					+ SQL_SERVER_ROW_NUMBER + " BETWEEN " + (start + 1)
	//					+ " AND " + (start + splitSize);
	//		}
	//	}

	//	/**
	//	 * return whether a where condition is needed to padding a "where" String
	//	 * 
	//	 * @param whereCnd String
	//	 * @return boolean
	//	 */
	//	public boolean isWhereNeeded(String whereCnd) {
	//		//append "where" if necessary
	//		String whereCndTrim = whereCnd.trim();
	//
	//		return !(whereCndTrim.toUpperCase(Locale.ENGLISH).startsWith("WHERE ")
	//				|| whereCndTrim.toUpperCase(Locale.ENGLISH).startsWith("GROUP ")
	//				|| whereCndTrim.toUpperCase(Locale.ENGLISH).startsWith(
	//						"HAVING ")
	//				|| whereCndTrim.toUpperCase(Locale.ENGLISH).startsWith("ORDER ")
	//				|| whereCndTrim.toUpperCase(Locale.ENGLISH).startsWith(
	//						"PROCEDURE ") || whereCndTrim.toUpperCase(
	//				Locale.ENGLISH).startsWith("FOR "));
	//	}

	//	/**
	//	 * format partition value
	//	 * 
	//	 * @param column Column
	//	 * @param partitionValue String
	//	 * @return String
	//	 */
	//	public String formatPartitionValue(Column column, String partitionValue) {
	//
	//		MSSQLDataTypeHelper dtHelper = MSSQLDataTypeHelper.getInstance(null);
	//		String ptValue = dtHelper.formatColumnValue(partitionValue, column);
	//		String dataType = dtHelper.getShownDataType(column);
	//		ptValue = "CAST(" + ptValue + " AS " + dataType + ") ";
	//
	//		return ptValue;
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
		return MSSQLSQLHelper.getInstance(null).replacePageQueryParameters(sql, rows,
				exportedRecords);
	}

	/**
	 * return database object name
	 * 
	 * @param objectName String
	 * @return String
	 */
	protected String getQuotedObjName(String objectName) {
		return DatabaseType.MSSQL.getSQLHelper(null).getQuotedObjName(objectName);
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.MSSQL;
	}
}
