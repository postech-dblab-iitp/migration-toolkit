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
package com.cubrid.cubridmigration.mssql;

import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;

/**
 * to provide methods and constants of data type in SQL Server
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-15
 */
public final class MSSQLDataTypeHelper extends
		DBDataTypeHelper { //NOPMD
	public static final int MAXSIZE = 1073741823;
	public static final int MSSQL_DT_DATETIMEOFFSET = -155;
	public static final int MSSQL_DT_SQL_VARIANT = -150;
	//public static final String NCHARMAXSIZE = "536870911";

	private static final long serialVersionUID = 1023086809683583695L;

	private static final String MSSQL_BIN_TYPES = "/bit/varbit/binary/varbinary/image/timestamp/";
	private static final String MSSQL_NVCHAR_TYPES = "/ntext/xml/sysname/";
	private static final String MSSQL_STR_TYPES = "/text/uniqueidentifier/";

	private static final MSSQLDataTypeHelper HELPER = new MSSQLDataTypeHelper();

	//	/**
	//	 * return whether column is money data type
	//	 * 
	//	 * @param sqlServerColumn Column
	//	 * @return boolean
	//	 */
	//	public static boolean isMoney(Column sqlServerColumn) {
	//		String dataType = sqlServerColumn.getDataType();
	//		return "smallmoney".equals(dataType) || "money".equals(dataType);
	//	}

	//	/**
	//	 * return whether column is number data type
	//	 * 
	//	 * @param col Column
	//	 * @return boolean
	//	 */
	//	public static boolean isNumber(Column col) {
	//		String dataType = col.getDataType();
	//
	//		return "float".equals(dataType) || "real".equals(dataType)
	//				|| "tinyint".equals(dataType) || "smallint".equals(dataType)
	//				|| "int".equals(dataType) || "bigint".equals(dataType)
	//				|| "numeric".equals(dataType) || "decimal".equals(dataType)
	//				|| "bit".equals(dataType);
	//	}

	//	/**
	//	 * return whether column is time data type
	//	 * 
	//	 * @param sqlServerColumn Column
	//	 * @return boolean
	//	 */
	//	public static boolean isTime(Column sqlServerColumn) {
	//		String dataType = sqlServerColumn.getDataType();
	//
	//		return "datetime".equals(dataType) || "smalldatetime".equals(dataType)
	//				|| "date".equals(dataType) || "time".equals(dataType)
	//				|| "datetime2".equals(dataType)
	//				|| "datetimeoffset".equals(dataType);
	//	}
	//	/**
	//	 * format column value
	//	 * 
	//	 * @param columnValue String
	//	 * @param sqlServerColumn Column
	//	 * @return String
	//	 */
	//	public String formatColumnValue(String columnValue, Column sqlServerColumn) {
	//		String formatColumnValue = columnValue;
	//
	//		if (isString(sqlServerColumn) || isTime(sqlServerColumn)) {
	//			formatColumnValue = "'" + columnValue + "'";
	//		} else if (isNString(sqlServerColumn)) {
	//			formatColumnValue = "N'" + columnValue + "'";
	//		} else if (isBinary(sqlServerColumn)) {
	//			formatColumnValue = "0x" + columnValue;
	//		} else if (isMoney(sqlServerColumn)) {
	//			formatColumnValue = "$" + columnValue;
	//		}
	//
	//		return formatColumnValue;
	//	}

	/**
	 * Singleton
	 * 
	 * @param version of oracle database
	 * @return DataTypeHelper
	 */
	public static MSSQLDataTypeHelper getInstance(String version) {
		return HELPER;
	}

	private MSSQLDataTypeHelper() {
		//
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.MSSQL;
	}

	/**
	 * return data type id
	 * 
	 * @param catalog Catalog
	 * @param dataType String
	 * @param precision Integer
	 * @param scale Integer
	 * 
	 * @return Integer
	 */
	public Integer getJdbcDataTypeID(Catalog catalog, String dataType,
			Integer precision, Integer scale) {
		String key = dataType;
		Map<String, List<DataType>> supportedDataType = catalog.getSupportedDataType();

		List<DataType> dataTypeList = supportedDataType.get(key);
		if (dataTypeList == null) {
			throw new IllegalArgumentException(
					"Not supported SQL Server data type(" + dataType + ")");
		}
		if (dataTypeList.size() == 1) {
			return dataTypeList.get(0).getJdbcDataTypeID();
		}
		throw new IllegalArgumentException(
				"Not supported  SQL Server data type(" + dataType + ": p="
						+ precision + ", s=" + scale + ")");
	}

	/**
	 * generate data type via JDBC meta data information of CUBRID
	 * 
	 * @param column Column
	 * @return String
	 */
	public String getShownDataType(Column column) {
		String colType = column.getDataType() == null ? ""
				: column.getDataType();
		Integer precision = column.getPrecision();
		Integer scale = column.getScale();
		colType = colType.replace("identity", "").trim();
		if (checkType("/text/xml/", colType)) {
			return colType;
		} else if (isGenericString(colType)) {
			return colType + "(" + precision + ")";
		} else if (checkType("/bit/varbit/binary/varbinary/", colType)) {
			return colType + "(" + precision + ")";
		} else if (checkType("/decimal/numeric/", colType)) {
			return colType + "(" + precision + "," + scale + ")";
		}
		return colType;
	}

	/**
	 * return whether column is binary data type
	 * 
	 * @param dataType Column
	 * @return boolean
	 */
	public boolean isBinary(String dataType) {
		return checkType(MSSQL_BIN_TYPES, dataType);
	}

	/**
	 * MSSQL server does not support collection type.
	 * 
	 * @param dataType Integer
	 * @return false
	 */
	public boolean isCollection(String dataType) {
		return false;
	}

	/**
	 * MSSQL NVARCHAR
	 * 
	 * @param dataType String
	 * @return true if it is nvarchar type
	 */
	public boolean isNVarchar(String dataType) {
		return super.isNVarchar(dataType)
				|| checkType(MSSQL_NVCHAR_TYPES, dataType);
	}

	/**
	 * MSSQL VARCHAR
	 * 
	 * @param dataType String
	 * @return true if it is varchar type
	 */
	public boolean isVarchar(String dataType) {
		return super.isVarchar(dataType)
				|| checkType(MSSQL_STR_TYPES, dataType);
	}

}
