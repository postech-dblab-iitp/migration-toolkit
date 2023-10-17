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

import java.sql.Types;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;

/**
 * OracleDataTypeHelper Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-21 created by Kevin Cao
 */
public final class OracleDataTypeHelper extends
		DBDataTypeHelper {
	private static final String NUMBER = "NUMBER";

	private static final OracleDataTypeHelper HELPER = new OracleDataTypeHelper();

	/**
	 * Singleton
	 * 
	 * @param version of oracle database
	 * @return DataTypeHelper
	 */
	public static OracleDataTypeHelper getInstance(String version) {
		return HELPER;
	}

	/**
	 * get the Number type's Show Type
	 * 
	 * @param precision Integer
	 * @param scale Integer
	 * @return NUMBER(x),NUMBER(x,y),NUMBER
	 */
	private static String getNumberShowType(Integer precision, Integer scale) {
		if (precision == null || precision == 0) {
			return NUMBER;
		}
		if (scale == null) {
			return "NUMBER(" + precision + ")";
		}
		return "NUMBER(" + precision + "," + scale + ")";
	}

	/**
	 * getNumberType
	 * 
	 * @param precision Integer
	 * @param scale Integer
	 * @return Integer
	 */
	private static Integer getNumberType(Integer precision, Integer scale) {
		if (precision == null) {
			if (scale == null) {
				return Types.NUMERIC;
			} else if (scale == 0) {
				return Types.BIGINT;
			}
		} else if (scale == null || scale == 0) {
			if (precision == 1) {
				return Types.BIT;
			} else if (precision == 3) {
				return Types.TINYINT;
			} else if (precision == 5) {
				return Types.SMALLINT;
			} else if (precision <= 10) {
				return Types.INTEGER;
			} else if (precision <= 38) {
				return Types.BIGINT;
			}
		}
		return Types.NUMERIC;
	}

	/**
	 * return get Oracle data type key
	 * 
	 * @param dataType String
	 * @return String
	 */
	public static String getOracleDataTypeKey(String dataType) {
		String key = dataType;

		if (dataType.matches("TIMESTAMP\\(\\d*\\)")) {
			key = "TIMESTAMP";
		} else if (dataType.matches("TIMESTAMP\\(\\d*\\) WITH TIME ZONE")) {
			key = "TIMESTAMP WITH TIME ZONE";
		} else if (dataType.matches("TIMESTAMP\\(\\d*\\) WITH LOCAL TIME ZONE")) {
			key = "TIMESTAMP WITH LOCAL TIME ZONE";
		} else if (dataType.matches("INTERVAL DAY\\(\\d*\\) TO SECOND\\(\\d*\\)")) {
			key = "INTERVALDS";
		} else if (dataType.matches("INTERVAL YEAR\\(\\d*\\) TO MONTH")) {
			key = "INTERVALYM";
		}
		return key;
	}

	private OracleDataTypeHelper() {
		//Do nothing here.
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.ORACLE;
	}

	/**
	 * return data type id TODO: to be refactory
	 * 
	 * @param catalog Catalog
	 * @param dataType String
	 * @param precision Integer
	 * @param scale Integer
	 * @return Integer
	 */
	public Integer getJdbcDataTypeID(Catalog catalog, String dataType,
			Integer precision, Integer scale) {
		if (NUMBER.equals(dataType)) {
			return getNumberType(precision, scale);
		} else if ("DATE".equals(dataType)) {
			return Types.DATE;
		} else if ("NCHAR".equals(dataType) || "NVARCHAR2".equals(dataType)) {
			return Types.CHAR;
		} else if ("NCLOB".equals(dataType)) {
			return Types.CLOB;
		} else if ("LONG".equals(dataType)) {
			return Types.CLOB;
		} else if ("BINARY_FLOAT".equals(dataType)) {
			return Types.FLOAT;
		} else if ("BINARY_DOUBLE".equals(dataType)) {
			return Types.DOUBLE;
		} else if ("BFILE".equals(dataType) || "ROWID".equals(dataType)
				|| "UROWID".equals(dataType)) {
			return null;
		} else if ("INTEGER".equals(dataType)) {
			return Types.INTEGER;
		}

		String key = getOracleDataTypeKey(dataType);

		//REAL, RAW, BLOB, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE, 
		//VARCHAR2, LONG RAW, NUMBER, CLOB, CHAR, STRUCT, FLOAT, DATE, LONG, 
		//INTERVALDS, INTERVALYM, ARRAY, TIMESTAMP, REF

		Map<String, List<DataType>> supportedDataType = catalog.getSupportedDataType();
		List<DataType> dataTypeList = supportedDataType.get(key);

		if (dataTypeList == null) {
			throw new IllegalArgumentException(
					"Not supported Oracle data type(" + dataType + ")");
		}

		if (dataTypeList.size() == 1) {
			return dataTypeList.get(0).getJdbcDataTypeID();
		}

		throw new IllegalArgumentException("Not supported Oracle data type("
				+ dataType + ": p=" + precision + ", s=" + scale + ")");

	}

	/**
	 * generate data type via JDBC meta data information of CUBRID
	 * 
	 * @param column Column
	 * @return String
	 */
	public String getShownDataType(Column column) {
		String colType = column.getDataType();
		Integer precision = column.getPrecision();
		Integer scale = column.getScale();
		if (isString(colType)) {
			if ("C".equals(column.getCharUsed())) {
				return colType + "(" + precision + " CHAR)";
			} else {
				return colType + "(" + precision + ")";
			}
		} else if (isNString(colType)) {
			return colType + "(" + precision + ")";
		} else if ("RAW".equals(colType)) {
			return colType + "(" + precision + ")";
		} else if (NUMBER.equals(colType)) {
			return getNumberShowType(precision, scale);
		} else if ("FLOAT".equals(colType)) {
			if (precision == 126) {
				return "FLOAT";
			} else {
				return "FLOAT(" + precision + ")";
			}
		} else if ("TIMESTAMP".equals(colType)) {
			return "TIMESTAMP(" + scale + ")";
		} else if ("TIMESTAMPTZ".equals(colType)) {
			return "TIMESTAMP(" + scale + ") WITH TIME ZONE";
		} else if ("TIMESTAMPLTZ".equals(colType)) {
			return "TIMESTAMP(" + scale + ") WITH LOCAL TIME ZONE";
		} else if ("INTERVALDS".equals(colType)) {
			return "INTERVAL DAY(" + precision + ") TO SECOND(" + scale + ")";
		} else if ("INTERVALYM".equals(colType)) {
			return "INTERVAL YEAR(" + precision + ") TO MONTH";
		}
		//		else if (checkType(
		//				"/LONG/LONG RAW/CLOB/NCLOB/BLOB/ROWID/DATE/BINARY_FLOAT/BINARY_DOUBLE/",
		//				colType)) {
		//			return colType;
		//		} 
		return colType;
	}

	/**
	 * Retrieves if The data type of column is binary type such as blob/bit ...
	 * 
	 * @param dataType Column
	 * @return true or false
	 */
	public boolean isBinary(String dataType) {
		return "blob".equalsIgnoreCase(dataType);
	}

	/**
	 * Oracle server does not support collection type.
	 * 
	 * @param dataType Integer
	 * @return false
	 */
	public boolean isCollection(String dataType) {
		return false;
	}
}
