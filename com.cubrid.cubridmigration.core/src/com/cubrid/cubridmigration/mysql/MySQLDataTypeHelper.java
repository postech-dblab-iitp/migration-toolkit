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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;

/**
 * 
 * MySqlDataTypeHelper Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-21 created by Kevin Cao
 */
public final class MySQLDataTypeHelper extends
		DBDataTypeHelper {
	private static final List<String> DATA_TYPE_1 = new ArrayList<String>();

	private static final List<String> DATA_TYPE_2 = new ArrayList<String>();

	private static final List<String> DATA_TYPE_3 = new ArrayList<String>();

	private static final List<String> DATA_TYPE_4 = new ArrayList<String>();

	private static final List<String> DATA_TYPE_5 = new ArrayList<String>();

	private static final MySQLDataTypeHelper HELPER = new MySQLDataTypeHelper();

	static {
		DATA_TYPE_1.add("tinyblob");
		DATA_TYPE_1.add("tinytext");
		DATA_TYPE_1.add("blob");
		DATA_TYPE_1.add("text");
		DATA_TYPE_1.add("mediumblob");
		DATA_TYPE_1.add("mediumtext");
		DATA_TYPE_1.add("longblob");
		DATA_TYPE_1.add("longtext");
		DATA_TYPE_1.add("time");
		DATA_TYPE_1.add("date");
		DATA_TYPE_1.add("timestamp");
		DATA_TYPE_1.add("datetime");

		DATA_TYPE_2.add("char");
		DATA_TYPE_2.add("varchar");
		DATA_TYPE_2.add("tinyint");
		DATA_TYPE_2.add("smallint");
		DATA_TYPE_2.add("mediumint");
		DATA_TYPE_2.add("int");
		DATA_TYPE_2.add("bigint");
		DATA_TYPE_2.add("bit");
		DATA_TYPE_2.add("binary");
		DATA_TYPE_2.add("year");
		DATA_TYPE_2.add("varbinary");

		DATA_TYPE_3.add("float");
		DATA_TYPE_3.add("double");
		DATA_TYPE_3.add("decimal");

		DATA_TYPE_4.add("enum");
		DATA_TYPE_4.add("set");

		DATA_TYPE_5.add("tinyblob");
		DATA_TYPE_5.add("blob");
		DATA_TYPE_5.add("mediumblob");
		DATA_TYPE_5.add("longblob");
		DATA_TYPE_5.add("bit");
	}

	/**
	 * Singleton
	 * 
	 * @param version of oracle database
	 * @return DataTypeHelper
	 */
	public static MySQLDataTypeHelper getInstance(String version) {
		return HELPER;
	}

	private MySQLDataTypeHelper() {
		//Do nothing here.
	}

	/**
	 * Retrieves the Database type.
	 * 
	 * @return DatabaseType
	 */
	public DatabaseType getDBType() {
		return DatabaseType.MYSQL;
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
			throw new IllegalArgumentException("Not supported MySQL data type("
					+ dataType + ")");
		}

		if (dataTypeList.size() == 1) {
			return dataTypeList.get(0).getJdbcDataTypeID();
		}

		throw new IllegalArgumentException("Not supported  MySQL data type("
				+ dataType + ": p=" + precision + ", s=" + scale + ")");
	}

	/**
	 * generate data type via JDBC meta data information of CUBRID
	 * 
	 * @param column Column
	 * @return String
	 */
	public String getShownDataType(Column column) {
		String type = column.getDataType();
		Integer precision = column.getPrecision();
		Integer scale = column.getScale();
		int index = type.indexOf(" unsigned");
		String colType = type;
		String remain = "";

		if (index > 0) {
			colType = type.substring(0, index);
			remain = " unsigned";
		}
		if (DATA_TYPE_1.indexOf(colType) >= 0) {
			return colType + remain;
		} else if (DATA_TYPE_2.indexOf(colType) >= 0) {
			return colType + "(" + precision + ")" + remain;

		} else if (DATA_TYPE_3.indexOf(colType) >= 0) {
			return colType + "(" + precision + "," + scale + ")" + remain;

		} else if (DATA_TYPE_4.indexOf(colType) >= 0) {
			return colType + remain;
		}
		return colType + remain;
	}

	/**
	 * Retrieves if The data type of column is binary type such as blob/bit ...
	 * 
	 * @param dataType Column
	 * @return true or false
	 */
	public boolean isBinary(String dataType) {
		return DATA_TYPE_5.indexOf(dataType) >= 0;
	}

	/**
	 * MSSQL server does not support collection type.
	 * 
	 * @param dataType Integer
	 * @return false
	 */
	public boolean isCollection(String dataType) {
		return checkType("/set/", dataType);
	}

	/**
	 * return the type part of a special data type,eg: <li>character(10), return
	 * "character" <li>set(integer), return "set" <li>integer, return "integer"
	 * 
	 * @param type String
	 * @return String
	 */
	public String parseMainType(String type) {
		int index = type.indexOf(" unsigned");
		String colType = type;
		String remain = "";

		if (index > 0) {
			colType = type.substring(0, index);
			remain = " unsigned";
		}

		index = colType.indexOf("(");

		if (-1 == index) { //the simplest case
			return colType + remain;
		} else { //the case like: set_of(bit) ,numeric(4,2)
			return colType.substring(0, index) + remain;
		}
	}

	/**
	 * return valid letter or digital number
	 * 
	 * @param jdbcType String
	 * @return int
	 */
	public int parsePrecision(String jdbcType) {
		String typeRemain = parseTypeRemain(jdbcType);

		if (null == typeRemain) {
			return -1;
		} else {
			String typePart = parseMainType(jdbcType);

			if ("enum".equals(typePart) || "set".equals(typePart)) {
				return -1;
			}

			if (typeRemain.endsWith(") unsigne")) {
				typeRemain = typeRemain.replaceAll("\\) unsigne", "");
			}

			int index = typeRemain.indexOf(",");

			if (index == -1) {
				return Integer.parseInt(typeRemain);
			} else {
				return Integer.parseInt(typeRemain.substring(0, index));
			}
		}
	}

	/**
	 * return float digital number
	 * 
	 * @param jdbcType String
	 * @return Integer
	 */
	public Integer parseScale(String jdbcType) {
		String typeRemain = parseTypeRemain(jdbcType);

		if (null == typeRemain) {
			return null;
		} else {
			String typePart = parseMainType(jdbcType);

			if ("enum".equals(typePart) || "set".equals(typePart)) {
				return null;
			}

			if (typeRemain.endsWith(") unsigne")) {
				typeRemain = typeRemain.replaceAll("\\) unsigne", "");
			}

			int index = typeRemain.indexOf(",");

			if (index == -1) {
				return null;
			} else {

				return Integer.parseInt(typeRemain.substring(index + 1,
						typeRemain.length()));
			}
		}
	}

	/**
	 * return the remained part of a special data type,eg: <li>character(10),
	 * return "10" <li>set(integer), return "integer" <li>integer, return null
	 * 
	 * @see #parseMainType(String)
	 * 
	 * @param type String
	 * @return String
	 */
	public String parseTypeRemain(String type) {
		int index = type.indexOf("(");

		if (-1 == index) { //the simplest case
			return null;
		} else { //the case like: set_of(bit) ,numeric(4,2)
			return type.substring(index + 1, type.length() - 1);
		}
	}

	/**
	 * Retrieves is the year type.
	 * 
	 * @param dataType String
	 * @return true if is year
	 */
	public boolean isYear(String dataType) {
		return checkType("/year/", dataType);
	}

}
