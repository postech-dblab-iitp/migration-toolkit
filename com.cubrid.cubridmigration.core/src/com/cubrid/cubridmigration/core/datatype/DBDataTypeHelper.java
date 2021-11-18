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
package com.cubrid.cubridmigration.core.datatype;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbtype.IDependOnDatabaseType;

/**
 * DBDataTypeHelper Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-8-27 created by Kevin Cao
 */
public abstract class DBDataTypeHelper implements
		IDependOnDatabaseType {

	protected static final String NUMERIC_TYPES = "/number/numeric/int/integer/short/smallint/mediumint/tinyint/real/float/double/bigint/decimal/dec/monetary/money/currency/";

	protected static final String AUTOINC_TYPES = "/int/integer/short/smallint/mediumint/tinyint/bigint/";

	protected static final String VARCHAR_TYPES = "/string/varchar/character varying/char varying/varchar2/";

	protected static final String NVARCHAR_TYPES = "/nvarchar/nchar varying/national character varying/nvarchar2/";

	protected static final String CHAR_TYPES = "/char/character/";

	protected static final String NCHAR_TYPES = "/nchar/national character/ncharacter/";

	protected static final String BINARY_TYPES = "/binary/varbinary/bit/bit varying/varbit/";

	//protected static final String LOB_TYPES = "/blob/clob/text/";

	/**
	 * Check if the input data type is in the model list.
	 * 
	 * @param model with /datatype1/datatype2/datatype3/ format, all characters
	 *        should be in lower case.
	 * @param dataType String
	 * @return true if model contains data type.
	 */
	protected boolean checkType(String model, String dataType) {
		if (StringUtils.isBlank(model)) {
			throw new IllegalArgumentException("MODEL String can't be empty.");
		}
		String plaintDT = getMainDataType(dataType).toLowerCase(Locale.US);
		if (StringUtils.isBlank(plaintDT)) {
			return false;
		}
		StringBuffer dt = new StringBuffer("/").append(plaintDT).append("/");
		return model.indexOf(dt.toString()) >= 0;
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
	public abstract Integer getJdbcDataTypeID(Catalog catalog, String dataType,
			Integer precision, Integer scale);

	/**
	 * Retrieves the main data type, for example: varchar(200) , varchar will be
	 * returned.
	 * 
	 * @param dataType full data type
	 * @return main data type
	 */
	public String getMainDataType(String dataType) {
		if (StringUtils.isBlank(dataType)) {
			return "";
		}
		int index = dataType.indexOf('(');

		if (index < 0) { // the simplest case
			return dataType.trim();
		} else { // the case like: set_of(bit) ,numeric(4,2)
			return dataType.substring(0, index).trim();
		}
	}

	/**
	 * return shown data type instance of a column
	 * 
	 * @param column Column
	 * @return String
	 */
	public abstract String getShownDataType(Column column);

	/**
	 * Retrieves if The data type of column is binary type such as blob/bit ...
	 * 
	 * @param dataType Column
	 * @return true or false
	 */
	public abstract boolean isBinary(String dataType);

	/**
	 * Retrieves the data type is char[] type
	 * 
	 * @param dataType String
	 * @return true if char
	 */
	public boolean isChar(String dataType) {
		return checkType(CHAR_TYPES, dataType);
	}

	/**
	 * return whether a data type is set type
	 * 
	 * @param dataType type
	 * @return boolean
	 */
	public abstract boolean isCollection(String dataType);

	/**
	 * if it is ENUM data type
	 * 
	 * @param dataType String
	 * @return true if is ENUM
	 */
	public boolean isEnum(String dataType) {
		return checkType("/enum/", dataType);
	}

	/**
	 * return whether data type is integer/short data type
	 * 
	 * @param dataType String
	 * @return boolean
	 */
	public boolean isInteger(String dataType) {
		return checkType("/short/smallint/int/integer/mediumint/tinyint/",
				dataType);
	}

	/**
	 * Retrieves the data type is numeric/integer/float/double/big integer......
	 * 
	 * @param dataType String, the input should not be with precision and scale.
	 *        for example: should be "numeric", not be "numeric(20)"
	 * @return true
	 */
	public boolean isGeneralizedNumeric(String dataType) {
		if (StringUtils.isBlank(dataType)) {
			throw new IllegalArgumentException("Data type should not be empty.");
		}
		String dt = dataType.toLowerCase(Locale.US);
		//Remove unsigned word
		int idx = dt.indexOf("unsigned");
		if (idx > 0) {
			dt = dt.substring(0, idx).trim();
		}
		return checkType(NUMERIC_TYPES, dt);
	}

	/**
	 * char,varchar,nchar,nvarchar types
	 * 
	 * @param dataType String
	 * @return boolean
	 */
	public boolean isGenericString(String dataType) {
		return isString(dataType) || isNString(dataType);
	}

	/**
	 * return whether column is NCHAR data type
	 * 
	 * @param dataType Column data type
	 * @return boolean
	 */
	public boolean isNChar(String dataType) {
		return checkType(NCHAR_TYPES, dataType);
	}

	/**
	 * return whether column is nchar data type
	 * 
	 * @param dataType Column data type
	 * @return boolean
	 */
	public boolean isNString(String dataType) {
		return isNChar(dataType) || isNVarchar(dataType);
	}

	/**
	 * Retrieves the data type is numeric/integer/float/double/big integer......
	 * 
	 * @param dataType String, the input should not be with precision and scale.
	 *        for example: should be "numeric", not be "numeric(20)"
	 * @return true
	 */
	public boolean isNumeric(String dataType) {
		if (StringUtils.isBlank(dataType)) {
			throw new IllegalArgumentException("Data type should not be empty.");
		}
		String dt = dataType.toLowerCase(Locale.US);
		//Remove unsigned word
		int idx = dt.indexOf("unsigned");
		if (idx > 0) {
			dt = dt.substring(0, idx).trim();
		}
		return checkType("/numeric/number/decimal/dec/", dt);
	}

	/**
	 * return whether column is NVARCHAR data type
	 * 
	 * @param dataType Data type
	 * @return boolean
	 */
	public boolean isNVarchar(String dataType) {
		return checkType(NVARCHAR_TYPES, dataType);
	}

	/**
	 * Retrieves the data type is string type
	 * 
	 * @param dataType String
	 * @return true if string (char or varchar)
	 */
	public boolean isString(String dataType) {
		return (isVarchar(dataType) || isChar(dataType));
	}

	/**
	 * whether auto increment of a CUBRID column can be edited TODO: MYSQL,MSSQL
	 * 
	 * @param dataType CUBRID column data type
	 * @param defaultValue CUBRID column default value
	 * @param scale CUBRID column scale
	 * @return boolean
	 */
	public boolean isSupportAutoIncr(String dataType, String defaultValue,
			Integer scale) {
		return checkType(AUTOINC_TYPES, dataType);
	}

	/**
	 * isValidDatatype TODO: MYSQL,MSSQL,Oracle
	 * 
	 * @param dataTypeInstance String
	 * @return boolean
	 */
	public boolean isValidDatatype(String dataTypeInstance) {
		return true;
	}

	/**
	 * Retrieves the value of data type is valid or not. Provide a simple
	 * validation. TODO: MYSQL,MSSQL,Oracle
	 * 
	 * @param dataType CUBRID data type string
	 * @param value default value
	 * @return true if value is valid
	 */
	public boolean isValidValue(String dataType, String value) {
		return true;
	}

	/**
	 * Retrieves the data type is VARCHAR type
	 * 
	 * @param dataType String
	 * @return true if VARCHAR
	 */
	public boolean isVarchar(String dataType) {
		return checkType(VARCHAR_TYPES, dataType);
	}

	/**
	 * Structure the input data type string. Be careful, the input string's
	 * format should match one of follows:
	 * varchar/varchar(2000)/numeric(38,2)/set(int)
	 * /set(varchar(200))/set(numeric(38.2)); The formats like
	 * "numeric(38.2) unsigned" should be turned into "numeric unsigned(38,2)"
	 * firstly.
	 * 
	 * @param fullDataType full data type name
	 * @return DataTypeInstance
	 */
	public DataTypeInstance parseDTInstance(String fullDataType) {
		if (StringUtils.isEmpty(fullDataType)) {
			throw new IllegalArgumentException("Data type can't be empty.");
		}
		DataTypeInstance result = new DataTypeInstance();
		String tmpType = fullDataType.trim();
		String pattern0 = "(.+?)\\((.+)\\)";
		Pattern pat0 = Pattern.compile(pattern0);
		Matcher matcher = pat0.matcher(tmpType);
		if (!matcher.matches()) {
			result.setName(tmpType);
			return result;
		}
		result.setName(matcher.group(1));

		String inner = matcher.group(2);
		//If it is ENUM type
		if (isEnum(result.getName())) {
			result.setElments(inner.trim());
			return result;
		}
		//If is collection type: set/multiset/sequence.
		if (isCollection(result.getName())) {
			DataTypeInstance subType = parseDTInstance(inner);
			if (subType != null) {
				result.setSubType(subType);
				return result;
			}
		}
		//Such as varchar(2000)
		if (inner.matches("\\s*(\\d+)\\s*")) {
			result.setPrecision(Integer.parseInt(inner.trim()));
			return result;
		}
		//Such as numeric(38,2)
		Pattern pat1 = Pattern.compile("\\s*(\\d+)\\s*,\\s*(\\d+)\\s*");
		Matcher matcher1 = pat1.matcher(inner);
		if (matcher1.matches()) {
			result.setPrecision(Integer.parseInt(matcher1.group(1)));
			result.setScale(Integer.parseInt(matcher1.group(2)));
			return result;
		}
		throw new IllegalArgumentException("Invalid data type:" + tmpType);
	}
}
