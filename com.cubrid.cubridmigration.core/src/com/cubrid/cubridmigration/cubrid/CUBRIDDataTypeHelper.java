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
package com.cubrid.cubridmigration.cubrid;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.datatype.DataTypeSymbol;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.cubrid.exception.UnSupportCUBRIDDataTypeException;

/**
 * CubridDataTypeHelper
 * 
 * @author Kevin.Wang Kevin Cao
 * @version 1.0 - 2011-11-29 created by Kevin.Wang
 */
public final class CUBRIDDataTypeHelper extends
		DBDataTypeHelper {
	public final static int DB_OBJ_NAME_MAX_LENGTH = 255;

	private static final Logger LOG = LogUtil.getLogger(CUBRIDDataTypeHelper.class);

	private static final String[] BIT_VALUE_PATTERN = new String[]{
			"(?i)b'([0|1])+'", "(?i)0b([0|1])+", "(?i)0x([[0-9]|[a-f]])+",
			"(?i)x'([[0-1]|[a-f]])+'" };

	private static final Map<Integer, DataTypeSymbol> CUBRID_DATATYPE_MAP = new HashMap<Integer, DataTypeSymbol>();
	private static final List<String> CUBRID_DATE_FUNCTIONS = new ArrayList<String>();
	private static final List<DataTypeSymbol> CUBRIDDATA_TYPES = new ArrayList<DataTypeSymbol>();
	private static final Map<String, DataTypeSymbol> DATATYPE_SYNONYMS_MAP = new HashMap<String, DataTypeSymbol>();
	private static final Map<Integer, Integer> TYPE_BYTES_MAP = new HashMap<Integer, Integer>();

	private static final CUBRIDDataTypeHelper HELPER = new CUBRIDDataTypeHelper();
	//init all cubrid datatype
	static {
		initTypeSpaceMap();
		initCUBRIDDateFunctions();
		initCUBRIDDataTypes();

		for (DataTypeSymbol symbol : CUBRIDDATA_TYPES) {
			//init DATATYPE_SYNONYMS_MAP
			for (String nickName : symbol.getNickNames()) {
				DATATYPE_SYNONYMS_MAP.put(nickName, symbol);
			}
		}

		for (DataTypeSymbol symbol : CUBRIDDATA_TYPES) {
			CUBRID_DATATYPE_MAP.put(symbol.getDataTypeID(), symbol);
		}
	}

	/**
	 * Singleton
	 * 
	 * @param version of oracle database
	 * @return DataTypeHelper
	 */
	public static CUBRIDDataTypeHelper getInstance(String version) {
		return HELPER;
	}

	/**
	 * initCUBRIDDataTypes
	 */
	private static void initCUBRIDDataTypes() {
		//small int
		DataTypeSymbol smallIntSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_SMALLINT, "short", "short");
		smallIntSymbol.getNickNames().add("smallint");
		smallIntSymbol.getNickNames().add("short");
		CUBRIDDATA_TYPES.add(smallIntSymbol);

		//int
		DataTypeSymbol intSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_INTEGER, "int", "int");
		intSymbol.getNickNames().add("integer");
		intSymbol.getNickNames().add("int");
		CUBRIDDATA_TYPES.add(intSymbol);

		//bigint
		DataTypeSymbol bigintSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_BIGINT, "bigint", "bigint");
		bigintSymbol.getNickNames().add("bigint");
		bigintSymbol.getNickNames().add("biginteger");
		CUBRIDDATA_TYPES.add(bigintSymbol);

		//numeric
		DataTypeSymbol numericSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_NUMERIC, "numeric", "numeric");
		numericSymbol.getNickNames().add("numeric");
		numericSymbol.getNickNames().add("decimal");
		numericSymbol.getNickNames().add("dec");
		CUBRIDDATA_TYPES.add(numericSymbol);

		//float
		DataTypeSymbol floatSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_FLOAT, "float", "float");
		floatSymbol.getNickNames().add("float");
		floatSymbol.getNickNames().add("real");
		CUBRIDDATA_TYPES.add(floatSymbol);

		//double
		DataTypeSymbol doubleSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_DOUBLE, "double", "double");
		doubleSymbol.getNickNames().add("double");
		doubleSymbol.getNickNames().add("double precision");
		CUBRIDDATA_TYPES.add(doubleSymbol);

		//monetary
		DataTypeSymbol monetarySymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_MONETARY, "monetary", "monetary");
		monetarySymbol.getNickNames().add("monetary");
		CUBRIDDATA_TYPES.add(monetarySymbol);

		//char
		DataTypeSymbol charSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_CHAR, "char", "char");
		charSymbol.getNickNames().add("char");
		charSymbol.getNickNames().add("character");
		charSymbol.getNickNames().add("nchar");
		charSymbol.getNickNames().add("national character");
		CUBRIDDATA_TYPES.add(charSymbol);

		//varchar
		DataTypeSymbol varcharSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_VARCHAR, "varchar", "varchar");
		varcharSymbol.getNickNames().add("varchar");
		varcharSymbol.getNickNames().add("char varying");
		varcharSymbol.getNickNames().add("character varying");
		varcharSymbol.getNickNames().add("nvarchar");
		varcharSymbol.getNickNames().add("varnchar");
		varcharSymbol.getNickNames().add("nchar varying");
		varcharSymbol.getNickNames().add("national character varying");
		CUBRIDDATA_TYPES.add(varcharSymbol);

		//nvarchar
		//		DataTypeSymbol ncharSymbol = new DataTypeSymbol(
		//				DataTypeConstant.CUBRID_DT_NCHAR, "nchar", "nchar");
		//		ncharSymbol.getNickNames().add("nchar");
		//		ncharSymbol.getNickNames().add("national character");
		//		CUBRIDDATA_TYPES.add(ncharSymbol);

		//nvarchar
		//		DataTypeSymbol nvarcharSymbol = new DataTypeSymbol(
		//				DataTypeConstant.CUBRID_DT_NVARCHAR, "varnchar", "varchar");
		//		nvarcharSymbol.getNickNames().add("nvarchar");
		//		nvarcharSymbol.getNickNames().add("varnchar");
		//		nvarcharSymbol.getNickNames().add("national character varying");
		//		CUBRIDDATA_TYPES.add(nvarcharSymbol);

		//time
		DataTypeSymbol timeSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_TIME, "time", "time");
		timeSymbol.getNickNames().add("time");
		CUBRIDDATA_TYPES.add(timeSymbol);

		//date
		DataTypeSymbol dateSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_DATE, "date", "date");
		dateSymbol.getNickNames().add("date");
		CUBRIDDATA_TYPES.add(dateSymbol);

		//timestamp
		DataTypeSymbol timestampSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_TIMESTAMP, "timestamp", "timestamp");
		timestampSymbol.getNickNames().add("timestamp");
		CUBRIDDATA_TYPES.add(timestampSymbol);

		//datetime
		DataTypeSymbol datetimeSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_DATETIME, "datetime", "datetime");
		datetimeSymbol.getNickNames().add("datetime");
		CUBRIDDATA_TYPES.add(datetimeSymbol);

		//bit
		DataTypeSymbol bitSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_BIT, "bit", "bit");
		bitSymbol.getNickNames().add("bit");
		CUBRIDDATA_TYPES.add(bitSymbol);

		//varbit
		DataTypeSymbol varbitSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_VARBIT, "bit varying", "bit varying");
		varbitSymbol.getNickNames().add("bit varying");
		varbitSymbol.getNickNames().add("varbit");
		CUBRIDDATA_TYPES.add(varbitSymbol);

		//set
		DataTypeSymbol setSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_SET, "set", "set");
		setSymbol.getNickNames().add("set_of");
		setSymbol.getNickNames().add("set");
		CUBRIDDATA_TYPES.add(setSymbol);

		//multiset
		DataTypeSymbol multisetSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_MULTISET, "multiset", "multiset");
		multisetSymbol.getNickNames().add("multiset_of");
		multisetSymbol.getNickNames().add("multiset");
		CUBRIDDATA_TYPES.add(multisetSymbol);

		//sequence
		DataTypeSymbol sequenceSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_SEQUENCE, "list", "list");
		sequenceSymbol.getNickNames().add("sequence_of");
		sequenceSymbol.getNickNames().add("sequence");
		sequenceSymbol.getNickNames().add("list");
		sequenceSymbol.getNickNames().add("list_of");
		CUBRIDDATA_TYPES.add(sequenceSymbol);

		//glo
		DataTypeSymbol gloSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_GLO, "glo", "glo");
		gloSymbol.getNickNames().add("glo");
		CUBRIDDATA_TYPES.add(gloSymbol);

		//fbo
		//		DataTypeSymbol fboSymbol = new DataTypeSymbol(
		//				DataTypeConstant.CUBRID_DT_FBO, "fbo", "fbo");
		//		fboSymbol.getNickNames().add("fbo");
		//		CUBRIDDATA_TYPES.add(fboSymbol);

		//object
		DataTypeSymbol objectSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_OBJECT, "object", "object");
		objectSymbol.getNickNames().add("object");
		CUBRIDDATA_TYPES.add(objectSymbol);

		//clob
		DataTypeSymbol clobSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_CLOB, "clob", "clob");
		clobSymbol.getNickNames().add("clob");
		CUBRIDDATA_TYPES.add(clobSymbol);

		//blob
		DataTypeSymbol blobSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_BLOB, "blob", "blob");
		blobSymbol.getNickNames().add("blob");
		CUBRIDDATA_TYPES.add(blobSymbol);

		//enum
		DataTypeSymbol enumSymbol = new DataTypeSymbol(
				DataTypeConstant.CUBRID_DT_ENUM, "enum", "enum");
		enumSymbol.getNickNames().add("enum");
		CUBRIDDATA_TYPES.add(enumSymbol);
	}

	/**
	 * initCUBRIDDateFunctions
	 */
	private static void initCUBRIDDateFunctions() {
		CUBRID_DATE_FUNCTIONS.add("CURRENT_TIMESTAMP");
		CUBRID_DATE_FUNCTIONS.add("SYS_TIMESTAMP");
		CUBRID_DATE_FUNCTIONS.add("SYSTIMESTAMP");
		CUBRID_DATE_FUNCTIONS.add("SYS_DATETIME");
		CUBRID_DATE_FUNCTIONS.add("SYSDATETIME");
		CUBRID_DATE_FUNCTIONS.add("CURRENT_DATETIME");
		CUBRID_DATE_FUNCTIONS.add("CURRENT_DATETIME()");
		CUBRID_DATE_FUNCTIONS.add("NOW()");
	}

	/**
	 * initTypeSpaceMap
	 */
	private static void initTypeSpaceMap() {
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_BIGINT, 8);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_DOUBLE, 8);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_FLOAT, 4);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_INTEGER, 4);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_MONETARY, 12);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_NUMERIC, 16);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_SMALLINT, 2);

		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_DATE, 4);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_TIME, 4);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_TIMESTAMP, 4);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_DATETIME, 8);

		//Ignore the LOB types
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_FBO, 0);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_GLO, 0);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_BLOB, 0);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_CLOB, 0);

		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_BIT, -1);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_VARBIT, -1);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_CHAR, -1);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_VARCHAR, -1);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_NCHAR, -1);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_NVARCHAR, -1);

		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_ENUM, 4);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_OBJECT, 256);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_SEQUENCE, 256);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_SET, 256);
		TYPE_BYTES_MAP.put(DataTypeConstant.CUBRID_DT_MULTISET, 256);
	}

	private CUBRIDDataTypeHelper() {
		//Do nothing here.
	}

	/**
	 * Retrieves the CUBRID data type ID. If data type is collection, only
	 * retrieves the main data type ID.
	 * 
	 * @param dataType String
	 * @return String
	 */
	public int getCUBRIDDataTypeID(String dataType) {
		final String mainDataType = getMainDataType(dataType);
		String lowCaseDataType = mainDataType.toLowerCase(Locale.ENGLISH);
		final DataTypeSymbol dataTypeSymbol = DATATYPE_SYNONYMS_MAP.get(lowCaseDataType);
		if (dataTypeSymbol != null) {
			return dataTypeSymbol.getDataTypeID();
		}
		throw new UnSupportCUBRIDDataTypeException(
				"Unsupported CUBRID data type:" + dataType);
	}

	/**
	 * get CUBRID data type
	 * 
	 * @return CUBRIDDataType[]
	 */
	public DataTypeSymbol[] getCUBRIDDataTypes() {
		DataTypeSymbol[] datatypes = new DataTypeSymbol[CUBRIDDATA_TYPES.size()];
		for (int i = 0; i < CUBRIDDATA_TYPES.size(); i++) {
			try {
				datatypes[i] = CUBRIDDATA_TYPES.get(i).clone();
			} catch (CloneNotSupportedException e) {
				LOG.error(e.getMessage());
			}
		}
		return datatypes;
	}

	/**
	 * Retrieves the bytes used of the data type.
	 * 
	 * @param col Column
	 * @return byte length
	 */
	public long getDataTypeByteSize(Column col) {
		Integer len = TYPE_BYTES_MAP.get(col.getJdbcIDOfDataType());
		len = len == null ? 0 : len;
		if (len < 0) {
			len = col.getPrecision();
		}
		return len;
	}

	/**
	 * return CUBRID data type ID
	 * 
	 * @param dataType String
	 * @return String
	 */
	public DataTypeSymbol getDataTypeSymbol(String dataType) {
		final String strType2Varchar = strType2Varchar(dataType);
		final String mainDataType = getMainDataType(strType2Varchar);
		String lowCaseDataType = mainDataType.toLowerCase(Locale.ENGLISH);
		final DataTypeSymbol dataTypeSymbol = DATATYPE_SYNONYMS_MAP.get(lowCaseDataType);
		if (dataTypeSymbol != null) {
			return dataTypeSymbol;
		}
		throw new UnSupportCUBRIDDataTypeException(
				"Unsupported CUBRID data type:" + dataType);
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
	 * return data type of a column in DDL
	 * 
	 * @param column Column
	 * @return String
	 */
	public String getDDLDataType(Column column) {
		return innerGetDataType(column, false);
	}

	/**
	 * return inner CUBRID data type of a data type instance
	 * 
	 * @param dataTypeInstance String
	 * @return String
	 */
	public String getInnerDataType(String dataTypeInstance) {
		String tmpDataType = strType2Varchar(dataTypeInstance);
		String dataType = getMainDataType(tmpDataType);
		Integer cubridDataTypeID = getCUBRIDDataTypeID(dataType);
		return CUBRID_DATATYPE_MAP.get(cubridDataTypeID).getInnerDataType();
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
		return getCUBRIDDataTypeID(dataType);
	}

	/**
	 * return precision of a data type
	 * 
	 * @param dataTypeInstance String
	 * @return Integer
	 * @throws NumberFormatException e
	 */
	public Integer getPrecision(String dataTypeInstance) throws NumberFormatException {
		String lowerCaseDataTypeInstance = strType2Varchar(dataTypeInstance);
		String outterDatatype = getMainDataType(lowerCaseDataTypeInstance);
		if (isCollection(outterDatatype)) {
			String subDataTypeStr = getRemain(lowerCaseDataTypeInstance);
			return getPrecision(subDataTypeStr);
		}
		String typeRemain = getRemain(lowerCaseDataTypeInstance);
		if (null == typeRemain) {
			return null;
		}
		int index = typeRemain.indexOf(',');
		if (index == -1) {
			return Integer.parseInt(typeRemain);
		}
		return Integer.parseInt(typeRemain.substring(0, index));
	}

	/**
	 * return the remained part of a special data type,eg: <li>character(10),
	 * return "10" <li>set(integer), return "integer" <li>integer, return null
	 * 
	 * @see #getMainDataType(String)
	 * 
	 * @param dataTypeInstance String
	 * @return String
	 */
	public String getRemain(String dataTypeInstance) {
		return getRemain(dataTypeInstance, false);
	}

	/**
	 * return the remained part of a special data type,eg: <li>character(10),
	 * return "10" <li>set(integer), return "integer" <li>integer, return null
	 * 
	 * @see #getMainDataType(String)
	 * 
	 * @param dataType String
	 * @param keepCase boolean true : don't change character case in order to
	 *        keep ENUM's elements
	 * @return String
	 */
	private String getRemain(String dataType, boolean keepCase) {
		if (dataType == null) {
			return null;
		}
		String tempType = strType2Varchar(dataType);

		int index = tempType.indexOf('(');

		if (-1 == index) { // the simplest case
			return null;
		} // the case like: set_of(bit) ,numeric(4,2)
		try {
			tempType = tempType.substring(index + 1, dataType.length() - 1);
			if (keepCase) {
				return tempType;
			}
			return tempType.toLowerCase(Locale.US);
		} catch (Exception ex) {
			LOG.debug("Can't get the getRemain:" + dataType + " with error "
					+ ex.getMessage());
		}
		return null;
	}

	/**
	 * return scale of a data type
	 * 
	 * @param dataType String
	 * @return Integer
	 * @throws NumberFormatException e
	 */
	public Integer getScale(String dataType) throws NumberFormatException {
		String lowerCasedDT = strType2Varchar(dataType);

		String outterDatatype = getMainDataType(lowerCasedDT);

		if (isCollection(outterDatatype)) {
			String subDataTypeStr = getRemain(lowerCasedDT);
			return getScale(subDataTypeStr);
		}
		String typeRemain = getRemain(lowerCasedDT);
		if (null == typeRemain) {
			return null;
		}
		int index = typeRemain.indexOf(',');

		if (index == -1) {
			return null;
		}
		return Integer.parseInt(typeRemain.substring(index + 1,
				typeRemain.length()));
	}

	/**
	 * return shown data type instance of a column
	 * 
	 * @param column Column
	 * @return String
	 */
	public String getShownDataType(Column column) {
		return innerGetDataType(column, true);
	}

	/**
	 * Retrieves the standard main name of the data type.
	 * 
	 * @param dataType if input is char varying(200)
	 * @return the result is varchar.
	 */
	public String getStdMainDataType(String dataType) {
		DataTypeSymbol dts = getDataTypeSymbol(dataType);
		return dts.getShownDataType();
	}

	/**
	 * return data type instance of a column
	 * 
	 * @param column Column
	 * @param isShownDataType boolean
	 * @return String
	 */
	private String innerGetDataType(Column column, boolean isShownDataType) {
		Integer dataTypeID = column.getJdbcIDOfDataType();
		Integer elemTypeID = column.getJdbcIDOfSubDataType();
		Integer precision = column.getPrecision();
		Integer scale = column.getScale();
		DataTypeSymbol dataTypeSymbol = CUBRID_DATATYPE_MAP.get(dataTypeID);

		if (isCollection(column.getDataType())) {
			String dataType = isShownDataType ? dataTypeSymbol.getShownDataType()
					: dataTypeSymbol.getInnerDataType();

			final String inType = innerGetShownDataTypeType(elemTypeID,
					precision, scale, isShownDataType);
			return StringUtils.isEmpty(inType) ? dataType : (dataType + "("
					+ inType + ")");
		}
		if (isEnum(column.getDataType())) {
			String dataType = isShownDataType ? dataTypeSymbol.getShownDataType()
					: dataTypeSymbol.getInnerDataType();
			String elements = getRemain(column.getShownDataType(), true);
			return dataType + "(" + elements + ")";
		}
		return innerGetShownDataTypeType(dataTypeID, precision, scale,
				isShownDataType);
	}

	/**
	 * return the shown data type instance
	 * 
	 * @param dataTypeID Integer
	 * @param vprecision valid digital number
	 * @param scale float digital number
	 * @param isShownDataType boolean
	 * @return String
	 */
	private String innerGetShownDataTypeType(Integer dataTypeID,
			Integer vprecision, Integer scale, boolean isShownDataType) {
		DataTypeSymbol dataTypeSymbol = CUBRID_DATATYPE_MAP.get(dataTypeID);
		if (dataTypeSymbol == null) {
			return "";
		}
		String dataType = isShownDataType ? dataTypeSymbol.getShownDataType()
				: dataTypeSymbol.getInnerDataType();

		Integer precision = vprecision;
		if (dataTypeID == DataTypeConstant.CUBRID_DT_NUMERIC) {
			//add for the bug 528
			Integer finalScale = scale;
			if (null == precision || 0 == precision) {
				if (null == finalScale) {
					finalScale = 8;
				}
				precision = DataTypeConstant.NUMERIC_MAX_PRECISIE_SIZE;
			}
			return dataType + "(" + precision + "," + finalScale + ")";
		}
		if (dataTypeID == DataTypeConstant.CUBRID_DT_CHAR
				|| dataTypeID == DataTypeConstant.CUBRID_DT_VARCHAR
				|| dataTypeID == DataTypeConstant.CUBRID_DT_BIT
				|| dataTypeID == DataTypeConstant.CUBRID_DT_VARBIT
				|| dataTypeID == DataTypeConstant.CUBRID_DT_NCHAR
				|| dataTypeID == DataTypeConstant.CUBRID_DT_NVARCHAR) {
			return dataType + "(" + precision + ")";
		}
		return dataType;
		// else if (dataTypeID == DataTypeConstant.CUBRID_DT_VARCHAR
		//				&& precision == DataTypeConstant.CUBRID_MAXSIZE) {
		//			return isShownDataType ? "string" : "varchar(1073741823)";
		//		}
	}

	/**
	 * return whether a CUBRID data type instance is valid
	 * 
	 * @param dataTypeInstance a data type instance not including
	 *        set/multiset/sequence types
	 * @return boolean
	 */
	private boolean innerVerifyDataType(String dataTypeInstance) {
		if (dataTypeInstance == null) {
			return false;
		}

		if (dataTypeInstance.indexOf('(') != -1
				&& dataTypeInstance.indexOf(')') == -1
				|| dataTypeInstance.indexOf('(') == -1
				&& dataTypeInstance.indexOf(')') != -1) {
			return false;
		}

		try {
			String datatype = getMainDataType(dataTypeInstance);
			if ("enum".equals(datatype)) {
				return true;
			}

			Integer precision = getPrecision(dataTypeInstance);
			Integer scale = getScale(dataTypeInstance);

			if (!isCUBRIDSupportedDataType(datatype)) {
				return false;
			}

			Integer dataTypeID = getCUBRIDDataTypeID(datatype);
			if (dataTypeID == DataTypeConstant.CUBRID_DT_NUMERIC) {
				if (precision != null && scale != null && precision <= 38
						&& scale >= 0 && precision >= scale) {
					return true;
				}
			} else if (dataTypeID == DataTypeConstant.CUBRID_DT_CHAR
					|| dataTypeID == DataTypeConstant.CUBRID_DT_VARCHAR
					|| dataTypeID == DataTypeConstant.CUBRID_DT_BIT
					|| dataTypeID == DataTypeConstant.CUBRID_DT_VARBIT) {
				if (precision != null && scale == null
						&& precision <= 1073741823) {
					return true;
				}
			} else if (dataTypeID == DataTypeConstant.CUBRID_DT_NCHAR
					|| dataTypeID == DataTypeConstant.CUBRID_DT_NVARCHAR) {
				if (precision != null && scale == null
						&& precision <= 1073741823 / 2) {
					return true;
				}
			} else {
				if (precision == null && scale == null) {
					return true;
				}
			}

			return false;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * return whether column is binary data type
	 * 
	 * @param dataType Column
	 * @return boolean
	 */
	public boolean isBinary(String dataType) {
		return checkType(BINARY_TYPES, dataType);
	}

	/**
	 * return whether a data type is set type
	 * 
	 * @param dataType String
	 * @return boolean
	 */
	public boolean isCollection(String dataType) {
		return checkType(
				"/set/set_of/multiset_of/multiset/sequence_of/sequence/list/",
				dataType);
	}

	//	/**
	//	 * return whether data type is char data type
	//	 * 
	//	 * @param dataType String
	//	 * @return boolean
	//	 */
	//	public boolean isChar(String dataType) {
	//		if (StringUtils.isEmpty(dataType)) {
	//			return false;
	//		}
	//		String lower = dataType.trim().toLowerCase(Locale.US);
	//		return lower.startsWith("varchar") || lower.startsWith("char");
	//		//				|| lower.startsWith("character varying")
	//		//				|| lower.startsWith("character")
	//		//				|| lower.startsWith("char varying");
	//	}

	/**
	 * return whether data type is CUBRID supported data type
	 * 
	 * @param dataType String
	 * @return boolean
	 */
	private boolean isCUBRIDSupportedDataType(String dataType) {

		if (DATATYPE_SYNONYMS_MAP.containsKey(dataType.toLowerCase(Locale.ENGLISH))) {
			return true;
		}

		for (DataTypeSymbol item : CUBRIDDATA_TYPES) {
			if (dataType.equalsIgnoreCase(item.getInnerDataType())) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Retrieves if the data type is object type.
	 * 
	 * @param dataType dataType
	 * @return true if object type
	 */
	public boolean isObjectType(String dataType) {
		return "OBJECT".equalsIgnoreCase(dataType);
	}

	/**
	 * The data type is "numeric/decimal/dec" type
	 * 
	 * @param dataType String
	 * @return true if it is numeric.
	 */
	public boolean isStrictNumeric(String dataType) {
		if (StringUtils.isEmpty(dataType)) {
			return false;
		}
		return checkType("/numeric/decimal/dec/monetary/", dataType);
	}

	/**
	 * whether auto increment of a CUBRID column can be edited
	 * 
	 * @param dataType CUBRID column data type
	 * @param defaultValue CUBRID column default value
	 * @param scale CUBRID column scale
	 * @return boolean
	 */
	public boolean isSupportAutoIncr(String dataType, String defaultValue,
			Integer scale) {
		final boolean isEmptyDef = StringUtils.isEmpty(defaultValue);
		if (!isEmptyDef) {
			return false;
		}
		if (super.isSupportAutoIncr(dataType, defaultValue, scale)) {
			return true;
		}
		return (dataType.equalsIgnoreCase("numeric") && scale != null && scale == 0);
	}

	/**
	 * isValidDatatype
	 * 
	 * @param dataType String
	 * @return boolean
	 */
	public boolean isValidDatatype(String dataType) {
		if (dataType == null) {
			return false;
		}
		String temp = strType2Varchar(dataType).toLowerCase(Locale.US);

		String outDatatype = getMainDataType(temp);
		if (isCollection(outDatatype)) {
			temp = getRemain(temp);
			temp = strType2Varchar(temp).toLowerCase(Locale.US);
		}
		return innerVerifyDataType(temp);
	}

	/**
	 * Retrieves the value of data type is valid or not. Provide a simple
	 * validation.
	 * 
	 * @param dataType CUBRID data type string
	 * @param value default value
	 * @return true if value is valid
	 */
	public boolean isValidValue(String dataType, String value) {
		if (StringUtils.isEmpty(value)) {
			return true;
		}
		String dt = dataType.trim().toLowerCase(Locale.US);
		String mindt = getMainDataType(dt);
		if (isString(mindt)) {
			String spre = dt.substring(dt.indexOf("(") + 1, dt.length() - 1);
			int pre = Integer.parseInt(spre);
			return value.length() <= pre;
		} else if (isInteger(mindt)) {
			try {
				Integer.parseInt(value);
			} catch (Exception ex) {
				return false;
			}
		} else if (dt.startsWith("long")) {
			try {
				Long.parseLong(value);
			} catch (Exception ex) {
				return false;
			}
		} else if (dt.startsWith("bigint")) {
			try {
				BigInteger bi = new BigInteger(value);
				return bi != null;
			} catch (Exception ex) {
				return false;
			}
		} else if (isGeneralizedNumeric(mindt)) {
			return StringUtils.isNumeric(value);
		} else if (dt.equals("date")) {
			try {
				new SimpleDateFormat("yyyy-MM-dd").parse(value);
			} catch (Exception ex) {
				return false;
			}
		} else if (dt.equals("time")) {
			try {
				new SimpleDateFormat("HH:mm:ss").parse(value);
			} catch (Exception ex) {
				return false;
			}
		} else if (dt.equals("datetime") || dt.equals("timestamp")) {
			String upper = value.toUpperCase(Locale.US);
			if (CUBRID_DATE_FUNCTIONS.contains(upper)) {
				return true;
			}
			try {
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value);
			} catch (Exception ex) {
				return false;
			}
		} else if (dt.startsWith("bit")) {
			for (String pat : BIT_VALUE_PATTERN) {
				if (value.matches(pat)) {
					return true;
				}
			}
		}
		return true;
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
		String tmp = strType2Varchar(fullDataType);
		DataTypeInstance result = super.parseDTInstance(tmp);
		return result;
	}

	/**
	 * Set a column data type
	 * 
	 * @param showDataType String
	 * @param col Column
	 */
	public void setColumnDataType(String showDataType, Column col) {
		String dtString = strType2Varchar(showDataType);
		DataTypeInstance dti = parseDTInstance(dtString);
		standardDataTypeInstance(dti);
		col.setDataTypeInstance(dti);
		col.setJdbcIDOfDataType(getCUBRIDDataTypeID(dti.getName()));
		final DataTypeInstance subType = dti.getSubType();
		if (subType != null) {
			col.setJdbcIDOfSubDataType(getCUBRIDDataTypeID(subType.getName()));
		}
	}

	/**
	 * Change the data type name into standard name.
	 * 
	 * @param dti has been changed.
	 */
	private void standardDataTypeInstance(DataTypeInstance dti) {
		//Change standard name
		dti.setName(getStdMainDataType(dti.getName()));
		final DataTypeInstance subType = dti.getSubType();
		if (subType != null) {
			//Change to standard name.
			subType.setName(getStdMainDataType(subType.getName()));
		}
	}

	/**
	 * "String" should be changed into varchar(1073741823)
	 * 
	 * @param dataType String
	 * @return if input is "string", varchar(1073741823) will be returned.
	 */
	private String strType2Varchar(String dataType) {
		return DataTypeConstant.CUBRID_STRING.equalsIgnoreCase(dataType) ? DataTypeConstant.CUBRID_MAX_VARCHAR
				: dataType;
	}
}
