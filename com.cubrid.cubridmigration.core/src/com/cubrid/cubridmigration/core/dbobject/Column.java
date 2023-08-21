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
package com.cubrid.cubridmigration.core.dbobject;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.dbtype.DBConstant;

/**
 * 
 * to store information of a column in the table
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-9 created by moulinwang
 */
public class Column extends
		DBObject {

	private static final long serialVersionUID = 3355892280452788215L;
	private String name;
	private Integer jdbcIDOfDataType;

	private String dataType;
	private Integer precision;
	private Integer scale;

	private boolean nullable = true;
	private boolean unique;
	private boolean shared = false;
	private String sharedValue;
	private String defaultValue;
	private TableOrView tableOrView;

	//The maximum length of the column in bytes
	private int byteLength;

	//The maximum length of the column in characters
	private int charLength;

	private boolean isAutoIncrement;
	private long autoIncSeedVal;
	private long autoIncIncrVal = 1;
	//private long autoIncMaxVal;
	/**
	 * CUBRID column supports element data type
	 */
	protected String subDataType = null;
	protected Integer jdbcIDOfSubDataType;

	private boolean defaultIsExpression = false;
	/**
	 * MySQL column supports column char set
	 */
	private String charset = null;

	private String shownDataType = "";

	/**
	 * Oracle column support "CHAR_USED" attribute of table "USER_TAB_COLUMNS"
	 */
	private String charUsed = null;

	/**
	 * Element string of CUBRID, MySQL enum type such as 'Y','N'...
	 */
	private String enumElements = null;

	
	private String graphDataType = "";
	private boolean isSelected = true;
	private boolean supportGraphDatatype = true;
	
	public Column() {
		//do nothing
	}
	
	public Column(String name) {
		this.setName(name);
		this.setGraphDataType("ID");
	}

	public Column(Table table) {
		this.tableOrView = table;
	}

	/**
	 * clone oracle column to CUBRID column
	 * 
	 * @return Column
	 */
	public Column cloneCol() {

		Column cubridColumn = new Column();
		cubridColumn.setName(getName());

		cubridColumn.setShared(isShared());
		cubridColumn.setSharedValue(getSharedValue());
		cubridColumn.setDataType(getDataType());
		cubridColumn.setShownDataType(getShownDataType());
		cubridColumn.setJdbcIDOfDataType(getJdbcIDOfDataType());
		cubridColumn.setSubDataType(getSubDataType());
		cubridColumn.setJdbcIDOfSubDataType(getJdbcIDOfSubDataType());
		cubridColumn.setPrecision(getPrecision());
		cubridColumn.setScale(getScale());

		cubridColumn.setDefaultValue(getDefaultValue());
		cubridColumn.setAutoIncrement(isAutoIncrement());
		cubridColumn.setAutoIncSeedVal(getAutoIncSeedVal());
		cubridColumn.setByteLength(getByteLength());
		cubridColumn.setCharLength(getCharLength());
		cubridColumn.setNullable(isNullable());
		cubridColumn.setUnique(isUnique());
		return cubridColumn;
	}

	public String getCharUsed() {
		return charUsed;
	}

	public long getAutoIncIncrVal() {
		return autoIncIncrVal;
	}

	//	public long getAutoIncMaxVal() {
	//		return autoIncMaxVal;
	//	}

	public long getAutoIncSeedVal() {
		return autoIncSeedVal;
	}

	public int getByteLength() {
		return byteLength;
	}

	public int getCharLength() {
		return charLength;
	}

	public String getCharset() {
		return charset;
	}

	public String getDataType() {
		return dataType;
	}

	/**
	 * DDL
	 * 
	 * @return DDL
	 */
	public String getDDL() {
		return null;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public Integer getJdbcIDOfDataType() {
		return jdbcIDOfDataType;
	}

	public Integer getJdbcIDOfSubDataType() {
		return jdbcIDOfSubDataType;
	}

	public String getName() {
		return name;
	}

	/**
	 * @return column
	 */
	public String getObjType() {
		return OBJ_TYPE_COLUMN;
	}

	public Integer getPrecision() {
		return precision == null ? 0 : precision;
	}

	public Integer getScale() {
		return scale == null ? 0 : scale;
	}

	public String getSharedValue() {
		return sharedValue;
	}

	public String getShownDataType() {
		return shownDataType;
	}

	public String getSubDataType() {
		return subDataType;
	}

	public TableOrView getTableOrView() {
		return tableOrView;
	}

	public boolean isAutoIncrement() {
		return isAutoIncrement;
	}

	public boolean isDefaultIsExpression() {
		return defaultIsExpression;
	}

	public boolean isNullable() {
		return nullable;
	}

	public boolean isShared() {
		return shared;
	}

	public boolean isUnique() {
		return unique;
	}

	public void setCharUsed(String addtionalInfo) {
		this.charUsed = addtionalInfo;
	}

	public void setAutoIncIncrVal(long autoIncIncrVal) {
		this.autoIncIncrVal = autoIncIncrVal;
	}

	//	public void setAutoIncMaxVal(long autoIncMaxVal) {
	//		this.autoIncMaxVal = autoIncMaxVal;
	//	}

	public void setAutoIncrement(boolean isAutoIncrement) {
		this.isAutoIncrement = isAutoIncrement;
	}

	public void setAutoIncSeedVal(long autoIncSeedVal) {
		this.autoIncSeedVal = autoIncSeedVal;
	}

	public void setByteLength(int length) {
		this.byteLength = length;
	}

	public void setCharLength(int charLength) {
		this.charLength = charLength;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public void setDefaultIsExpression(boolean defaultIsExpression) {
		this.defaultIsExpression = defaultIsExpression;
	}

	/**
	 * 
	 * @param defaultValue if value == 'NULL', null will be set.
	 */
	public void setDefaultValue(String defaultValue) {
		if (DBConstant.DB_NULL_VALUE.equalsIgnoreCase(defaultValue)) {
			this.defaultValue = null;
		} else {
			this.defaultValue = defaultValue;
		}
	}

	/**
	 * If default value is null, the NULL string will be retrieved.
	 * 
	 * @return If default value is null, the NULL string will be retrieved.
	 */
	public String getDefaultValueDisplayString() {
		if (this.defaultValue == null) {
			return DBConstant.DB_NULL_VALUE;
		}
		return this.defaultValue;
	}

	public void setJdbcIDOfDataType(Integer jdbcIDOfDataType) {
		this.jdbcIDOfDataType = jdbcIDOfDataType;
	}

	public void setJdbcIDOfSubDataType(Integer jdbcIDOfSubDataType) {
		this.jdbcIDOfSubDataType = jdbcIDOfSubDataType;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public void setPrecision(Integer precision) {
		this.precision = precision;
	}

	public void setScale(Integer scale) {
		this.scale = scale;
	}

	public void setShared(boolean shared) {
		this.shared = shared;
	}

	public void setSharedValue(String sharedValue) {
		this.sharedValue = sharedValue;
	}

	public void setShownDataType(String shownDataType) {
		this.shownDataType = shownDataType == null ? "" : shownDataType;
	}

	public void setSubDataType(final String subDataType) {
		this.subDataType = subDataType;
	}

	public void setTableOrView(TableOrView tableOrView) {
		this.tableOrView = tableOrView;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	public String getEnumElements() {
		return enumElements;
	}

	public void setEnumElements(String enumElements) {
		this.enumElements = enumElements;
	}

	/**
	 * DataTypeInstance. be carefully, the data type id was not set in this
	 * method.
	 * 
	 * @param dti DataTypeInstance
	 */
	public void setDataTypeInstance(DataTypeInstance dti) {
		this.shownDataType = dti.getShownDataType();
		this.dataType = dti.getName();
		this.precision = dti.getPrecision();
		this.scale = dti.getScale();
		this.enumElements = dti.getElments();
		DataTypeInstance sdti = dti.getSubType();
		if (sdti == null) {
			this.subDataType = null;
			return;
		}
		this.subDataType = sdti.getName();
		this.precision = sdti.getPrecision();
		this.scale = sdti.getScale();
	}

	/**
	 * Create a DataTypeInstance of column's data type.
	 * 
	 * @return DataTypeInstance
	 */
	public DataTypeInstance getDataTypeInstance() {
		DataTypeInstance result = new DataTypeInstance();
		result.setName(this.dataType);
		if (StringUtils.isNotBlank(subDataType)) {
			DataTypeInstance sub = new DataTypeInstance();
			sub.setName(subDataType);
			sub.setPrecision(precision);
			sub.setScale(scale);
			sub.setElments(enumElements);
			result.setSubType(sub);
		} else {
			result.setPrecision(precision);
			result.setScale(scale);
			result.setElments(enumElements);
		}
		return result;
	}
	
	public void setGraphDataType(String type) {
	    this.graphDataType = type;
	}
	
	public String getGraphDataType() {
        return this.graphDataType;
    }
	
	public void setSupportGraphDataType(boolean support) {
	    this.supportGraphDatatype = support;
	}
	
	public boolean getSupportGraphDataType() {
        return this.supportGraphDatatype;
    }
	
	public void setSelected(boolean isSelected) {
		this.isSelected = isSelected;
	}
	
	public boolean isSelected() {
		return isSelected;
	}
}
