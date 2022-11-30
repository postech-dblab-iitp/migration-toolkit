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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 
 * FK
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-15 created by moulinwang
 */
public class FK extends
		DBObject {

	private static final long serialVersionUID = -7515442469234740812L;
	public final static int ON_UPDATE_CASCADE = 0;
	public final static int ON_UPDATE_RESTRICT = 1;
	public final static int ON_UPDATE_SET_NULL = 2;
	public final static int ON_UPDATE_NO_ACTION = 3;

	public final static int ON_DELETE_CASCADE = 0;
	public final static int ON_DELETE_RESTRICT = 1;
	public final static int ON_DELETE_SET_NULL = 2;
	public final static int ON_DELETE_NO_ACTION = 3;

	private Table table;

	private String name;
	private String ddl;

	//private int deferability;
	private int deleteRule;
	private int updateRule;

	private String referencedTableName;
	//private String onCacheObject;

	private final Map<String, String> col2RefMapping = new TreeMap<String, String>();

	public FK(Table table) {
		if (table == null) {
			throw new RuntimeException("Table can't be NULL.");
		}
		this.table = table;
	}

	/**
	 * Used by JSONObject, developer don't use this constructor
	 */
	public FK() {

	}

	/**
	 * Set table of FK
	 * 
	 * @param table Table
	 */
	protected void setTable(Table table) {
		if (table == null) {
			throw new RuntimeException("table can't be NULL.");
		}
		this.table = table;
	}

	//	public int getDeferability() {
	//		return deferability;
	//	}
	//
	//	public void setDeferability(int deferability) {
	//		this.deferability = deferability;
	//	}

	public int getDeleteRule() {
		return deleteRule;
	}

	public void setDeleteRule(int deleteRule) {
		this.deleteRule = deleteRule;
	}

	public int getUpdateRule() {
		return updateRule;
	}

	public void setUpdateRule(int updateRule) {
		this.updateRule = updateRule;
	}

	public String getReferencedTableName() {
		return referencedTableName;
	}

	public void setReferencedTableName(String referencedTableName) {
		this.referencedTableName = referencedTableName;
	}

	public List<String> getCol2RefMapping() {
		return new ArrayList<String>(col2RefMapping.values());
	}

	//	/**
	//	 * Set reference column name of the foreign key
	//	 * 
	//	 * @param referencedColumnNames of referenced column names
	//	 */
	//	public void setReferencedColumnNames(List<String> referencedColumnNames) {
	//		if (referencedColumnNames == null) {
	//			this.col2RefMapping.clear();
	//			return;
	//		}
	//		this.col2RefMapping.clear();
	//		this.col2RefMapping.addAll(referencedColumnNames);
	//	}

	/**
	 * add Referenced Column Name
	 * 
	 * @param colName String
	 * @param refColName String
	 */
	public void addRefColumnName(String colName, String refColName) {
		if (col2RefMapping.get(colName) == null) {
			col2RefMapping.put(colName, refColName);
		}
	}

	/**
	 * add Referenced Column Name
	 * 
	 * @param cols Map<String, String>
	 */
	public void setColumns(Map<String, String> cols) {
		col2RefMapping.clear();
		if (cols == null || cols.isEmpty()) {
			return;
		}
		col2RefMapping.putAll(cols);
	}

	//	/**
	//	 * add ColumnNames Column Name
	//	 * 
	//	 * @param columnName String
	//	 */
	//	public void addColumnName(String columnName) {
	//		if (columnNames.indexOf(columnName) < 0) {
	//			columnNames.add(columnName);
	//		}
	//	}

	//	/**
	//	 * 
	//	 * @return the columnNames
	//	 */
	//	public List<String> getColumnNames() {
	//		List<String> columnNames = super.getColumnNames();
	//		if (columnNames.isEmpty()) {
	//			columnNames.addAll(this.columnNames);
	//		}
	//		return columnNames;
	//	}
	//
	//	/**
	//	 * @param columnNames the columnNames to set
	//	 */
	//	public void setColumnNames(List<String> columnNames) {
	//		super.setColumnNames(columnNames);
	//		if (columnNames == null) {
	//			this.columnNames.clear();
	//			return;
	//		}
	//		this.columnNames.clear();
	//		this.columnNames.addAll(columnNames);
	//	}

	//	/**
	//	 * 
	//	 * @return the onCacheObject
	//	 */
	//	public String getOnCacheObject() {
	//		return onCacheObject;
	//	}
	//
	//	/**
	//	 * @param onCacheObject the onCacheObject to set
	//	 */
	//	public void setOnCacheObject(String onCacheObject) {
	//		this.onCacheObject = onCacheObject;
	//	}

	/**
	 * @return object type
	 */
	public String getObjType() {
		return OBJ_TYPE_FK;
	}

	public String getName() {
		return name;
	}

	public String getDDL() {
		return ddl;
	}

	public Table getTable() {
		return table;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getColumnNames() {
		return new ArrayList<String>(col2RefMapping.keySet());
	}

	public void setDDL(String ddl) {
		this.ddl = ddl;
	}

	public Map<String, String> getColumns() {
		return new TreeMap<String, String>(col2RefMapping);
	}
	
	public String getRefColumns(String fkColumn) {
		return col2RefMapping.get(fkColumn);
	}

	/**
	 * Copy attributes from source
	 * 
	 * @param src FK
	 */
	public void copyFrom(FK src) {
		if (src == null) {
			return;
		}
		setName(src.getName());
		setColumns(src.getColumns());
		//setDeferability(src.getDeferability());
		setDeleteRule(src.getDeleteRule());
		setUpdateRule(src.getUpdateRule());
		setReferencedTableName(src.getReferencedTableName());
	}

	/**
	 * get FK String
	 * 
	 * @return String
	 */
	public String getFKString() {
		StringBuffer txtBuffer = new StringBuffer();
		// source table name
		txtBuffer.append(this.getName()).append("(").append(this.getTable().getName()).append("[");
		// source column name
		List<String> columnNames = this.getColumnNames();
		for (int i = 0; i < columnNames.size(); i++) {
			String column = columnNames.get(i);
			txtBuffer.append(column);
			if (i != columnNames.size() - 1) {
				txtBuffer.append(",");
			}
		}
		// Referenced table name
		txtBuffer.append("] ").append(" > ").append(this.getReferencedTableName()).append("[");
		// Referenced column name
		List<String> referencedColumnNames = this.getCol2RefMapping();
		for (int i = 0; i < referencedColumnNames.size(); i++) {
			String column = referencedColumnNames.get(i);
			txtBuffer.append(column);
			if (i != referencedColumnNames.size() - 1) {
				txtBuffer.append(",");
			}
		}
		txtBuffer.append("]").append(")");
		return txtBuffer.toString();
	}
}
