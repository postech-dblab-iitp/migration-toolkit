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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 
 * Index
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-15 created by moulinwang
 */
public class Index extends
		DBObject {
	private static final long serialVersionUID = 4983959397364084979L;

	protected Comparator<String> comparator = new Comparator<String>() {

		public int compare(String o1, String o2) {
			return o1.equals(o2) ? 0 : 1;
		}

	};
	protected String indexName;
	//protected String migrationPrefix;
	private boolean unique;
	private boolean reverse;
	//Key is column name, and value is order "A" or "D" or "null"
	protected Map<String, Boolean> indexColumns = new TreeMap<String, Boolean>(comparator);
	protected Table table;

	private int indexType;
	protected String ddl;

	/**
	 * Used by JSONObject, developer don't use this constructor
	 */
	public Index() {

	}

	/**
	 * Set table of index
	 * 
	 * @param table Table
	 */
	protected void setTable(Table table) {
		if (table == null) {
			throw new RuntimeException("table can't be NULL.");
		}
		this.table = table;
	}

	public Index(Table table) {
		if (table == null) {
			throw new RuntimeException("table can't be NULL.");
		}
		this.table = table;
	}

	/**
	 * Copy attributes from source
	 * 
	 * @param src Index
	 */
	public void copyFrom(Index src) {
		setIndexColumns(src.getIndexColumns());
		setName(src.getName());
		setReverse(src.isReverse());
		setUnique(src.isUnique());
		setIndexType(src.getIndexType());
	}

	public String getName() {
		return indexName;
	}

	public void setName(String indexName) {
		this.indexName = indexName;
	}

	public boolean isUnique() {
		return unique;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	public List<String> getColumnNames() {
		return new ArrayList<String>(indexColumns.keySet());
	}

	/**
	 * Retrieves index columns configuration
	 * 
	 * @return column name to order
	 */
	public Map<String, Boolean> getIndexColumns() {
		TreeMap<String, Boolean> treeMap = new TreeMap<String, Boolean>(comparator);
		treeMap.putAll(indexColumns);
		return treeMap;
	}

	/**
	 * Set index columns
	 * 
	 * @param columns of index
	 */
	public void setIndexColumns(Map<String, Boolean> columns) {
		if (columns == null || columns.isEmpty()) {
			this.indexColumns.clear();
			return;
		}
		this.indexColumns.clear();
		for (Map.Entry<String, Boolean> entry : columns.entrySet()) {
			this.indexColumns.put(entry.getKey(),
					entry.getValue() == null ? false : entry.getValue());
		}

	}

	/**
	 * getColumnOrderRules
	 * 
	 * @return List<Boolean>
	 */
	public List<Boolean> getColumnOrderRules() {
		return new ArrayList<Boolean>(indexColumns.values());
	}

	/**
	 * 
	 * getColumnOrderRulesString
	 * 
	 * @return List<String>
	 */
	public List<String> getColumnOrderRulesString() {
		List<String> result = new ArrayList<String>();
		for (Boolean role : indexColumns.values()) {
			result.add(role ? "A" : "D");
		}
		return result;
	}

	public Table getTable() {
		return table;
	}

	public int getIndexType() {
		return indexType;
	}

	public void setIndexType(int indexType) {
		this.indexType = indexType;
	}

	/**
	 * add column
	 * 
	 * @param column Column
	 * @param order true if asc, false if desc
	 */
	public void addColumn(String column, boolean order) {
		if (indexColumns.get(column) == null) {
			indexColumns.put(column, order);
		}
	}

	//	public String getMigrationPrefix() {
	//		return migrationPrefix;
	//	}
	//
	//	public void setMigrationPrefix(String migrationPrefix) {
	//		this.migrationPrefix = migrationPrefix;
	//	}

	/**
	 * 
	 * @return the reverse
	 */
	public boolean isReverse() {
		return reverse;
	}

	/**
	 * @param reverse the reverse to set
	 */
	public void setReverse(boolean reverse) {
		this.reverse = reverse;
	}

	/**
	 * @return object type
	 */
	public String getObjType() {
		return OBJ_TYPE_INDEX;
	}

	/**
	 * DDL
	 * 
	 * @return DDL
	 */
	public String getDDL() {
		return ddl;
	}

	/**
	 * DDL
	 * 
	 * @param ddl DDL
	 */
	public void setDDL(String ddl) {
		this.ddl = ddl;
	}

	/**
	 * get Index String
	 * 
	 * @return String
	 */
	public String getIndexString() {
		StringBuffer txtBuffer = new StringBuffer();
		txtBuffer.append(this.getName()).append("(");

		List<String> columnNames = this.getColumnNames();
		for (int i = 0; i < columnNames.size(); i++) {
			String column = columnNames.get(i);
			txtBuffer.append(column);
			if (i != columnNames.size() - 1) {
				txtBuffer.append(",");
			}
		}
		txtBuffer.append(")");
		return txtBuffer.toString();
	}

	/**
	 * whether is Index Node PK
	 * 
	 * @return true if the index is PK
	 */
	public boolean isIndexNodePK() {
		if (!"unique index".equals(this.getIndexType())) {
			return false;
		}
		PK pk = this.getTable().getPk();
		if (pk == null) {
			return true;
		}
		List<String> icn = this.getColumnNames();
		List<String> pcn = pk.getPkColumns();
		for (String pkColumn : pcn) {
			if (icn.contains(pkColumn)) {
				return true;
			}
		}
		return false;
	}
}
