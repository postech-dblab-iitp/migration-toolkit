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

import org.apache.commons.collections.CollectionUtils;

/**
 * 
 * Primary key object
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-15 created by moulinwang
 */
public class PK extends
		DBObject {

	private static final long serialVersionUID = -7120242805071397679L;

	private String name;
	private final List<String> pkColumns = new ArrayList<String>();
	private Table table;
	private String ddl;

	public PK(Table table) {
		if (table == null) {
			throw new RuntimeException("Table can't be NULL.");
		}
		this.table = table;
	}

	/**
	 * Used by JSONObject, developer don't use this constructor
	 */
	public PK() {

	}

	/**
	 * Set table of PK
	 * 
	 * @param table Table
	 */
	protected void setTable(Table table) {
		if (table == null) {
			throw new RuntimeException("table can't be NULL.");
		}
		this.table = table;
	}

	/**
	 * @return object type
	 */
	public String getObjType() {
		return OBJ_TYPE_PK;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * getPkColumns
	 * 
	 * @return List<String>
	 */
	public List<String> getPkColumns() {
		return new ArrayList<String>(pkColumns);
	}

	/**
	 * Add column of PK
	 * 
	 * @param colName column name
	 */
	public void addColumn(String colName) {
		if (pkColumns.indexOf(colName) < 0) {
			pkColumns.add(colName);
		}
	}

	/**
	 * Set PK columns
	 * 
	 * @param colNames List<String>
	 */
	public void setPkColumns(List<String> colNames) {
		pkColumns.clear();
		if (CollectionUtils.isEmpty(colNames)) {
			return;
		}
		for (String col : colNames) {
			addColumn(col);
		}
	}

	/**
	 * Remove column
	 * 
	 * @param colName column name
	 */
	public void removeColumn(String colName) {
		pkColumns.remove(colName);
	}

	/**
	 * Clear all columns
	 * 
	 */
	public void clearColumns() {
		pkColumns.clear();
	}

	public Table getTable() {
		return table;
	}

	public String getDDL() {
		return ddl;
	}

	public void setDDL(String ddl) {
		this.ddl = ddl;
	}

	@Override
	public String getSourceDBObject() {
		// TODO Auto-generated method stub
		return null;
	}
}
