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
 * to store a table or view information <li>for table, including columns,
 * constraint <li>for view, including columns, query specifications
 * 
 * @author moulinwang
 * @version 1.0 - 2010-11-01 created by moulinwang
 */
public abstract class TableOrView extends
		DBObject {

	private static final long serialVersionUID = -1637002612150615363L;
	protected Schema schema;
	protected String name;
	/**
	 * Owner name such as SCOTT of SCOTT.EMP on Oracle. It is a null value
	 * except Oracle.
	 */
	protected String owner;
	protected final List<Column> columns = new ArrayList<Column>();

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public List<Column> getColumns() {
		return new ArrayList<Column>(columns);
	}

	/**
	 * Change the values of table columns
	 * 
	 * @param columns to be set
	 */
	public void setColumns(List<Column> columns) {
		if (CollectionUtils.isEmpty(columns)) {
			this.columns.clear();
			return;
		}
		this.columns.clear();
		for (Column col : columns) {
			addColumn(col);
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * add column
	 * 
	 * @param column Column
	 */
	public void addColumn(Column column) {
		if (column == null || getColumnByName(column.getName()) != null) {
			return;
		}
		columns.add(column);
		column.setTableOrView(this);
	}

	/**
	 * get Column By column Name
	 * 
	 * @param name String
	 * @return Column
	 */
	public Column getColumnByName(String name) {
		for (Column column : columns) {
			if (column.getName().equalsIgnoreCase(name)) {
				return column;
			}
		}
		return null;
	}

	/**
	 * get Column By column Name
	 * 
	 * @param name String
	 * @return Column
	 */
	public Column getColumnWithNoCase(String name) {
		for (Column column : columns) {
			if (column.getName().equalsIgnoreCase(name)) {
				return column;
			}
		}
		return null;
	}

	/**
	 * remove column
	 * 
	 * @param column Column
	 */
	public void removeColumn(Column column) {
		columns.remove(column);
	}

}
