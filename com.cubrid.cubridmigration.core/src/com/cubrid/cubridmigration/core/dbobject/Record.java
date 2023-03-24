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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;

/**
 * Record
 * 
 * @author Jessie Huang
 * @version 1.0 - 2009-9-17
 */
public class Record {
	private List<ColumnValue> columnValueList = new ArrayList<ColumnValue>();

	public List<ColumnValue> getColumnValueList() {
		return columnValueList;
	}

	public void setColumnValueList(List<ColumnValue> columnValueList) {
		this.columnValueList = columnValueList;
	}

	/**
	 * add NamedValue
	 * 
	 * @param column Column
	 * @param value Object
	 */
	public void addColumnValue(Column column, Object value) {
		columnValueList.add(new ColumnValue(column, value));
	}
	
	public void addColumnValue(ColumnValue colVal) {
		columnValueList.add(colVal);
	}

	/**
	 * to String
	 * 
	 * @return String
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("\"[");

		for (int i = 0; i < columnValueList.size(); i++) {
			if (i > 0) {
				buf.append(",");
			}
			ColumnValue item = columnValueList.get(i);

			buf.append(item.getColumn().getName()).append("=").append(
					item.getValue());
		}

		buf.append("]\"");
		return buf.toString();
	}

	/**
	 * NamedValue, Can't be changed after created.
	 * 
	 * @author Jessie Huang
	 * @version 1.0 - 2009-9-17
	 */
	public static class ColumnValue {
		private Column column;
		private Object value;

		public ColumnValue(Column column, Object value) {
			this.column = column;
			this.value = value;
		}

		public Column getColumn() {
			return column;
		}

		public Object getValue() {
			return value;
		}
	}

	/**
	 * Retrieves a map with column name key and column data value
	 * 
	 * @return a map which key is the column name and the value is the column
	 *         data.
	 */
	public Map<String, Object> getColumnValueMap() {
		Map<String, Object> result = new HashMap<String, Object>();
		for (ColumnValue cv : columnValueList) {
			result.put(cv.getColumn().getName(), cv.getValue());
		}
		return result;
	}



}
