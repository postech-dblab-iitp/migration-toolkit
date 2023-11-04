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
package com.cubrid.cubridmigration.graph.stmt;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.cubrid.stmt.handler.DefaultHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.SetterHandler;
import com.cubrid.cubridmigration.graph.stmt.handler.GraphDateHandler;
import com.cubrid.cubridmigration.graph.stmt.handler.GraphIntHandler;
import com.cubrid.cubridmigration.graph.stmt.handler.GraphNumberHandler;
import com.cubrid.cubridmigration.graph.stmt.handler.GraphStringHandler;

/**
 * CUBRIDParameterSetter responses to read source data value and transform it to
 * target data value and set it to statement parameters.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-31 created by Kevin Cao
 */
public class GraphParameterSetter {

	private final Map<String, SetterHandler> handlerMap = new HashMap<String, SetterHandler>();
	private final DefaultHandler defaultHandler = new DefaultHandler();

	public GraphParameterSetter(MigrationConfiguration config) {
		String sourceCharset = config.getSourceCharset();
		String targetCharset = config.getTargetCharSet();
		
		GraphDateHandler dateHandler = new GraphDateHandler();
		handlerMap.put("date", dateHandler);
		
		GraphIntHandler intHandler = new GraphIntHandler();
		handlerMap.put("integer", intHandler);
		
		GraphStringHandler stringHandler = new GraphStringHandler();
		handlerMap.put("string", stringHandler);
		
		GraphNumberHandler numberHandler = new GraphNumberHandler();
		handlerMap.put("float", numberHandler);
	}

	/**
	 * Set column value to prepared statement.
	 * 
	 * @param record Record
	 * @param stmt PreparedStatement
	 */
	public void setRecord2Statement(Record record, PreparedStatement stmt) {
		int len = record.getColumnValueList().size();
		try {
			for (int i = 0; i < len; i++) {
				ColumnValue columnValue = record.getColumnValueList().get(i);
				Object value = columnValue.getValue();
				final SetterHandler handler = getHandler(columnValue);
				if (value == null) {
					handler.setNull(stmt, i);
				} else {
					handler.handle(stmt, i, columnValue);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new NormalMigrationException(e);
		}
	}
	
	public void setEdgeRecord2Statement(Record record, PreparedStatement pstmt) {
		int len = record.getColumnValueList().size();
		try {
			for (int i = 0; i < len; i++) {
				ColumnValue columnValue = record.getColumnValueList().get(i);
				Object value = columnValue.getValue();
				final SetterHandler handler = getHandler(columnValue);
				if (value == null) {
					handler.setNull(pstmt, i);
				} else {
					handler.handle(pstmt, i, columnValue);
				}
			}
			for (int i = 0; i < len; i++) {
				ColumnValue columnValue = record.getColumnValueList().get(i);
				Object value = columnValue.getValue();
				final SetterHandler handler = getHandler(columnValue);
				if (value == null) {
					handler.setNull(pstmt, i + 2);
				} else {
					handler.handle(pstmt, i + 2, columnValue);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new NormalMigrationException(e);
		}
	}

	/**
	 * If cannot find hander, return a default handler.
	 * 
	 * @param columnValue ColumnValue
	 * @return SetterHandler
	 */
	private SetterHandler getHandler(ColumnValue columnValue) {
//		SetterHandler setterHandler = handlerMap.get(columnValue.getColumn().getJdbcIDOfDataType());
		SetterHandler setterHandler = handlerMap.get(columnValue.getColumn().getGraphDataType());
		if (setterHandler == null) {
			setterHandler = defaultHandler;
		}
		return setterHandler;
	}

}
