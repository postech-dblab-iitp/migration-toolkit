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
package com.cubrid.cubridmigration.cubrid.stmt;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.cubrid.stmt.handler.BitHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.BlobHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.ClobHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.DateHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.DateTimeHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.DefaultHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.DoubleHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.FloatHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.IntHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.NVarcharHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.NumericHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.SetHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.SetterHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.TimestampHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.VarBitHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.VarcharHandler;

/**
 * CUBRIDParameterSetter responses to read source data value and transform it to
 * target data value and set it to statement parameters.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-31 created by Kevin Cao
 */
public class CUBRIDParameterSetter {

	private final Map<Integer, SetterHandler> handlerMap = new HashMap<Integer, SetterHandler>();
	private final DefaultHandler defaultHandler = new DefaultHandler();

	public CUBRIDParameterSetter(MigrationConfiguration config) {
		String sourceCharset = config.getSourceCharset();
		String targetCharset = config.getTargetCharSet();
		//Build data type to handler map.
		handlerMap.put(DataTypeConstant.CUBRID_DT_BIT, new BitHandler());
		handlerMap.put(DataTypeConstant.CUBRID_DT_VARBIT, new VarBitHandler());

		handlerMap.put(DataTypeConstant.CUBRID_DT_BLOB, new BlobHandler());
		handlerMap.put(DataTypeConstant.CUBRID_DT_CLOB, new ClobHandler(
				sourceCharset, targetCharset));

		handlerMap.put(DataTypeConstant.CUBRID_DT_DATE, new DateHandler());
		handlerMap.put(DataTypeConstant.CUBRID_DT_DATETIME,
				new DateTimeHandler());
		handlerMap.put(DataTypeConstant.CUBRID_DT_TIMESTAMP,
				new TimestampHandler());

		handlerMap.put(DataTypeConstant.CUBRID_DT_FLOAT, new FloatHandler());
		handlerMap.put(DataTypeConstant.CUBRID_DT_DOUBLE, new DoubleHandler());
		handlerMap.put(DataTypeConstant.CUBRID_DT_NUMERIC, new NumericHandler());

		handlerMap.put(DataTypeConstant.CUBRID_DT_SMALLINT, defaultHandler);
		handlerMap.put(DataTypeConstant.CUBRID_DT_INTEGER, defaultHandler);
//		handlerMap.put(DataTypeConstant.CUBRID_DT_INTEGER, new IntHandler());
		handlerMap.put(DataTypeConstant.CUBRID_DT_BIGINT, defaultHandler);

		SetHandler value = new SetHandler();
		handlerMap.put(DataTypeConstant.CUBRID_DT_SEQUENCE, value);
		handlerMap.put(DataTypeConstant.CUBRID_DT_SET, value);
		handlerMap.put(DataTypeConstant.CUBRID_DT_MULTISET, value);

		VarcharHandler charHandler = new VarcharHandler();
		handlerMap.put(DataTypeConstant.CUBRID_DT_CHAR, charHandler);
		handlerMap.put(DataTypeConstant.CUBRID_DT_VARCHAR, charHandler);

		NVarcharHandler ncharHandler = new NVarcharHandler();
		handlerMap.put(DataTypeConstant.CUBRID_DT_NCHAR, ncharHandler);
		handlerMap.put(DataTypeConstant.CUBRID_DT_NVARCHAR, ncharHandler);
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
		SetterHandler setterHandler = handlerMap.get(columnValue.getColumn().getJdbcIDOfDataType());
		if (setterHandler == null) {
			setterHandler = defaultHandler;
		}
		return setterHandler;
	}

}
