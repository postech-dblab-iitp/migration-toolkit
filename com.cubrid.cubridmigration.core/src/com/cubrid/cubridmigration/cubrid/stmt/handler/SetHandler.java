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
package com.cubrid.cubridmigration.cubrid.stmt.handler;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDFormator;

/**
 * DefaultHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-9-1 created by Kevin Cao
 */
public class SetHandler extends
		DefaultHandler {
	/**
	 * 
	 * @param stmt PreparedStatement
	 * @param idx parameter index
	 * @param columnValue ColumnValue
	 * @throws SQLException when SQL error.
	 */
	public void handle(PreparedStatement stmt, int idx, ColumnValue columnValue) throws SQLException {
		Column column = columnValue.getColumn();
		//Integer dataTypeID = column.getJdbcIDOfDataType();
		Object value = columnValue.getValue();
		if ("".equals(value)
				&& !CUBRIDDataTypeHelper.getInstance(null).isGenericString(
						columnValue.getColumn().getSubDataType())) {
			stmt.setNull(idx + 1, Types.NULL);
			return;
		}
		Object[] values;
		try {
			if (value.getClass().isArray()) {
				values = (Object[]) value;
			} else {
				values = CUBRIDFormator.getCollectionValues(column.getDataTypeInstance(),
						String.valueOf(value));
			}

			Method method = Class.forName("cubrid.jdbc.driver.CUBRIDPreparedStatement", false,
					stmt.getClass().getClassLoader()).getMethod("setCollection",
					new Class[] {int.class, java.lang.Object[].class});
			method.invoke(stmt, idx + 1, values);
		} catch (Exception e) {
			throw new NormalMigrationException(e);
		}
	}
}
