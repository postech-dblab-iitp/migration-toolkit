package com.cubrid.cubridmigration.cubrid.stmt.handler;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;

public class IntHandler extends DefaultHandler {
	
	@Override
	public void handle(PreparedStatement stmt, int idx, ColumnValue columnValue) throws SQLException {
		Object value = columnValue.getValue();
		String strValue = value.toString();
		int intValue = Integer.parseInt(strValue);
		
		stmt.setInt(idx + 1, intValue);
	}
}