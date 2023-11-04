package com.cubrid.cubridmigration.graph.stmt.handler;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;

public class GraphIntHandler extends DefaultHandler {
	@Override
	public void handle(PreparedStatement stmt, int idx, ColumnValue columnValue) throws SQLException {
		Object value = columnValue.getValue();
		String strValue = value.toString();
		int intValue = Integer.parseInt(strValue);
		
		stmt.setInt(idx + 1, intValue);
	}
}
