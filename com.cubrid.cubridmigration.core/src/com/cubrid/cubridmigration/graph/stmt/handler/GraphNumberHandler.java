package com.cubrid.cubridmigration.graph.stmt.handler;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;

public class GraphNumberHandler extends DefaultHandler {
	@Override
	public void handle(PreparedStatement stmt, int idx, ColumnValue columnValue) throws SQLException {
		Object value = columnValue.getValue();
		String strValue = value.toString();
		float intValue = Float.parseFloat(strValue);
		
		stmt.setFloat(idx + 1, intValue);
	}
}
