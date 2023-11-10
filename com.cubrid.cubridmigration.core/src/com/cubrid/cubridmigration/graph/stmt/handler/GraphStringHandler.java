package com.cubrid.cubridmigration.graph.stmt.handler;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;

public class GraphStringHandler extends DefaultHandler {
	public void handle(PreparedStatement stmt, int idx, ColumnValue columnValue) throws SQLException {
		Object value = columnValue.getValue();
		stmt.setString(idx + 1, String.valueOf(value));
	}
	
	public void setNull(PreparedStatement stmt, int idx) throws SQLException {
		stmt.setNull(idx + 1, Types.NULL);
	}
}
