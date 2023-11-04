package com.cubrid.cubridmigration.graph.stmt.handler;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;

public class GraphDateHandler extends DefaultHandler {
	public void handle(PreparedStatement stmt, int idx, ColumnValue columnValue) throws SQLException {
		//Column column = columnValue.getColumn();
		//Integer dataTypeID = column.getJdbcIDOfDataType();
		Object value = columnValue.getValue();
		if ("".equals(value)) {
			stmt.setNull(idx + 1, Types.NULL);
			return;
		}
		stmt.setDate(idx + 1, Date.valueOf(String.valueOf(value)));
	}
}
