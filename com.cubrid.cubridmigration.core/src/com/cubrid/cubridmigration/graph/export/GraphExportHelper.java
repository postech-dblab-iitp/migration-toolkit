package com.cubrid.cubridmigration.graph.export;

import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

public class GraphExportHelper extends DBExportHelper {

	//GDB GraphExportHelper
	
	public DatabaseType getDBType() {
		return null;
	}

	@Override
	protected String getQuotedObjName(String objectName) {
		return null;
	}

	@Override
	public String getPagedSelectSQL(String sql, long pageSize,
			long exportedRecords, PK pk) {
		
		StringBuffer buffer = new StringBuffer(sql);
		
		if (!(exportedRecords <= 0)) {
			buffer.append(" skip " + exportedRecords);
		}
		
		buffer.append(" limit " + pageSize);
		
		return buffer.toString();
	}

	@Override
	public String getGraphSelectSQL(Vertex v, boolean targetIsCSV) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getPagedSelectSQLForVertexCSV(Vertex v, String sql, long realPageCount,
			long totalExported, PK pk) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getGraphSelectSQL(Edge e) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getPagedSelectSQLForEdgeCSV(Edge e, String sql, long realPageCount,
			long totalExported, PK pk, boolean hasMultiSchema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getGraphSelectSQL(Edge e, boolean targetIsCSV) {
		// TODO Auto-generated method stub
		return null;
	}

}
