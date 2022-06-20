package com.cubrid.cubridmigration.graph;

import java.util.List;

import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Node;

public class GraphSQLHelper extends SQLHelper {
	private List<Node> migratedNodeList;
	private List<Edge> migratedEdgeList;
	
	public List<Node> getMigratedNodeList() {
		return migratedNodeList;
	}
	public void setMigratedNodeList(List<Node> migratedNodeList) {
		this.migratedNodeList = migratedNodeList;
	}
	public List<Edge> getMigratedEdgeList() {
		return migratedEdgeList;
	}
	public void setMigratedEdgeList(List<Edge> migratedEdgeList) {
		this.migratedEdgeList = migratedEdgeList;
	}
	
	private final static GraphSQLHelper HELPER = new GraphSQLHelper();
	
	public static GraphSQLHelper getInstance(String version) {
		return HELPER;
	}
	@Override
	public String getQuotedObjName(String objectName) {
		return new StringBuffer("\"").append(objectName).append("\"").toString();
	}
	@Override
	public String getTestSelectSQL(String sql) {
		//GDB testSelectSQL
		return null;
	}
}
