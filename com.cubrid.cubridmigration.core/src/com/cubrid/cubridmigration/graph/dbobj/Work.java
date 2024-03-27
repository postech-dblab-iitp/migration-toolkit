package com.cubrid.cubridmigration.graph.dbobj;

public class Work {
	private int workType;
	private Edge edge;
	
	public Work(Edge edge, int workType) {
		this.edge = edge;
		this.workType = workType;
	}
	
	public Edge getEdge() {
		return this.edge;
	}
	
	public int getWorkType() {
		return this.workType;
	}
}
