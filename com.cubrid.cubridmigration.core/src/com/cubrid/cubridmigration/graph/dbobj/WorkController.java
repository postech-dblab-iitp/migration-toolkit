package com.cubrid.cubridmigration.graph.dbobj;

public class WorkController {
	private	int workType;
	private Edge edge;
	
	
	public void setWork(Work work) {
		this.edge = work.getEdge();
		this.workType = work.getWorkType();
	}
	
	public Edge getEdge() {
		return this.edge;
	}
	
	public int getWorkType() {
		return this.workType;
	}
	
	public Work createWork(int workType, Edge edge) {
		return new Work(edge, workType);
	}
}
