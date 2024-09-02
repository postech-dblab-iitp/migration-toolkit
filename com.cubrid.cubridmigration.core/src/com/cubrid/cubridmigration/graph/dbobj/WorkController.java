package com.cubrid.cubridmigration.graph.dbobj;

import com.cubrid.cubridmigration.core.dbobject.DBObject;

public class WorkController {
	private	int workType;
	private DBObject gdbObject;
	private String originalName;
	
	public void setWork(Work work) {
		this.workType = work.getWorkType();
		this.gdbObject = work.getObject();
		this.originalName = work.getOriginalName();
	}
	
	public int getWorkType() {
		return this.workType;
	}
	
	public DBObject getObject() {
		return this.gdbObject;
	}
	
	public String getOriginalName() {
		return this.originalName;
	}
	
	public Work createWork(int workType, DBObject object) {
		return new Work(workType, object);
	}
	
	public Work createWork(int workType, DBObject object, String originalName) {
		return new Work(workType, object, originalName);
	}
}
