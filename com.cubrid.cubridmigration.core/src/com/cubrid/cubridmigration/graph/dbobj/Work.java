package com.cubrid.cubridmigration.graph.dbobj;

import com.cubrid.cubridmigration.core.dbobject.DBObject;

public class Work {
	private int workType;
	private DBObject object;
	private String originalName;
	
	public Work(int workType, DBObject obj) {
		this.workType = workType;
		this.object = obj;
	}
	
	public Work(int workType, DBObject object, String originalName) {
		this.workType = workType;
		this.object = object;
		this.originalName = originalName;
	}

	public int getWorkType() {
		return this.workType;
	}
	
	public DBObject getObject() {
		return this.object;
	}
	
	public String getOriginalName() {
		return this.originalName;
	}

	public void setOriginalName(String name) {
		this.originalName = name;
	}
	
}
