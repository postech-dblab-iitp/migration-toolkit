package com.cubrid.cubridmigration.graph.dbobj;

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.poi.util.SystemOutLogger;

public class WorkBuffer {
	private Deque<Work> undoList = new ArrayDeque<Work>();
	private Deque<Work> redoList = new ArrayDeque<Work>();
	
	public void addWork(Work work) {
		undoList.push(work);
		
		System.out.println("work add in undo");
		
		if (!redoList.isEmpty()) {
			redoList.clear();
		}
		
		printLog();
	}
	
	public Work undo() {
		Work work = undoList.pop();
		redoList.push(work);
		
		System.out.println("execute undo");
		
		printLog();
		return work;
	}
	
	public boolean isUndoListEmpty() {
		return undoList.isEmpty();
	}
	
	public Work redo() {
		Work work = redoList.pop();
		undoList.push(work);
		
		System.out.println("execute redo");
		
		printLog();
		return work;
	}
	
	public boolean isRedoListEmpty() {
		return redoList.isEmpty();
	}
	
	
	//log code
	public void printLog() {
		System.out.println("data count in undoList : " + undoList.size());
		System.out.println("data count in redoList : " + redoList.size());
	}
}
