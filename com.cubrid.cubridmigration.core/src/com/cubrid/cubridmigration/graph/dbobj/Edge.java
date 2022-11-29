package com.cubrid.cubridmigration.graph.dbobj;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.dbobject.Column;

public class Edge {
    
    public static final int NONE = 0;
    public static final int SECOND_FK_TYPE = 1;
    public static final int INTERMEDIATE_FK_TYPE = 2;
    public static final int JOINTABLE_TYPE = 3;
    public static final int RECURSIVE_TYPE = 4;
    
	private int id;
	
	//GDB isSelected for check page
	private boolean isSelected;
	
	private String edgeLabel;
	private Vertex startVertex;
	private Vertex endVertex;
	
	private String startVertexName;
	private String endVertexName;
	
	private List<Column> columnList;
	
	private List<String> fkColumnList = new ArrayList<String>();
	
	private Map<String, String> edgeProperties;
    
	private String fkString;
	
	private int edgeType = NONE;
	private String owner;
	
	boolean isHavePKStartVertex = false;
	
	public Edge() {
	}
	
	public List<Column> getColumnList() {
		return columnList;
	}
	public void setColumnList(List<Column> columnList) {
		this.columnList = columnList;
	}
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public boolean isSelected() {
		return isSelected;
	}
	public void setSelect(boolean isSelected) {
		this.isSelected = isSelected;
	}
	public String getEdgeLabel() {
		return edgeLabel;
	}
	public void setEdgeLabel(String edgeLabel) {
		this.edgeLabel = edgeLabel;
	}
	public Vertex getStartVertex() {
		return startVertex;
	}
	public void setStartVertex(Vertex startVertex) {
		this.startVertex = startVertex;
	}
	public Vertex getEndVertex() {
		return endVertex;
	}
	public void setEndVertex(Vertex endVertex) {
		this.endVertex = endVertex;
	}
	public String getStartVertexName() {
		return startVertexName;
	}
	public void setStartVertexName(String startVertexName) {
		this.startVertexName = startVertexName;
	}
	public String getEndVertexName() {
		return endVertexName;
	}
	public void setEndVertexName(String endVertexName) {
		this.endVertexName = endVertexName;
	}
	public Map<String, String> getEdgeProperties() {
		return edgeProperties;
	}
	public void setEdgeProperties(Map<String, String> edgeProperties) {
		this.edgeProperties = edgeProperties;
	}
	
	public String getFKString() {
		return fkString;
	}
	public void setFKSring(String fkString) {
		this.fkString = fkString;
	}
    
	public void setEdgeType(int type) {
	    this.edgeType = type;
	}
	
	public int getEdgeType() {
	    return this.edgeType;
	}
	
	public void setOwner(String owner) {
	    this.owner = owner;
	}
	
	public String getOwner() {
	    return this.owner;
	}
	
	public void addFKColumnList(String id) {
		fkColumnList.add(id);
	}
	
	public List<String> getFKColumnList() {
		return fkColumnList;
	}
	
	public void setHavePKStartVertex(boolean haved) {
	    this.isHavePKStartVertex = haved;
	}
	
	public boolean isHavePKStartVertex() {
	    return this.isHavePKStartVertex;
	}
}
