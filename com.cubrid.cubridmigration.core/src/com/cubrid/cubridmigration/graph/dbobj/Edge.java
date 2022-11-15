package com.cubrid.cubridmigration.graph.dbobj;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.dbobject.Column;

public class Edge {
    
    public static final int NONE = 0;
    public static final int RECURSIVE_TYPE = 1;
    public static final int JOINTABLE_TYPE = 2;
    public static final int FK_TYPE = 2;
    
	private int id;
	
	//GDB isSelected for check page
	private boolean isSelected;
	
	private String edgeLabel;
	private Vertex startVertex;
	private List<Vertex> endVertex;
	
	private String startVertexName;
	private List<String> endVertexName;
	
	private List<Column> columnList;
	
	private Map<String, String> edgeProperties;
	
	private int edgeType = NONE;
	
	public Edge() {
		this.endVertex = new ArrayList<Vertex>();
		this.endVertexName = new ArrayList<String>();
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
	public List<Vertex> getEndVertex() {
		return endVertex;
	}
	public void setEndVertex(Vertex endVertex) {
		this.endVertex.add(endVertex);
	}
	public String getStartVertexName() {
		return startVertexName;
	}
	public void setStartVertexName(String startVertexName) {
		this.startVertexName = startVertexName;
	}
	public List<String> getEndVertexName() {
		return endVertexName;
	}
	public void setEndVertexName(String endVertexName) {
		this.endVertexName.add(endVertexName);
	}
	public Map<String, String> getEdgeProperties() {
		return edgeProperties;
	}
	public void setEdgeProperties(Map<String, String> edgeProperties) {
		this.edgeProperties = edgeProperties;
	}
	
	public void setEdgeType(int type) {
	    this.edgeType = type;
	}
	
	public int getEdgeType() {
	    return this.edgeType;
	}
}
