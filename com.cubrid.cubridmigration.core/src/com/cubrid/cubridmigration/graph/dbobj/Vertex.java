package com.cubrid.cubridmigration.graph.dbobj;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.dbobject.Column;

public class Vertex {
	private int id;

	//GDB is selected for select page
	private boolean isSelected;
	private String vertexLabel;
	private Map<String, String> vertexProperties;
	private List<Vertex> endVertexes;
	
	private List<Column> columnList;
	
	public Vertex() {
		this.endVertexes = new ArrayList<Vertex>();
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
	public void setSelected(boolean isSelected) {
		this.isSelected = isSelected;
	}
	public String getVertexLabel() {
		return vertexLabel;
	}
	public void setVertexLabel(String vertexLabel) {
		this.vertexLabel = vertexLabel;
	}
	public Map<String, String> getVertexProperties() {
		return vertexProperties;
	}
	public void setVertexProperties(Map<String, String> vertexProperties) {
		this.vertexProperties = vertexProperties;
	}
	public List<Vertex> getEndVertexes() {
		return endVertexes;
	}
	public void setEndVertexes(List<Vertex> endVertexes) {
		for (Vertex vertex : endVertexes) {
			this.endVertexes.add(vertex);
		}
	}
}
