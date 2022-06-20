package com.cubrid.cubridmigration.graph.dbobj;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.dbobject.Column;

public class Edge {
	private int id;
	
	//GDB isSelected for check page
	private boolean isSelected;
	
	private String edgeLabel;
	private Node startNode;
	private List<Node> endNode;
	
	private String startNodeName;
	private List<String> endNodeName;
	
	private List<Column> columnList;
	
	private Map<String, String> edgeProperties;
	
	public Edge() {
		this.endNode = new ArrayList<Node>();
		this.endNodeName = new ArrayList<String>();
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
	public Node getStartNode() {
		return startNode;
	}
	public void setStartNode(Node startNode) {
		this.startNode = startNode;
	}
	public List<Node> getEndNode() {
		return endNode;
	}
	public void setEndNode(Node endNode) {
		this.endNode.add(endNode);
	}
	public String getStartNodeName() {
		return startNodeName;
	}
	public void setStartNodeName(String startNodeName) {
		this.startNodeName = startNodeName;
	}
	public List<String> getEndNodeName() {
		return endNodeName;
	}
	public void setEndNodeName(String endNodeName) {
		this.endNodeName.add(endNodeName);
	}
	public Map<String, String> getEdgeProperties() {
		return edgeProperties;
	}
	public void setEdgeProperties(Map<String, String> edgeProperties) {
		this.edgeProperties = edgeProperties;
	}
	
}
