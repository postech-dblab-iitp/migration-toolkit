package com.cubrid.cubridmigration.graph.dbobj;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.dbobject.Column;

public class Node {
	private int id;

	//GDB is selected for select page
	private boolean isSelected;
	private String nodeLabel;
	private Map<String, String> nodeProperties;
	private List<Node> endNodes;
	
	private List<Column> columnList;
	
	public Node() {
		this.endNodes = new ArrayList<Node>();
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
	public String getNodeLabel() {
		return nodeLabel;
	}
	public void setNodeLabel(String nodeLabel) {
		this.nodeLabel = nodeLabel;
	}
	public Map<String, String> getNodeProperties() {
		return nodeProperties;
	}
	public void setNodeProperties(Map<String, String> nodeProperties) {
		this.nodeProperties = nodeProperties;
	}
	public List<Node> getEndNodes() {
		return endNodes;
	}
	public void setEndNodes(List<Node> endNodes) {
		for (Node node : endNodes) {
			this.endNodes.add(node);
		}
	}
}
