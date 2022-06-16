package com.cubrid.cubridmigration.graph.dbobj;

import java.util.ArrayList;
import java.util.List;

public class GraphDictionary {
	private List<Node> migratedNodeList;
	private List<Edge> migratedEdgeList;

	public GraphDictionary() {
		this.migratedEdgeList = new ArrayList<Edge>();
		this.migratedNodeList = new ArrayList<Node>();
	}
	
	public List<Node> getMigratedNodeList() {
		return migratedNodeList;
	}
	public Node getMigratedNodeByName(String nodeLabel) {
		for (Node tarNode : migratedNodeList) {
			if (tarNode.getNodeLabel().equals(nodeLabel)) {
				return tarNode;
			}
		}
		return null;
	}
	public void setMigratedNodeList(Node migratedNode) {
		this.migratedNodeList.add(migratedNode);
	}
	public List<Edge> getMigratedEdgeList() {
		return migratedEdgeList;
	}
	public void setMigratedEdgeList(Edge migratedEdge) {
		this.migratedEdgeList.add(migratedEdge);
	}
	
	public void printNodeAndEdge(){
		StringBuilder relationNodes = new StringBuilder();
		
		for (Node node : migratedNodeList) {
			for (Edge edge : migratedEdgeList) {
				if (edge.getStartNodeName() != null && edge.getStartNodeName().equals(node.getNodeLabel())) {
					relationNodes.append("   " + edge.getEndNodeName() + "   ");
				}
			}
			
			System.out.println(node.getNodeLabel() + "----->" + relationNodes);
			relationNodes.delete(0, relationNodes.length());
		}
	}
	
	public void clean() {
		migratedNodeList.clear();
		migratedEdgeList.clear();
	}
	
	public void setNodeAndEdge() {
		for (Edge edge : migratedEdgeList) {
			for (Node node : migratedNodeList) {
				String startNodeName = edge.getStartNodeName();
				if (node.getNodeLabel().equals(startNodeName)) {
					edge.setStartNode(node);
				}
				
				for (String name : edge.getEndNodeName()) {
					if (node.getNodeLabel().equals(name)) {
						edge.setEndNode(node);
					}
				}
			}
		}
		
		for (Node node : migratedNodeList) {
			String startNodeName = node.getNodeLabel();
			for (Edge edge : migratedEdgeList) {
				if (edge.getStartNodeName().equals(startNodeName)) {
					node.setEndNodes(edge.getEndNode());
				}
			}
		}
	}
}
	
	
