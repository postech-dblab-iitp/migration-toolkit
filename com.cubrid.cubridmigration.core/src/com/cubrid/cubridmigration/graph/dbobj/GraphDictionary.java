package com.cubrid.cubridmigration.graph.dbobj;

import java.util.ArrayList;
import java.util.List;

public class GraphDictionary {
	private List<Vertex> migratedVertexList;
	private List<Edge> migratedEdgeList;

	public GraphDictionary() {
		this.migratedEdgeList = new ArrayList<Edge>();
		this.migratedVertexList = new ArrayList<Vertex>();
	}
	
	public List<Vertex> getMigratedVertexList() {
		return migratedVertexList;
	}
	public Vertex getMigratedVertexByName(String vertexLabel) {
		for (Vertex tarVertex : migratedVertexList) {
			if (tarVertex.getVertexLabel().equals(vertexLabel)) {
				return tarVertex;
			}
		}
		return null;
	}
	public void addMigratedVertexList(Vertex migratedVertex) {
		this.migratedVertexList.add(migratedVertex);
	}
	
	public List<Edge> getMigratedEdgeList() {
		return migratedEdgeList;
	}
	
	public void addMigratedEdgeList(Edge migratedEdge) {
		this.migratedEdgeList.add(migratedEdge);
	}
	
	public void addMigratedEdgeList(List<Edge> edgeList) {
		this.migratedEdgeList.addAll(edgeList);
	}
	
	public void removeEdge(String edgeName) {
		int listSize = migratedEdgeList.size();
		
		for (int i = 0; i < listSize; i++) {
			Edge edge = migratedEdgeList.get(i);
			if (edge.getEdgeLabel().equals(edgeName)) {
				migratedEdgeList.remove(i);
				return;
			}
		}
	}
	
	public void printVertexAndEdge(){
		StringBuilder relationVertexes = new StringBuilder();
		
		for (Vertex vertex : migratedVertexList) {
			for (Edge edge : migratedEdgeList) {
				if (edge.getStartVertexName() != null && edge.getStartVertexName().equals(vertex.getVertexLabel())) {
					relationVertexes.append("   " + edge.getEndVertexName() + "   ");
				}
			}
			
			System.out.println(vertex.getVertexLabel() + "----->" + relationVertexes);
			relationVertexes.delete(0, relationVertexes.length());
		}
	}
	
	public String getPrintVertexAndEdgeInfo(){
		StringBuilder retPrint = new StringBuilder();
		StringBuilder relationVertexes = new StringBuilder();
		StringBuilder vertexPrint = new StringBuilder();
		StringBuilder edgePrint = new StringBuilder();
		String lineSeparator = System.getProperty("line.separator");
		
		vertexPrint.append(lineSeparator);
		vertexPrint.append(" ----- < Vertex > ------- ");
		vertexPrint.append(lineSeparator);
		
		edgePrint.append(lineSeparator);
		edgePrint.append(" ----- < Edge > ------- ");
		edgePrint.append(lineSeparator);
			
		for (Edge edge : migratedEdgeList) {
			edgePrint.append(edge.getEdgeLabel()).append(lineSeparator);
		}
		
		for (Vertex vertex : migratedVertexList) {
			vertexPrint.append(vertex.getVertexLabel()).append(lineSeparator);
			for (Edge edge : migratedEdgeList) {
				if (edge.getStartVertexName() != null && edge.getStartVertexName().equals(vertex.getVertexLabel())) {
					relationVertexes.append("   " + edge.getEndVertexName() + "   ");
				}
			}
			
			retPrint.append(vertex.getVertexLabel() + "----->" + relationVertexes).append(lineSeparator);
			relationVertexes.delete(0, relationVertexes.length());
		}
		
		edgePrint.append(lineSeparator);
		edgePrint.append(" ----- < Graph Design > ------- ");
		edgePrint.append(lineSeparator);
		
		retPrint.insert(0, edgePrint.toString());
		retPrint.insert(0, vertexPrint.toString());
		
		return retPrint.toString();
	}
	
	public void clean() {
		migratedVertexList.clear();
		migratedEdgeList.clear();
	}
	
	// For Zest Graph Lib
	public void setVertexAndEdge() {
		for (Edge edge : migratedEdgeList) {
			for (Vertex vertex : migratedVertexList) {
				String startVertexName = edge.getStartVertexName();
				if (vertex.getVertexLabel().equals(startVertexName)) {
					edge.setStartVertex(vertex);
				}
				
				if (vertex.getVertexLabel().equals(edge.getEndVertexName())) {
					edge.setEndVertex(vertex);
				}
			}
			
			edge.transColToProp();
		}
		
		for (Vertex vertex : migratedVertexList) {
			String startVertexName = vertex.getVertexLabel();
			for (Edge edge : migratedEdgeList) {
				if (edge.getStartVertexName().equals(startVertexName)) {
					vertex.addEndVertexes(edge.getEndVertex());
				}
			}
			
			vertex.transColToProp();
		}
	}
}
	
	
