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
	public void setMigratedVertexList(Vertex migratedVertex) {
		this.migratedVertexList.add(migratedVertex);
	}
	public List<Edge> getMigratedEdgeList() {
		return migratedEdgeList;
	}
	public void setMigratedEdgeList(Edge migratedEdge) {
		this.migratedEdgeList.add(migratedEdge);
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
	
	public void clean() {
		migratedVertexList.clear();
		migratedEdgeList.clear();
	}
	
	public void setVertexAndEdge() {
		for (Edge edge : migratedEdgeList) {
			for (Vertex vertex : migratedVertexList) {
				String startVertexName = edge.getStartVertexName();
				if (vertex.getVertexLabel().equals(startVertexName)) {
					edge.setStartVertex(vertex);
				}
				
				for (String name : edge.getEndVertexName()) {
					if (vertex.getVertexLabel().equals(name)) {
						edge.setEndVertex(vertex);
					}
				}
			}
		}
		
		for (Vertex vertex : migratedVertexList) {
			String startVertexName = vertex.getVertexLabel();
			for (Edge edge : migratedEdgeList) {
				if (edge.getStartVertexName().equals(startVertexName)) {
					vertex.setEndVertexes(edge.getEndVertex());
				}
			}
		}
	}
}
	
	
