package com.cubrid.cubridmigration.graph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

public class GraphSQLHelper extends SQLHelper {
	private List<Vertex> migratedVertexList;
	private List<Edge> migratedEdgeList;
	
	public List<Vertex> getMigratedVertexList() {
		return migratedVertexList;
	}
	public void setMigratedVertexList(List<Vertex> migratedVertexList) {
		this.migratedVertexList = migratedVertexList;
	}
	public List<Edge> getMigratedEdgeList() {
		return migratedEdgeList;
	}
	public void setMigratedEdgeList(List<Edge> migratedEdgeList) {
		this.migratedEdgeList = migratedEdgeList;
	}
	
	private final static GraphSQLHelper HELPER = new GraphSQLHelper();
	
	public static GraphSQLHelper getInstance(String version) {
		return HELPER;
	}
	
	public String getVertexDDL(Vertex v, Record record) {
		String ddl = getTargetInsertVertex(v, record);
		
		return ddl;
	}
	
	public String getEdgeDDL(Edge e, Record record) {
		String ddl = new String();
		
		if (e.getEdgeType() == Edge.JOINTABLE_TYPE) {
			ddl = getTargetInsertJoinEdge(e, record);
			
		} else {
			for (int i=0 ; i < e.getfkCol2RefMappingSize(); i++) {
				ddl = getTargetInsertEdge(e, i);				
			}
			
		}
		
		return ddl;
	}
	
	private String getTargetInsertEdge(Edge e, int index) {
		StringBuffer buffer = new StringBuffer();
		
		buffer.append("(m:").append(e.getEndVertexName()).append(")");
		buffer.append(" where ");
		buffer.append("n.").append(e.getFKColumnNames().get(index)).append(" = ");
		buffer.append("m.").append(e.getREFColumnNames(e.getFKColumnNames().get(index))).append(" ");
		buffer.append("create (n)-[r:").append(e.getEdgeLabel()).append("]->(m) return count(r)");
		
		return buffer.toString();
	}
	
	private String getTargetInsertJoinEdge(Edge e, Record record) {
		
			Map<String, Object> colValMap = record.getColumnValueMap();
			
			if (!e.getColumnbyName(e.getFKColumnNames().get(0)).getSupportGraphDataType()
					|| !e.getColumnbyName(e.getREFColumnNames(e.getFKColumnNames().get(0))).getSupportGraphDataType()) {
				return null;
			}
			
			StringBuffer buffer = new StringBuffer();
			buffer.append("Match (n:").append(e.getStartVertexName());
			buffer.append("), (m:").append(e.getEndVertexName());
			buffer.append(")");
			buffer.append(" where n.").append(e.getFKColumnNames().get(0)).append(" = ");
			buffer.append(colValMap.get(e.getFKColumnNames().get(0)));
			buffer.append(" and m.").append(e.getREFColumnNames(e.getFKColumnNames().get(1))).append(" = ");
			buffer.append(colValMap.get(e.getFKColumnNames().get(1)));
			buffer.append(" create (n)-[r:").append(e.getEdgeLabel().replaceAll(" ", "_"));
			
			if (e.getColumnList() != null) {
				buffer.append('{');
				
				for (int index = 0; index < e.getColumnList().size(); index++) {
					buffer.append(e.getColumnList().get(index).getName() + ":");
					
					if (e.getColumnList().get(index).getGraphDataType().equals("string")) {
						buffer.append("\"");
						buffer.append(colValMap.get(e.getColumnList().get(index).getName()));
						buffer.append("\"");
						
					} else if (e.getColumnList().get(index).getGraphDataType().equals("date")){
						buffer.append("date(");
						buffer.append(colValMap.get(e.getColumnList().get(index).getName()));
						buffer.append(')');
						
					} else if (e.getColumnList().get(index).getGraphDataType().equals("datetime")){
						buffer.append("datetime(");
						buffer.append(colValMap.get(e.getColumnList().get(index).getName()));
						buffer.append(')');
						
					} else {
						buffer.append(colValMap.get(e.getColumnList().get(index).getName()));
					}
					
					if (index < e.getColumnList().size() - 1) {
						buffer.append(", ");
					}
				}
				
				buffer.append('}');
			}
			
			buffer.append("]");
			buffer.append("->(m) ");
			buffer.append("return count(r)");
			return buffer.toString();
	}
	
	private String getTargetInsertVertex(Vertex v, Record record) {
		int supportColumCount = 0;
		StringBuffer buffer = new StringBuffer("CREATE (n: ").append(v.getVertexLabel()).append(" {");
		List<Column> columns = v.getColumnList();
		int len = columns.size();
		
		HashMap<String, Object> colValMap = (HashMap<String, Object>) record.getColumnValueMap();
		
		for (int i = 0; i < len; i++) {
			if (!columns.get(i).isSelected()) {
				continue;
			}
			
			if (!columns.get(i).getSupportGraphDataType()) {
				continue;
			}
			
			supportColumCount++;
			
			if (i > 0) {
				buffer.append(", ");
			}
			String columnName = columns.get(i).getName();
			columnName = columnName.replaceAll("\"", "");
			buffer.append(columnName).append(':');
			
			if (columns.get(i).getGraphDataType().equals("datetime")) {
				buffer.append("datetime(");
				buffer.append(colValMap.get(columnName));
				buffer.append(")");
				
			} else if (columns.get(i).getGraphDataType().equals("date")) {
				buffer.append("date(");
				buffer.append(colValMap.get(columnName));
				buffer.append(")");
				
			} else if (columns.get(i).getGraphDataType().equals("string")){
				buffer.append("\"");
				
				String colVal = (String) colValMap.get(columnName);
				
				if (colVal != null) {
					colVal = colVal.trim();
				}
				
				buffer.append(colVal);
				buffer.append("\"");
			} else {
				buffer.append(colValMap.get(columnName));
			}
		}
		
		if (supportColumCount == 0) {
			return null;
		}
		
		buffer.append("}");
		buffer.append(")");
		buffer.append(" return n");
		return buffer.toString();
	}
	
	
	
	@Override
	public String getQuotedObjName(String objectName) {
		return new StringBuffer("\"").append(objectName).append("\"").toString();
	}
	@Override
	public String getTestSelectSQL(String sql) {
		//GDB testSelectSQL
		return null;
	}
}
