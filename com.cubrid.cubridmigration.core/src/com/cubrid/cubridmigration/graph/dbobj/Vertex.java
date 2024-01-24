package com.cubrid.cubridmigration.graph.dbobj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.dbobject.PK;

public class Vertex extends DBObject {
    
    public static final int NONE = 0;
    public static final int FIRST_TYPE = 1;
    public static final int SECOND_TYPE = 2;
    public static final int INTERMEDIATE_TYPE = 3;
    public static final int RECURSIVE_TYPE = 4;
    
	private int id;

	//GDB is selected for select page
	private boolean isNameChanged;
	private boolean isSelected;
	private String tableName;
	private String vertexLabel;
	private Map<String, String> vertexProperties = new HashMap<String, String>();
	private List<Vertex> endVertexes;
	
	private List<Column> columnList = new ArrayList<Column>();
	
	private int vertexType = NONE;
	private boolean hasPK = false;
	//need for source export 
	private String owner;
	private String condition;
	private String ddl;
	
	private long oid;
	private PK pk;
	
	public Vertex() {
		this.endVertexes = new ArrayList<Vertex>();
	}
	
	public List<Column> getColumnList() {
		return columnList;
	}
	public void setColumnList(List<Column> columnList) {
		this.columnList = columnList;
	}
	public void addColumn(Column col) {
		this.columnList.add(col);
	}
	public void addColumnFirst(Column col) {
		this.columnList.add(0, col);
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
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTableName() {
		return tableName;
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
	public void putVertexProperties(String key, String value) {
		this.vertexProperties.put(key, value);
	}
	
	public List<Vertex> getEndVertexes() {
		return endVertexes;
	}
	public void setEndVertexes(List<Vertex> endVertexes) {
		for (Vertex vertex : endVertexes) {
			this.endVertexes.add(vertex);
		}
	}
	
	public void addEndVertexes(Vertex endVertexes) {
		this.endVertexes.add(endVertexes);
	}
    
	public void setVertexType(int type) {
	    this.vertexType = type;
	}
	
	public int getVertexType() {
		return this.vertexType;
	}
	
	public void setHasPK(boolean has) {
	    this.hasPK = has;
	}
	
	public boolean getHasPK() {
	    return this.hasPK;
	}
	
	public void setOwner(String owner) {
	    this.owner = owner;
	}
	
	public String getOwner() {
	    return this.owner;
	}
	
	public void setCondition(String condition) {
	    this.condition = condition;
	}
	
	public String getCondition() {
	    return this.condition;
	}
	
	public Column getColumnByName(String name) {
		for (Column column : columnList) {
			if (column.getName().equalsIgnoreCase(name)) {
				return column;
			}
		}
		return null;
	}
	
	public void transColToProp(){
		for (Column col : columnList) {
			vertexProperties.put(col.getName(), col.getDataType());
		}
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return vertexLabel;
	}

	@Override
	public String getObjType() {
		// TODO Auto-generated method stub
		return "Vertex";
	}

	@Override
	public String getDDL() {
		// TODO Auto-generated method stub
		return ddl;
	}
	
	public void setDDL(String ddl) {
		this.ddl = ddl;
	}

	public long getOid() {
		return oid;
	}

	public void setOid(long oid) {
		this.oid = oid;
	}
	
	public void setPK(PK pk) {
		this.pk = pk;
	}
	
	public void addPK(PK pk) {
		if (this.pk == null) {
			this.setPK(pk);
		} else {
			for (String col_name : pk.getPkColumns()) {
				this.pk.addColumn(col_name);				
			}
		}
	}
	
	public PK getPK() {
		return this.pk;
	}
	
	public boolean isNameChanged() {
		return tableName.equals(vertexLabel);
	}

	public void setNameChanged(boolean isNameChanged) {
		this.isNameChanged = isNameChanged;
	}
}
