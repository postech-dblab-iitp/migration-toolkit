/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */
package com.cubrid.cubridmigration.core.dbobject;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

/**
 * to store a table information
 * 
 * @author caoyilin
 * @version 1.0 - 2009-9-9 created by caoyilin
 */
public class Table extends
		TableOrView {

	private static final long serialVersionUID = -2370089094517339978L;

	private PK pk = null;
	private final List<FK> fks = new ArrayList<FK>();
	private final List<Index> indexes = new ArrayList<Index>();
	private PartitionInfo partitionInfo = null;
	private long tableRowCount;
	private String createSql;
	private boolean isReuseOID = false;

	private int importedKeysCount;
	private int exportedKeysCount;

	public boolean isReuseOID() {
		return isReuseOID;
	}

	public void setReuseOID(boolean isReuseOID) {
		this.isReuseOID = isReuseOID;
	}

	public Table() {
		super();
	}

	public Table(Schema schema) {
		super();
		this.schema = schema;
	}

	/**
	 * add FK,the old FK will be removed.
	 * 
	 * @param fk FK
	 */
	public void addFK(FK fk) {
		if (fk == null) {
			throw new RuntimeException("Index can't be NULL");
		}
		if (fk.getTable() != null && !fk.getTable().equals(this)) {
			throw new RuntimeException("Index was set into a wrong table.");
		}
		FK old = getFKByName(fk.getName());
		if (old == null) {
			fks.add(fk);
			fk.setTable(this);
		}
	}

	/**
	 * add Index,If the name of index is already existed in the list, the old
	 * one will be removed.
	 * 
	 * @param index Index
	 */
	public void addIndex(Index index) {
		if (index == null) {
			throw new RuntimeException("Index can't be NULL");
		}
		if (index.getTable() != null && !index.getTable().equals(this)) {
			throw new RuntimeException("Index was set into a wrong table.");
		}
		Index old = getIndexByName(index.getName());
		if (old == null) {
			indexes.add(index);
			index.setTable(this);
		}
	}

	//	/**
	//	 * return equal flag
	//	 * 
	//	 * @param obj Object
	//	 * @return boolean
	//	 */
	//	public boolean equals(Object obj) {
	//
	//		if (this == obj) {
	//			return true;
	//		}
	//
	//		if (obj == null) {
	//			return false;
	//		}
	//
	//		if (getClass() != obj.getClass()) {
	//			return false;
	//		}
	//
	//		Table other = (Table) obj;
	//
	//		if (name == null) {
	//			if (other.name != null) {
	//				return false;
	//			}
	//		} else if (!name.equals(other.name)) {
	//			return false;
	//		}
	//
	//		return true;
	//	}

	public String getDDL() {
		return createSql;
	}

	/**
	 * get FK By fk Name
	 * 
	 * @param name String
	 * @return FK
	 */
	public FK getFKByName(String name) {
		if (name == null) {
			return null;
		}
		for (FK fk : fks) {
			if (name.equalsIgnoreCase(fk.getName())) {
				return fk;
			}
		}
		return null;
	}

	public List<FK> getFks() {
		return new ArrayList<FK>(fks);
	}

	/**
	 * get Index By Index Name
	 * 
	 * @param name String,can't be null
	 * @return Index if NULL name input
	 */
	public Index getIndexByName(String name) {
		if (name == null) {
			return null;
		}
		for (Index index : indexes) {
			if (name.equals(index.getName())) {
				return index;
			}
		}
		return null;
	}

	/**
	 * get Indexes
	 * 
	 * @return List<Index>
	 */
	public List<Index> getIndexes() {
		return new ArrayList<Index>(indexes);
	}

	/**
	 * @return object type
	 */
	public String getObjType() {
		return OBJ_TYPE_TABLE;
	}

	public PartitionInfo getPartitionInfo() {
		return partitionInfo;
	}

	public PK getPk() {
		return pk;
	}

	public long getTableRowCount() {
		return tableRowCount;
	}

	/**
	 * removeFKByName
	 * 
	 * @param fkName String
	 */
	public void removeFK(String fkName) {
		FK removefk = getFKByName(fkName);
		if (removefk != null) {
			fks.remove(removefk);
		}
	}

	/**
	 * removeIndexByName
	 * 
	 * @param indexName String
	 */
	public void removeIndex(String indexName) {
		Index removeindex = getIndexByName(indexName);
		if (removeindex != null) {
			indexes.remove(removeindex);
		}
	}

	public void setDDL(String createSql) {
		this.createSql = createSql;
	}

	public void setPartitionInfo(PartitionInfo partitionInfo) {
		this.partitionInfo = partitionInfo;
	}

	/**
	 * Set PK
	 * 
	 * @param pk primary key
	 */
	public void setPk(PK pk) {
		this.pk = pk;
		if (pk != null) {
			pk.setTable(this);
		}
	}

	/**
	 * Set table row count
	 * 
	 * @param tableRowCount long
	 */
	public void setTableRowCount(long tableRowCount) {
		if (tableRowCount < 0) {
			throw new RuntimeException("Table Row count can't be negative.");
		}
		this.tableRowCount = tableRowCount;
	}

	/**
	 * Reset the FKs
	 * 
	 * @param tfks List<FK>
	 */
	public void setFKs(List<FK> tfks) {
		if (CollectionUtils.isEmpty(tfks)) {
			this.fks.clear();
			return;
		}
		this.fks.clear();
		for (FK fk : tfks) {
			addFK(fk);
		}
	}

	/**
	 * Set indexes
	 * 
	 * @param idxs List<Index>
	 */
	public void setIndexes(List<Index> idxs) {
		if (CollectionUtils.isEmpty(idxs)) {
			this.indexes.clear();
			return;
		}
		this.indexes.clear();
		for (Index idx : idxs) {
			addIndex(idx);
		}
	}

	/**
	 * Retrieves if the table has PK or not.
	 * 
	 * @return yes means have PK.
	 */
	public boolean hasPK() {
		return pk != null && CollectionUtils.isNotEmpty(pk.getPkColumns());
	}

	/**
	 * getImportedKeysCount
	 *
	 * @return the importedKeysCount
	 */
	public int getImportedKeysCount() {
		return importedKeysCount;
	}

	/**
	 * setImportedKeysCount
	 *
	 * @param importedKeysCount
	 */
	public void setImportedKeysCount(int importedKeysCount) {
		this.importedKeysCount = importedKeysCount;
	}

	/**
	 * getExportedKeysCount
	 *
	 * @return the exportedKeysCount
	 */
	public int getExportedKeysCount() {
		return exportedKeysCount;
	}

	/**
	 * setExportedKeysCount
	 *
	 * @param exportedKeysCount
	 */
	public void setExportedKeysCount(int exportedKeysCount) {
		this.exportedKeysCount = exportedKeysCount;
	}
}
