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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.cubrid.cubridmigration.core.common.DBUtils;

/**
 * partition information
 * 
 * @author moulinwang
 */
public class PartitionInfo implements
		Cloneable,
		Serializable {
	private static final long serialVersionUID = 7470191627547681950L;

	public final static String PARTITION_METHOD_RANGE = "RANGE";
	public final static String PARTITION_METHOD_LIST = "LIST";
	public final static String PARTITION_METHOD_HASH = "HASH";
	public final static String PARTITION_METHOD_LINEARHASH = "LINEAR HASH";
	public final static String PARTITION_METHOD_KEY = "KEY";
	public final static String PARTITION_METHOD_LINEARKEY = "LINEAR KEY";

	private String partitionMethod;
	private String partitionExp;
	private String partitionFunc;
	private String ddl;
	private int partitionCount;
	private int partitionColumnCount;
	private List<Column> partitionColumns = new ArrayList<Column>();
	private List<PartitionTable> partitions = new ArrayList<PartitionTable>();

	private String subPartitionMethod;
	private String subPartitionExp;
	private String subPartitionFunc;
	private int subPartitionCount;
	private int subPartitionColumnCount;
	private List<Column> subPartitionColumns = new ArrayList<Column>();
	private List<PartitionTable> subPartitions = new ArrayList<PartitionTable>();

	private boolean boundaryValueOnRight = false;

	/**
	 * add partition
	 * 
	 * @param partition Partition
	 */
	public void addPartition(PartitionTable partition) {
		partitions.add(partition);
		partition.setPartitionIdx(partitions.size() - 1);
		this.partitionCount = partitions.size();
	}

	/**
	 * add column
	 * 
	 * @param column Column
	 */
	public void addPartitionColumn(Column column) {
		partitionColumns.add(column);
	}

	/**
	 * add subpartition
	 * 
	 * @param subpartition Partition
	 */
	public void addSubPartition(PartitionTable subpartition) {
		subPartitions.add(subpartition);
	}

	/**
	 * add column
	 * 
	 * @param column Column
	 */
	public void addSubPartitionColumn(Column column) {
		subPartitionColumns.add(column);
	}

	/**
	 * clone
	 * 
	 * @return Object
	 */
	public Object clone() {
		PartitionInfo partition;
		try {
			partition = (PartitionInfo) super.clone();
		} catch (CloneNotSupportedException e) {
			return null;
		}

		partition.setPartitions(new ArrayList<PartitionTable>(getPartitions()));
		partition.setPartitionColumns(new ArrayList<Column>(
				getPartitionColumns()));
		partition.setSubPartitionColumns(new ArrayList<Column>(
				getSubPartitionColumns()));

		return partition;
	}

	public String getDDL() {
		return ddl;
	}

	public int getPartitionColumnCount() {
		return partitionColumnCount;
	}

	public List<Column> getPartitionColumns() {
		return partitionColumns;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public String getPartitionExp() {
		return partitionExp;
	}

	public String getPartitionFunc() {
		return partitionFunc;
	}

	public String getPartitionMethod() {
		return partitionMethod;
	}

	public List<PartitionTable> getPartitions() {
		return partitions;
	}

	/**
	 * get partition table by name
	 * 
	 * @param partitionName String
	 * @return PartitionTable
	 */
	public PartitionTable getPartitionTableByName(String partitionName) {
		for (PartitionTable p : partitions) {
			if (partitionName.equals(p.getPartitionName())) {
				return p;
			}
		}

		return null;
	}

	/**
	 * get partition table by position
	 * 
	 * @param positionIndex int
	 * @return PartitionTable
	 */
	public PartitionTable getPartitionTableByPosition(int positionIndex) {
		for (PartitionTable p : partitions) {
			if (positionIndex == p.getPartitionIdx()) {
				return p;
			}
		}

		return null;
	}

	public int getSubPartitionColumnCount() {
		return subPartitionColumnCount;
	}

	public List<Column> getSubPartitionColumns() {
		return subPartitionColumns;
	}

	public int getSubPartitionCount() {
		return subPartitionCount;
	}

	public String getSubPartitionExp() {
		return subPartitionExp;
	}

	public String getSubPartitionFunc() {
		return subPartitionFunc;
	}

	public String getSubPartitionMethod() {
		return subPartitionMethod;
	}

	/**
	 * getPartitionNameByIdx
	 * 
	 * @param partitionIdx int
	 * @return partitionName
	 */
	public String getSubPartitionNameByIdx(int partitionIdx) {
		for (PartitionTable subPartition : subPartitions) {
			if (subPartition.getPartitionIdx() == partitionIdx) {
				return subPartition.getPartitionName();
			}
		}

		return null;
	}

	public List<PartitionTable> getSubPartitions() {
		return subPartitions;
	}

	public boolean isBoundaryValueOnRight() {
		return boundaryValueOnRight;
	}

	public void setBoundaryValueOnRight(boolean boundaryValueOnRight) {
		this.boundaryValueOnRight = boundaryValueOnRight;
	}

	public void setDDL(String partitionDDL) {
		this.ddl = partitionDDL;
	}

	public void setPartitionColumnCount(int partitionColumnCount) {
		this.partitionColumnCount = partitionColumnCount;
	}

	public void setPartitionColumns(List<Column> partionColumns) {
		this.partitionColumns = partionColumns;
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}

	/**
	 * setPartitionExp
	 * 
	 * @param partitionExp String
	 */
	public void setPartitionExp(String partitionExp) {
		this.partitionExp = partitionExp;
		this.partitionFunc = DBUtils.parsePartitionFunc(partitionExp);
	}

	public void setPartitionFunc(String partitionFunc) {
		this.partitionFunc = partitionFunc;
	}

	public void setPartitionMethod(String partitionMethod) {
		this.partitionMethod = partitionMethod;
	}

	public void setPartitions(List<PartitionTable> partitions) {
		this.partitions = partitions;
	}

	public void setSubPartitionColumnCount(int subPartitionColumnCount) {
		this.subPartitionColumnCount = subPartitionColumnCount;
	}

	public void setSubPartitionColumns(List<Column> subPartionColumns) {
		this.subPartitionColumns = subPartionColumns;
	}

	public void setSubPartitionCount(int subPartitionCount) {
		this.subPartitionCount = subPartitionCount;
	}

	public void setSubPartitionExp(String subPartitionExp) {
		this.subPartitionExp = subPartitionExp;
	}

	public void setSubPartitionFunc(String subPartitionFunc) {
		this.subPartitionFunc = subPartitionFunc;
	}

	public void setSubPartitionMethod(String subPartitionMethod) {
		this.subPartitionMethod = subPartitionMethod;
	}

	public void setSubPartitions(List<PartitionTable> subPartitions) {
		this.subPartitions = subPartitions;
	}
}
