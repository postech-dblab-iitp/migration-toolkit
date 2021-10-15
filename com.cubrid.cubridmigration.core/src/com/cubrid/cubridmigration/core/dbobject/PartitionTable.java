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

/**
 * 
 * Partition
 * 
 * @author JessieHuang
 * @version 1.0 - 2010-02-03 created by JessieHuang
 */
public class PartitionTable extends
		DBObject {

	private static final long serialVersionUID = 7399169268253338903L;

	private int partitionIdx;
	private String partitionName;
	private String partitionDesc; //maxValue

	/**
	 * Retrieves the name
	 * 
	 * @return String
	 */
	public String getName() {
		return partitionName;
	}

	public String getPartitionName() {
		return partitionName;
	}

	public void setPartitionName(String partitionName) {
		this.partitionName = partitionName;
	}

	public int getPartitionIdx() {
		return partitionIdx;
	}

	public void setPartitionIdx(int partitionIdx) {
		this.partitionIdx = partitionIdx;
	}

	public String getPartitionDesc() {
		return partitionDesc;
	}

	public void setPartitionDesc(String partitionDesc) {
		this.partitionDesc = partitionDesc;
	}

	/**
	 * @return object type
	 */
	public String getObjType() {
		return OBJ_TYPE_PARTITION;
	}

	/**
	 * DDL
	 * 
	 * @return DDL
	 */
	public String getDDL() {
		return "";
	}
}