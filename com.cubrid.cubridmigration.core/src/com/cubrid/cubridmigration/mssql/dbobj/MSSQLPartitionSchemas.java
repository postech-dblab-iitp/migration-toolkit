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
package com.cubrid.cubridmigration.mssql.dbobj;

import java.util.List;

/**
 * PartitionSchemas Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-11-13 created by Kevin Cao
 */
public class MSSQLPartitionSchemas {

	private boolean boundaryValueOnRight;

	private Long dataSpaceId;

	private long functionId;

	private String name;

	private int parameterId;

	private int partitionCount; //[fanout]

	private List<String> partitionRangeValues;

	private String partitionType;

	private String systemType;

	public boolean getBoundaryValueOnRight() {
		return boundaryValueOnRight;
	}

	public Long getDataSpaceId() {
		return dataSpaceId;
	}

	public long getFunctionId() {
		return functionId;
	}

	public String getName() {
		return name;
	}

	public int getParameterId() {
		return parameterId;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public List<String> getPartitionRangeValues() {
		return partitionRangeValues;
	}

	public String getPartitionType() {
		return partitionType;
	}

	public String getSystemType() {
		return systemType;
	}

	public void setBoundaryValueOnRight(boolean boundaryValueOnRight) {
		this.boundaryValueOnRight = boundaryValueOnRight;
	}

	public void setDataSpaceId(Long dataSpaceId) {
		this.dataSpaceId = dataSpaceId;
	}

	public void setFunctionId(long functionId) {
		this.functionId = functionId;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setParameterId(int parameterId) {
		this.parameterId = parameterId;
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}

	public void setPartitionRangeValues(List<String> partitionRangeValues) {
		this.partitionRangeValues = partitionRangeValues;
	}

	public void setPartitionType(String partitionType) {
		this.partitionType = partitionType;
	}

	public void setSystemType(String systemType) {
		this.systemType = systemType;
	}

}
