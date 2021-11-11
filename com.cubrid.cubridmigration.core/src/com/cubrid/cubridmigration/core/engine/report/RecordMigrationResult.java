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
package com.cubrid.cubridmigration.core.engine.report;

import java.io.Serializable;

/**
 * 
 * RecordMigrationResult Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-7 created by Kevin Cao
 */
public class RecordMigrationResult implements
		Serializable {

	private static final long serialVersionUID = -341299780722494648L;
	private String srcSchema;
	private String source;
	private String target;
	private long startExportTime;
	private long endExportTime;

	private long startImportTime;
	private long endImportTime;

	private long totalCount;
	private long expCount;
	private long impCount;

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public long getStartExportTime() {
		return startExportTime;
	}

	public void setStartExportTime(long startExportTime) {
		this.startExportTime = startExportTime;
	}

	public long getEndExportTime() {
		return endExportTime;
	}

	public void setEndExportTime(long endExportTime) {
		this.endExportTime = endExportTime;
	}

	public long getStartImportTime() {
		return startImportTime;
	}

	public void setStartImportTime(long startImportTime) {
		this.startImportTime = startImportTime;
	}

	public long getEndImportTime() {
		return endImportTime;
	}

	public void setEndImportTime(long endImportTime) {
		this.endImportTime = endImportTime;
	}

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	public long getExpCount() {
		return expCount;
	}

	public void setExpCount(long expCount) {
		this.expCount = expCount;
	}

	public long getImpCount() {
		return impCount;
	}

	public void setImpCount(long impCount) {
		this.impCount = impCount;
	}

	public String getSrcSchema() {
		return srcSchema;
	}

	public void setSrcSchema(String srcSchema) {
		this.srcSchema = srcSchema;
	}

	/**
	 * @return true if the migration has error
	 */
	public boolean isDataMigrationHasError() {
		return getTotalCount() != getExpCount() || getExpCount() != getImpCount();
	}
}
