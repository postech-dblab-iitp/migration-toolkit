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
package com.cubrid.cubridmigration.core.engine.event;

import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;

/**
 * 
 * ExportCSVEvent Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-5-28 created by Kevin Cao
 */
public class ExportCSVEvent extends
		MigrationEvent {

	private final SourceCSVConfig sourceTable;
	private final int recordCount;

	public ExportCSVEvent(SourceCSVConfig tt, int recordCount) {
		sourceTable = tt;
		this.recordCount = recordCount;
	}

	public SourceCSVConfig getSourceCSV() {
		return sourceTable;
	}

	public int getRecordCount() {
		return recordCount;
	}

	/**
	 * To String
	 * 
	 * @return String
	 */
	public String toString() {
		if (recordCount == 0) {
			return "No record in the CSV file [" + sourceTable.getName() + "].";
		}
		return new StringBuffer().append("Exported ").append(recordCount).append(
				" records from CSV file [").append(sourceTable.getName()).append(
				"] successfully.").toString();
	}

	/**
	 * The event's importance level
	 * 
	 * @return level
	 */
	public int getLevel() {
		return 2;
	}
}
