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
 * Import CSV Event Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-03-4 created by Kevin Cao
 */
public class ImportCSVEvent extends
		MigrationEvent implements
		IMigrateDataErrorEvent {

	private final SourceCSVConfig csv;
	private final int recordCount;
	private final long size;

	private final boolean success;

	private final Throwable error;
	private final String errorFile;

	public ImportCSVEvent(SourceCSVConfig csv, int recordCount, long size) {
		this.csv = csv;
		this.recordCount = recordCount;
		this.success = true;
		this.error = null;
		this.size = size;
		this.errorFile = null;
	}

	public ImportCSVEvent(SourceCSVConfig csv, int recordCount, long size,
			Throwable error, String errorFile) {
		this.csv = csv;
		this.recordCount = recordCount;
		this.success = false;
		this.error = error;
		this.size = size;
		this.errorFile = errorFile;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public long getSize() {
		return size;
	}

	public SourceCSVConfig getCsv() {
		return csv;
	}

	public boolean isSuccess() {
		return success;
	}

	public Throwable getError() {
		return error;
	}

	/**
	 * To String
	 * 
	 * @return String
	 */
	public String toString() {
		if (recordCount == 0) {
			return "No data imported in [" + csv + "].";
		}
		StringBuffer sb = new StringBuffer();
		sb.append("Imported ").append(recordCount).append(" row(s) into [").append(
				csv.getTarget()).append("]");
		if (success) {
			return sb.append(" successfully.").toString();
		} else {
			return sb.append(" unsuccessfully. Error:").append(error).toString();
		}
	}

	public String getErrorFile() {
		return errorFile;
	}

	/**
	 * The event's importance level
	 * 
	 * @return level
	 */
	public int getLevel() {
		return success ? 2 : 1;
	}
}
