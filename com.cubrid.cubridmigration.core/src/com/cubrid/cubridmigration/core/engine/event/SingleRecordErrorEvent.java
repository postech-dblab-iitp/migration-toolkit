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

import java.util.List;

import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.dbobject.TableOrView;

/**
 * 
 * If transform a record failed, the error event will be fired.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-30 created by Kevin Cao
 */
public class SingleRecordErrorEvent extends
		MigrationEvent {

	private final Record record;
	private final Throwable error;

	public SingleRecordErrorEvent(Record record, Throwable error) {
		this.record = record;
		this.error = error;
	}

	public Throwable getError() {
		return error;
	}

	/**
	 * To String
	 * 
	 * @return event string
	 */
	public String toString() {
		List<ColumnValue> columnValueList = record.getColumnValueList();
		if (columnValueList.isEmpty()) {
			return "No columns table.";
		}
		StringBuffer sb = new StringBuffer();
		TableOrView tb = columnValueList.get(0).getColumn().getTableOrView();
		if (tb != null) {
			sb.append("[").append(tb.getName()).append("]");
		}
		sb.append("Error:").append(error.getMessage()).append(";Values:[");
		int flag = 0;
		for (ColumnValue cv : columnValueList) {
			if (flag > 0) {
				sb.append(",");
			}
			sb.append(cv.getColumn().getName()).append(":").append(
					cv.getValue());
			flag++;
		}
		sb.append("]");
		return sb.toString();
	}

	/**
	 * The event's importance level
	 * 
	 * @return level
	 */
	public int getLevel() {
		return 3;
	}
}
