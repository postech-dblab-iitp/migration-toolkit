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
package com.cubrid.cubridmigration.mysql.dbobj;

import com.cubrid.cubridmigration.core.dbobject.Trigger;


/**
 * MySQL trigger
 * 
 * @author moulinwang
 * @version 1.0 - 2009-11-11
 */
public class MySQLTrigger extends
		Trigger {
	private static final long serialVersionUID = -6309951867739303359L;
	private String eventTable;
	private String actionTiming;
	private String eventManipulation;
	private String actionStmt;

	public String getEventTable() {
		return eventTable;
	}

	public void setEventTable(String eventTable) {
		this.eventTable = eventTable;
	}

	public String getActionTiming() {
		return actionTiming;
	}

	public void setActionTiming(String actionTiming) {
		this.actionTiming = actionTiming;
	}

	public String getEventManipulation() {
		return eventManipulation;
	}

	public void setEventManipulation(String eventManipulation) {
		this.eventManipulation = eventManipulation;
	}

	public String getActionStmt() {
		return actionStmt;
	}

	public void setActionStmt(String actionStmt) {
		this.actionStmt = actionStmt;
	}

	/**
	 * get a trigger's DDL
	 * 
	 * @return String
	 */
	public String getDDL() {

		if (null == triggerDDL) {
			StringBuffer buf = new StringBuffer();
			buf.append("CREATE TRIGGER `");
			buf.append(getName()).append("` ");
			buf.append(getActionTiming()).append(" ");
			buf.append(getEventManipulation()).append(" ON ");
			buf.append(getEventTable()).append("\r\n");

			buf.append("  FOR EACH ROW BEGIN\r\n  ");
			buf.append(getActionStmt());

			this.triggerDDL = buf.toString();
		}

		return triggerDDL;
	}
}
