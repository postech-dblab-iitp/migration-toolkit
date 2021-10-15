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

import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.dbobject.View;

/**
 * CreateObjectFailEvent Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-11 created by Kevin Cao
 */
public class CreateObjectEvent extends
		MigrationEvent implements
		IMigrationErrorEvent {

	private Throwable error;

	protected final DBObject dbObject;

	private final boolean isSuccess;

	public DBObject getDbObject() {
		return dbObject;
	}

	/**
	 * Create an instance with success flag
	 * 
	 * @param dbObject DBObject
	 * @param error Throwable
	 */
	public CreateObjectEvent(DBObject dbObject) {
		this.dbObject = dbObject;
		isSuccess = true;
	}

	/**
	 * Create an instance with failure flag
	 * 
	 * @param dbObject DBObject
	 * @param error Throwable
	 */
	public CreateObjectEvent(DBObject dbObject, Throwable error) {
		this.dbObject = dbObject;
		this.error = error;
		isSuccess = false;
	}

	/**
	 * To String
	 * 
	 * @return String
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer();
		if (dbObject instanceof Table) {
			sb.append("table[").append(dbObject.getName()).append("]");
		} else if (dbObject instanceof PK) {
			sb.append("primary key[").append(((PK) dbObject).getTable().getName()).append("]");
		} else if (dbObject instanceof FK) {
			sb.append("foreign key[").append(dbObject.getName()).append("]");
		} else if (dbObject instanceof Index) {
			sb.append("index[").append(dbObject.getName()).append("]");
		} else if (dbObject instanceof Procedure) {
			sb.append("procedure[").append(dbObject.getName()).append("]");
		} else if (dbObject instanceof Function) {
			sb.append("function[").append(dbObject.getName()).append("]");
		} else if (dbObject instanceof Trigger) {
			sb.append("trigger[").append(dbObject.getName()).append("]");
		} else if (dbObject instanceof View) {
			sb.append("view[").append(dbObject.getName()).append("]");
		} else if (dbObject instanceof Sequence) {
			sb.append("sequence[").append(dbObject.getName()).append("]");
		}
		if (error != null) {
			return "Create " + sb.toString() + " unsuccessfully." + " Detail:" + error.getMessage();
		}
		return "Create " + sb.toString() + " successfully.";
	}

	/**
	 * Get error
	 * 
	 * @return error
	 */
	public Throwable getError() {
		return error;
	}

	/**
	 * The event's importance level
	 * 
	 * @return level
	 */
	public int getLevel() {
		return isSuccess ? 2 : 1;
	}

	public boolean isSuccess() {
		return isSuccess;
	}
}
