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
 * DBObjMigrationResult
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-7 created by Kevin Cao
 */
public class DBObjMigrationResult implements
		Serializable {

	private static final long serialVersionUID = 7283320978882945903L;
	private String owner;

	private String objName;

	private String objType;

	private boolean succeed = false;
	private String ddl;
	private String error;
	
	private String sourceObj;
	
	public String getSourceObj() {
		return sourceObj;
	}

	public void setSourceObj(String sourceObj) {
		this.sourceObj = sourceObj;
	}

	public String getDdl() {
		return ddl;
	}

	public String getError() {
		return error;
	}

	public String getObjName() {
		return objName;
	}

	public String getObjType() {
		return objType;
	}

	public String getOwner() {
		return owner;
	}

	public boolean isSucceed() {
		return succeed;
	}

	public void setDdl(String ddl) {
		this.ddl = ddl;
	}

	public void setError(String error) {
		this.error = error;
	}

	public void setObjName(String objName) {
		this.objName = objName;
	}

	public void setObjType(String objType) {
		this.objType = objType;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public void setSucceed(boolean succeed) {
		this.succeed = succeed;
	}

}
