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
package com.cubrid.cubridmigration.oracle;

import com.cubrid.cubridmigration.core.sql.SQLHelper;

/**
 * 
 * OracleSQLHelper
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-10-25
 */
public class OracleSQLHelper extends
		SQLHelper {

	private static final OracleSQLHelper INS = new OracleSQLHelper();

	/**
	 * Singleton factory.
	 * 
	 * @param version MSSQL server version
	 * @return MSSQLDDLUtil
	 */
	public static OracleSQLHelper getInstance(String version) {
		return INS;
	}

	private OracleSQLHelper() {
		//Hide the constructor for singleton
	}

	/**
	 * append "rownum = 0" to SELECT statement
	 * 
	 * @param sql SELECT statement
	 * @return String
	 */
	public String getTestSelectSQL(String sql) {
		return "SELECT * FROM ( " + sql + " ) WHERE 1<>1";
	}

	/**
	 * return database object name
	 * 
	 * @param objectName String
	 * @return String
	 */
	public String getQuotedObjName(String objectName) {
		return new StringBuffer("\"").append(objectName).append("\"").toString();
	}
}
