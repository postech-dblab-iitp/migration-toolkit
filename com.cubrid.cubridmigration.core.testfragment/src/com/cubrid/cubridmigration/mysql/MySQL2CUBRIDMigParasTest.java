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
package com.cubrid.cubridmigration.mysql;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.TimeZone;

import org.junit.Test;

import com.cubrid.cubridmigration.tibero.trans.MigrationParameters;

public class MySQL2CUBRIDMigParasTest {

	@Test
	public void testMySQL2CUBRIDMigParas() throws Exception {
		//branch 1st
		//branch 2nd.
		InputStream is = MySQL2CUBRIDMigParasTest.class.getResourceAsStream("/com/cubrid/cubridmigration/mysql/MySQL2CUBRIDParams.xml");
		InputStreamReader reader = new InputStreamReader(is, "utf-8");
		char[] buf = new char[1];
		StringBuffer sb = new StringBuffer();
		while (reader.read(buf) > 0) {
			sb.append(buf[0]);
		}
		MigrationParameters.loadFromPreference(null);
		MigrationParameters.loadFromPreference(sb.toString());
		MigrationParameters.getReplacedDate(null, null);
		MigrationParameters.getReplacedDate("2011-12-12", null);
		MigrationParameters.getReplacedDate("2011-00-00", null);
		MigrationParameters.getReplacedTime(null, null);
		MigrationParameters.getReplacedTime("12:12:12", TimeZone.getDefault());
		MigrationParameters.getReplacedTime("12:12:61", TimeZone.getDefault());
		MigrationParameters.getReplacedTimestamp(null, TimeZone.getDefault());
		MigrationParameters.getReplacedTimestamp("2011-12-12 12:12:12",
				TimeZone.getDefault());
		MigrationParameters.getReplacedTimestamp("2011-12-12 12:12:12.111",
				TimeZone.getDefault());
	}
}
