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
package com.cubrid.cubridmigration.cubrid.export.handler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.TestUtil2;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.cubrid.export.handler.CUBRIDSetTypeHandler;

/**
 * 
 * Test Class CUBRIDSetTypeHandler
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-3-9 created by Kevin Cao
 */
public class CUBRIDSetTypeHandlerTest {

	@Test
	public void testGetObject() throws Exception {
		CUBRIDSetTypeHandler handler = new CUBRIDSetTypeHandler();
		Connection con = TestUtil2.getCUBRIDConn();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select * from test_set");
			Column column1 = new Column();
			column1.setName("f1");
			Column column2 = new Column();
			column1.setName("f2");
			Column column3 = new Column();
			column1.setName("f3");
			List<Object> result = new ArrayList<Object>();
			while (rs.next()) {
				column1.setName("f1");
				result.add(handler.getJdbcObject(rs, column1));
				column2.setName("f2");
				result.add(handler.getJdbcObject(rs, column2));
				column3.setName("f3");
				result.add(handler.getJdbcObject(rs, column3));
			}
			Assert.assertFalse(result.isEmpty());
		} finally {
			con.close();
		}
	}
}
