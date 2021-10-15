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

import java.util.Locale;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.mysql.export.MySQLExportHelper;

public class MySQLExportHelperTest {

	//	@Test
	//	public void testIsWhereNeeded() {
	//		String whereCnd = "limit 0";
	//		boolean isNeeded = new MySQLExportHelper().isWhereNeeded(whereCnd);
	//		Assert.assertEquals(false, isNeeded);
	//
	//		whereCnd = "where 1=1";
	//		isNeeded = new MySQLExportHelper().isWhereNeeded(whereCnd);
	//		Assert.assertEquals(false, isNeeded);
	//
	//		whereCnd = "for 1=1";
	//		isNeeded = new MySQLExportHelper().isWhereNeeded(whereCnd);
	//		Assert.assertEquals(false, isNeeded);
	//
	//		whereCnd = "having 1=1";
	//		isNeeded = new MySQLExportHelper().isWhereNeeded(whereCnd);
	//		Assert.assertEquals(false, isNeeded);
	//
	//		whereCnd = "group by";
	//		isNeeded = new MySQLExportHelper().isWhereNeeded(whereCnd);
	//		Assert.assertEquals(false, isNeeded);
	//
	//		whereCnd = "order by";
	//		isNeeded = new MySQLExportHelper().isWhereNeeded(whereCnd);
	//		Assert.assertEquals(false, isNeeded);
	//
	//		whereCnd = "PROCEDURE 1-2";
	//		isNeeded = new MySQLExportHelper().isWhereNeeded(whereCnd);
	//		Assert.assertEquals(false, isNeeded);
	//
	//		whereCnd = "1=1 ";
	//		isNeeded = new MySQLExportHelper().isWhereNeeded(whereCnd);
	//		Assert.assertEquals(true, isNeeded);
	//	}

	@Test
	public void testGetAppendLimit0() {
		//String sql = "select * from test limit 100;";
		//String expected = "select * from test LIMIT 1";
		//String returnSql = new MySQLExportHelper().getTestSelectSQL(sql);
		//Assert.assertEquals(expected, returnSql);

		//sql = "select * from test";
		//expected = "select * from test LIMIT 1";
		//returnSql = new MySQLExportHelper().getTestSelectSQL(sql);
		//Assert.assertEquals(expected, returnSql);
	}

	@Test
	public void testMatchMySQLLimit() {
		MySQLExportHelper r = new MySQLExportHelper();
		Assert.assertEquals(true, r.matchMySQLLimit(" LIMIT 10 "));
		Assert.assertEquals(true, r.matchMySQLLimit(" LIMIT 10 , 12"));
		Assert.assertEquals(false, r.matchMySQLLimit(" LIMIT 10 12"));
		Assert.assertEquals(true, r.matchMySQLLimit(" LIMIT 10 OFFSET 12"));
	}

	@Test
	public void testReplaceWithMySQLLimit0() {
		MySQLExportHelper r = new MySQLExportHelper();
		Assert.assertEquals(" LIMIT 0 ss",
				r.replaceWithMySQLLimit0(" LIMIT 10 ss"));
		Assert.assertEquals(" LIMIT 0 dd",
				r.replaceWithMySQLLimit0(" LIMIT 10 , 12 dd"));
		Assert.assertEquals(" LIMIT 10 12",
				r.replaceWithMySQLLimit0(" LIMIT 10 12"));
		Assert.assertEquals(" LIMIT 0 ",
				r.replaceWithMySQLLimit0(" LIMIT 10 OFFSET 12"));
		Assert.assertEquals(" LIMIT ", r.replaceWithMySQLLimit0(" LIMIT "));

	}

	@Test
	public void testLimit() {
		String sqlFilterPart = " limit";
		boolean match = sqlFilterPart.toLowerCase(Locale.ENGLISH).matches(
				".*?\\s+limit\\s*.*");
		Assert.assertEquals(true, match);
	}

	@Test
	public void testFillTablesRowCount() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getMySQLConfig();
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
	}
}
