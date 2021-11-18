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
package com.cubrid.cubridmigration.core.sql;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.common.json.ReadFile;

/**
 * 
 * SQLHelperTest
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-11
 */
public class SQLHelperTest {
	private SQLHelper sqlHelper = new SQLHelper() {

		@Override
		public String getTestSelectSQL(String sql) {
			return sql;
		}

		@Override
		public String getQuotedObjName(String objectName) {
			return objectName;
		}
	};

	/**
	 * test GetQuerySpec
	 * 
	 */
	@Test
	public void testGetQuerySpec() {
		String viewDDL1 = "CREATE ALGORITHM=UNDEFINED DEFINER=`mydbadmin`@`192.168.1.175` SQL SECURITY DEFINER VIEW `tgt_view` AS "
				+ "select `tgt`.`d` AS `d`,`tgt`.`ff` AS `ff`,`tgt`.`aa` AS `aa` from `tgt` ";
		String querySpec1 = sqlHelper.getViewQuerySpec(viewDDL1);
		Assert.assertNotNull(querySpec1);

		String querySpec2 = sqlHelper.getViewQuerySpec("ss");
		Assert.assertEquals("", querySpec2);
		Assert.assertEquals(
				"select `tgt`.`d` AS `d`,`tgt`.`ff` AS `ff`,`tgt`.`aa` AS `aa` from `tgt` ",
				querySpec1);
		String viewDDL2 = "CREATE ALGORITHM=UNDEFINED DEFINER=`mydbadmin`@`192.168.1.34` SQL SECURITY DEFINER VIEW `tgtview` AS "
				+ "select `tgt`.`d` AS `d`,`tgt`.`gsdf` AS `gsdf`,`tgt`.`ff` AS `ff`,`tgt`.`aa` AS `aa`,`tgt`.`t1` AS `t1` from `tgt`";
		String querySpec3 = sqlHelper.getViewQuerySpec(viewDDL2);
		Assert.assertNotNull(querySpec3);
		Assert.assertEquals(
				"select `tgt`.`d` AS `d`,`tgt`.`gsdf` AS `gsdf`,`tgt`.`ff` AS `ff`,`tgt`.`aa` AS `aa`,`tgt`.`t1` AS `t1` from `tgt`",
				querySpec3);

		viewDDL2 = "create view [tgtview(c1,c2)] AS "
				+ "select `tgt`.`d` AS `d`,`tgt`.`gsdf` AS `gsdf`,`tgt`.`ff` AS `ff`,`tgt`.`aa` AS `aa`,`tgt`.`t1` AS `t1` from `tgt`";
		querySpec3 = sqlHelper.getViewQuerySpec(viewDDL2);
		Assert.assertNotNull(querySpec3);
		Assert.assertEquals(
				"select `tgt`.`d` AS `d`,`tgt`.`gsdf` AS `gsdf`,`tgt`.`ff` AS `ff`,`tgt`.`aa` AS `aa`,`tgt`.`t1` AS `t1` from `tgt`",
				querySpec3);

	}

	@Test
	public void testGetQuerySpec2() throws IOException {
		String path = System.getProperty("user.dir")
				+ "/src/com/cubrid/cubridmigration/core/dbmetadata/";
		String viewDDL = ReadFile.readFile(path + "view.txt");

		System.out.println(sqlHelper.getViewQuerySpec(viewDDL));

	}

}
