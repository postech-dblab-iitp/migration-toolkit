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
package com.cubrid.cubridmigration.cubrid.export;

import java.math.BigInteger;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;

public class CUBRIDExportHelperTest {

	@Test
	public void testCUBRIDExportHelper() {
		CUBRIDExportHelper helper = new CUBRIDExportHelper();
		//helper.getTestSelectSQL("select * from code");

		Assert.assertEquals("select * from code WHERE ROWNUM  BETWEEN 1001 AND 2000",
				helper.getPagedSelectSQL("select * from code", 1000, 1000, null));
		Assert.assertEquals(
				"select * from code order by f1 FOR ORDERBY_NUM()  BETWEEN 1001 AND 2000",
				helper.getPagedSelectSQL("select * from code order by f1", 1000, 1000, null));
		Assert.assertEquals(
				"select * from code group by f1 HAVING  GROUPBY_NUM()  BETWEEN 1001 AND 2000",
				helper.getPagedSelectSQL("select * from code group by f1", 1000, 1000, null));
		Assert.assertEquals(
				"select * from code group by f1 having f1=1 AND  GROUPBY_NUM()  BETWEEN 1001 AND 2000",
				helper.getPagedSelectSQL("select * from code group by f1 having f1=1", 1000, 1000,
						null));
		Assert.assertEquals("select * from code where 1=1 AND ROWNUM  BETWEEN 1001 AND 2000",
				helper.getPagedSelectSQL("select * from code where 1=1 ", 1000, 1000, null));
	}

	@Test
	public void testGetSelectCountSQL() {
		CUBRIDExportHelper helper = new CUBRIDExportHelper();
		SourceEntryTableConfig setc = new SourceEntryTableConfig();
		setc.addColumnConfig("f1", "f1", true);
		setc.setName("t1");
		setc.setTarget("t1");
		setc.setOwner("o1");
		setc.setCondition("where f1=1");
		Assert.assertEquals(helper.getSelectCountSQL(setc),
				"SELECT COUNT(1)  FROM \"t1\" where f1=1");

		setc.setOwner("");
		setc.setCondition("f1=1");
		Assert.assertEquals(helper.getSelectCountSQL(setc),
				"SELECT COUNT(1)  FROM \"t1\" WHERE  f1=1");

		setc.setOwner("o1");
		setc.setCondition("where f1=1;");
		Assert.assertEquals(helper.getSelectCountSQL(setc),
				"SELECT COUNT(1)  FROM \"t1\" where f1=1");
	}

	@Test
	public void testFillTablesRowCount() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getCubridConfig();
		CUBRIDExportHelper helper = new CUBRIDExportHelper();
		config.setImplicitEstimate(true);
		helper.fillTablesRowCount(config);
		final Table table = config.getSrcCatalog().getSchemas().get(0).getTables().get(0);
		Assert.assertTrue(table.getTableRowCount() == 0);
		config.setImplicitEstimate(false);
		helper.fillTablesRowCount(config);
		Assert.assertTrue(table.getTableRowCount() > 0);

		config.setSourceType(MigrationConfiguration.SQL);
		helper.fillTablesRowCount(config);
	}

	@Test
	public void testGetSerialCurrentValue() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getCubridConfig();
		CUBRIDExportHelper helper = new CUBRIDExportHelper();
		SourceSequenceConfig sq = config.getExpSerialCfg().get(0);
		Assert.assertEquals(new BigInteger("1"),
				helper.getSerialStartValue(config.getSourceConParams(), sq));
	}
}
