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

import java.sql.Types;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.oracle.OracleDataTypeHelper;

public class OracleDataTypeHelperTest {
	OracleDataTypeHelper helper = OracleDataTypeHelper.getInstance(null);

	@Test
	public void testOracleDataTypeHelper() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getOracleConfig();

		Assert.assertEquals(new Integer(Types.VARCHAR),
				helper.getJdbcDataTypeID(config.getSrcCatalog(), "VARCHAR2",
						4000, null));

		Assert.assertEquals(new Integer(Types.FLOAT), helper.getJdbcDataTypeID(
				config.getSrcCatalog(), "BINARY_FLOAT", 4000, null));

		Assert.assertEquals(new Integer(Types.FLOAT), helper.getJdbcDataTypeID(
				config.getSrcCatalog(), "BINARY_FLOAT", 4000, null));

		Assert.assertEquals(new Integer(Types.DOUBLE),
				helper.getJdbcDataTypeID(config.getSrcCatalog(),
						"BINARY_DOUBLE", null, null));

		Assert.assertNull(helper.getJdbcDataTypeID(config.getSrcCatalog(),
				"BFILE", null, null));
		Assert.assertNull(helper.getJdbcDataTypeID(config.getSrcCatalog(),
				"ROWID", null, null));
		Assert.assertNull(helper.getJdbcDataTypeID(config.getSrcCatalog(),
				"UROWID", null, null));

		Assert.assertEquals(new Integer(Types.BIGINT),
				helper.getJdbcDataTypeID(config.getSrcCatalog(), "NUMBER",
						null, 0));
		Assert.assertEquals(new Integer(Types.NUMERIC),
				helper.getJdbcDataTypeID(config.getSrcCatalog(), "NUMBER",
						null, null));

		Assert.assertEquals(new Integer(Types.NUMERIC),
				helper.getJdbcDataTypeID(config.getSrcCatalog(), "NUMBER", 38,
						2));
		Assert.assertEquals(
				new Integer(Types.BIT),
				helper.getJdbcDataTypeID(config.getSrcCatalog(), "NUMBER", 1, 0));
		Assert.assertEquals(
				new Integer(Types.TINYINT),
				helper.getJdbcDataTypeID(config.getSrcCatalog(), "NUMBER", 3, 0));
		Assert.assertEquals(
				new Integer(Types.SMALLINT),
				helper.getJdbcDataTypeID(config.getSrcCatalog(), "NUMBER", 5, 0));
		Assert.assertEquals(new Integer(Types.INTEGER),
				helper.getJdbcDataTypeID(config.getSrcCatalog(), "NUMBER", 10,
						null));
		Assert.assertEquals(new Integer(Types.BIGINT),
				helper.getJdbcDataTypeID(config.getSrcCatalog(), "NUMBER", 38,
						null));

		/////////////////getShownDataType
		Column column = new Column();
		column.setName("f1");
		column.setDataType("VARCHAR2");
		column.setPrecision(4000);

		Assert.assertEquals("VARCHAR2(4000)", helper.getShownDataType(column));
		column.setDataType("CHAR");
		column.setPrecision(128);
		Assert.assertEquals("CHAR(128)", helper.getShownDataType(column));

		column.setDataType("NUMBER");
		column.setPrecision(null);
		column.setScale(null);
		Assert.assertEquals("NUMBER", helper.getShownDataType(column));
		column.setPrecision(0);
		column.setScale(null);
		Assert.assertEquals("NUMBER", helper.getShownDataType(column));

		column.setPrecision(38);
		column.setScale(null);
		Assert.assertEquals("NUMBER(38,0)", helper.getShownDataType(column));

		column.setPrecision(38);
		column.setScale(2);
		Assert.assertEquals("NUMBER(38,2)", helper.getShownDataType(column));

		/////////////////getOracleDataTypeKey
		Assert.assertEquals("TIMESTAMP",
				OracleDataTypeHelper.getOracleDataTypeKey("TIMESTAMP(38)"));
		Assert.assertEquals(
				"TIMESTAMP WITH TIME ZONE",
				OracleDataTypeHelper.getOracleDataTypeKey("TIMESTAMP(38) WITH TIME ZONE"));
		Assert.assertEquals(
				"TIMESTAMP WITH LOCAL TIME ZONE",
				OracleDataTypeHelper.getOracleDataTypeKey("TIMESTAMP(38) WITH LOCAL TIME ZONE"));
		Assert.assertEquals(
				"INTERVALDS",
				OracleDataTypeHelper.getOracleDataTypeKey("INTERVAL DAY(2) TO SECOND(10)"));
		Assert.assertEquals(
				"INTERVALYM",
				OracleDataTypeHelper.getOracleDataTypeKey("INTERVAL YEAR(2012) TO MONTH"));
		Assert.assertEquals("NUMBER",
				OracleDataTypeHelper.getOracleDataTypeKey("NUMBER"));
	}
}
