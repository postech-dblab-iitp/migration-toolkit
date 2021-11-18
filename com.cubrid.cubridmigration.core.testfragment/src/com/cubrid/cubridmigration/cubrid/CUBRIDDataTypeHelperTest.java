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
package com.cubrid.cubridmigration.cubrid;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.dbobject.Column;

/**
 * 
 * CUBRIDDataTypeTest
 * 
 * @author moulinwang Kevin Cao
 * @version 1.0 - 2010-10-4
 */
public class CUBRIDDataTypeHelperTest {
	CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);

	@Test
	public void testCUBRIDDataTypeMethods() {
		String[][] types = new String[][]{
				{"CLOB", "clob", "clob" },
				{"BLOB", "blob", "blob" },
				{"character(1)", "char(1)", "char" },
				{"char(1)", "char(1)", "char" },
				{"character varying(1073741823)", "varchar(1073741823)",
						"varchar" },
				{"STRING", "varchar(1073741823)", "varchar" },
				{"character varying(30)", "varchar(30)", "varchar" },
				{"VARCHAR(30)", "varchar(30)", "varchar" },
				{"national character(1)", "char(1)", "char" },
				{"NCHAR(1)", "char(1)", "char" },
				{"national character varying(4)", "varchar(4)", "varchar" },
				{"VARNCHAR(4)", "varchar(4)", "varchar" },
				{"bit(10)", "bit(10)", "bit" },
				{"bit varying(30)", "bit varying(30)", "bit varying" },
				{"varbit(30)", "bit varying(30)", "bit varying" },
				{"numeric(15,0)", "numeric(15,0)", "numeric" },
				{"dec(15,0)", "numeric(15,0)", "numeric" },
				{"decimal(15,0)", "numeric(15,0)", "numeric" },
				{"integer", "int", "int" },
				{"smallint", "short", "short" },
				{"monetary", "monetary", "monetary" },
				{"float", "float", "float" },
				{"real", "float", "float" },
				{"double", "double", "double" },
				{"double precision", "double", "double" },
				{"date", "date", "date" },
				{"time", "time", "time" },
				{"timestamp", "timestamp", "timestamp" },
				{"set_of(numeric(15,0))", "set(numeric(15,0))", "set" },
				{"multiset_of(string)", "multiset(varchar(1073741823))",
						"multiset" },
				{"sequence_of(char(10))", "list(char(10))", "list" },
				{"sequence_of(numeric(10,2))", "list(numeric(10,2))", "list" },
				{"sequence_of(datetime)", "list(datetime)", "list" },
				{"SEQUENCE_OF(DATETIME)", "list(datetime)", "list" },
				{"ENUM('1','2','a','A')", "enum('1','2','a','A')", "enum" } };
		for (String[] strs : types) {
			System.out.println(strs[0]);
			Column column = new Column();
			dataTypeHelper.setColumnDataType(strs[0], column);
			Assert.assertTrue(dataTypeHelper.isValidDatatype(strs[0]));
			String shownType = dataTypeHelper.getShownDataType(column);
			Assert.assertEquals(strs[1], shownType);
			Assert.assertEquals(strs[2],
					dataTypeHelper.getStdMainDataType(strs[0]));
		}
	}

	/**
	 * testIsValidDatatype
	 */
	@Test
	public final void testIsValidDatatype() {
		String dataTypeInstance = null;
		boolean flag = dataTypeHelper.isValidDatatype(dataTypeInstance);
		Assert.assertFalse(flag);

		dataTypeInstance = "smallint";
		flag = dataTypeHelper.isValidDatatype(dataTypeInstance);
		Assert.assertTrue(flag);

		dataTypeInstance = "VARCHAR(10)";
		flag = dataTypeHelper.isValidDatatype(dataTypeInstance);

		Assert.assertTrue(flag);

		dataTypeInstance = "VARCHAR(2";
		flag = dataTypeHelper.isValidDatatype(dataTypeInstance);
		Assert.assertFalse(flag);

		dataTypeInstance = "VARCHAR2(2";
		flag = dataTypeHelper.isValidDatatype(dataTypeInstance);
		Assert.assertFalse(flag);

		dataTypeInstance = "VARCHAR(a)";
		flag = dataTypeHelper.isValidDatatype(dataTypeInstance);
		Assert.assertFalse(flag);

		dataTypeInstance = "sequence_of(char(10)";
		Assert.assertFalse(dataTypeHelper.isValidDatatype(dataTypeInstance));

		dataTypeInstance = "char(10))";
		Assert.assertFalse(dataTypeHelper.isValidDatatype(dataTypeInstance));
	}

	/**
	 * testGetScale
	 */
	@Test
	public final void testGetScale() {
		String jdbcType = "numeric(10,3)";
		int res = dataTypeHelper.getScale(jdbcType);
		System.out.println(res);
		Assert.assertEquals(3, res);

		String jdbcType2 = "SET(numeric(10,3))";
		int res2 = dataTypeHelper.getScale(jdbcType2);
		System.out.println(res2);
		Assert.assertEquals(3, res2);

		Assert.assertNull(dataTypeHelper.getScale("varchar(200)"));
		Assert.assertNull(dataTypeHelper.getScale("numeric(38)"));
		Assert.assertNull(dataTypeHelper.getScale("int"));
	}

	/**
	 * testGetPrecision
	 */
	@Test
	public final void testGetPrecision() {
		Assert.assertTrue(10 == dataTypeHelper.getPrecision("SET(numeric(10,3))"));
		Assert.assertTrue(10 == dataTypeHelper.getPrecision("SET(varchar(10))"));
		Assert.assertNull(dataTypeHelper.getPrecision("SET(int)"));
		Assert.assertTrue(10 == dataTypeHelper.getPrecision("numeric(10)"));
		Assert.assertTrue(10 == dataTypeHelper.getPrecision("varchar(10)"));
		Assert.assertNull(dataTypeHelper.getPrecision("int"));
	}

	/**
	 * testGetTypeRemain
	 */
	@Test
	public final void testGetRemain() {
		Assert.assertEquals("int", dataTypeHelper.getRemain("MULTISET(int)"));
		Assert.assertEquals("varchar(10)",
				dataTypeHelper.getRemain("MULTISET(varchar(10))"));
		Assert.assertEquals("numeric(10,2)",
				dataTypeHelper.getRemain("MULTISET(numeric(10,2))"));
		Assert.assertEquals("numeric(10,2)",
				dataTypeHelper.getRemain("MULTISET(NUMERIC(10,2))"));
	}

	@Test
	public void testIsValidValue() {
		CUBRIDDataTypeHelper helper = CUBRIDDataTypeHelper.getInstance(null);
		Assert.assertTrue(helper.isValidValue("char(1)", "a"));
		Assert.assertFalse(helper.isValidValue("char(1)", "aa"));
		Assert.assertTrue(helper.isValidValue("varchar(2)", "aa"));
		Assert.assertFalse(helper.isValidValue("varchar(2)", "aaa"));
		Assert.assertTrue(helper.isValidValue("int", "1"));
		Assert.assertFalse(helper.isValidValue("int", "a"));

		Assert.assertTrue(helper.isValidValue("integer", "1"));
		Assert.assertFalse(helper.isValidValue("integer", "b"));
		Assert.assertFalse(helper.isValidValue("integer", "99999999999999999"));

		Assert.assertTrue(helper.isValidValue("short", "1"));
		Assert.assertFalse(helper.isValidValue("short", "b"));

		Assert.assertTrue(helper.isValidValue("smallint", "1"));
		Assert.assertFalse(helper.isValidValue("smallint", "a"));

		Assert.assertTrue(helper.isValidValue("long", "111"));
		Assert.assertFalse(helper.isValidValue("long", "ff"));

		Assert.assertTrue(helper.isValidValue("bigint", "111"));
		Assert.assertFalse(helper.isValidValue("bigint", "ddd"));

		Assert.assertTrue(helper.isValidValue("date", "2013-01-01"));
		Assert.assertFalse(helper.isValidValue("date", "fasdf"));

		Assert.assertTrue(helper.isValidValue("time", "01:01:01.001"));
		Assert.assertFalse(helper.isValidValue("time", "fasdfasdf"));

		Assert.assertTrue(helper.isValidValue("datetime",
				"2013-01-01 01:01:01.001"));
		Assert.assertFalse(helper.isValidValue("datetime", "ttttt"));

		Assert.assertTrue(helper.isValidValue("timestamp",
				"2013-01-01 01:01:01"));
		Assert.assertFalse(helper.isValidValue("timestamp", "ffff"));

		Assert.assertTrue(helper.isValidValue("bit(8)", "b'00000001'"));
		Assert.assertTrue(helper.isValidValue("bit(8)", "B'00000001'"));
		Assert.assertTrue(helper.isValidValue("bit(8)", "x'FF'"));
		Assert.assertTrue(helper.isValidValue("bit(8)", "X'ff'"));
		Assert.assertTrue(helper.isValidValue("bit(8)", "0xFF"));
		Assert.assertTrue(helper.isValidValue("bit(8)", "0Xff"));
		Assert.assertTrue(helper.isValidValue("bit(8)", "0b00000001"));
		Assert.assertTrue(helper.isValidValue("bit(8)", "0B00000001"));
		Assert.assertTrue(helper.isValidValue("bit varying(16)", "b'00000001'"));
		Assert.assertFalse(!helper.isValidValue("bit(8)", "b'0a000001'"));
	}

	@Test
	public void testparseDTInstance() {
		DataTypeInstance dti = dataTypeHelper.parseDTInstance("int");
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("int", dti.getName());
		Assert.assertNull(dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());

		dti = dataTypeHelper.parseDTInstance("enum('1','2','4','3','A')");
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("enum", dti.getName());
		Assert.assertNull(dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertEquals("'1','2','4','3','A'", dti.getElments());

		dti = dataTypeHelper.parseDTInstance("varchar(100)");
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("varchar", dti.getName());
		Assert.assertEquals(new Integer(100), dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());

		dti = dataTypeHelper.parseDTInstance("numeric(38,2)");
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("numeric", dti.getName());
		Assert.assertEquals(new Integer(38), dti.getPrecision());
		Assert.assertEquals(new Integer(2), dti.getScale());
		Assert.assertNull(dti.getElments());

		dti = dataTypeHelper.parseDTInstance("numeric(38)");
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("numeric", dti.getName());
		Assert.assertEquals(new Integer(38), dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());

		dti = dataTypeHelper.parseDTInstance("set(int)");
		Assert.assertEquals("set", dti.getName());
		Assert.assertNull(dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());
		Assert.assertNotNull(dti.getSubType());
		dti = dti.getSubType();
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("int", dti.getName());
		Assert.assertNull(dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());

		dti = dataTypeHelper.parseDTInstance("set(varchar(100))");
		Assert.assertEquals("set", dti.getName());
		Assert.assertNull(dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());
		Assert.assertNotNull(dti.getSubType());
		dti = dti.getSubType();
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("varchar", dti.getName());
		Assert.assertEquals(new Integer(100), dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());

		dti = dataTypeHelper.parseDTInstance("set(numeric(38,2))");
		Assert.assertEquals("set", dti.getName());
		Assert.assertNull(dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());
		Assert.assertNotNull(dti.getSubType());
		dti = dti.getSubType();
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("numeric", dti.getName());
		Assert.assertEquals(new Integer(38), dti.getPrecision());
		Assert.assertEquals(new Integer(2), dti.getScale());
		Assert.assertNull(dti.getElments());

		dti = dataTypeHelper.parseDTInstance("SET(NUMERIC(38,2))");
		Assert.assertEquals("SET", dti.getName());
		Assert.assertNull(dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());
		Assert.assertNotNull(dti.getSubType());
		dti = dti.getSubType();
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("NUMERIC", dti.getName());
		Assert.assertEquals(new Integer(38), dti.getPrecision());
		Assert.assertEquals(new Integer(2), dti.getScale());
		Assert.assertNull(dti.getElments());

		dti = dataTypeHelper.parseDTInstance("SET(STRING)");
		Assert.assertEquals("SET", dti.getName());
		Assert.assertNull(dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());
		Assert.assertNotNull(dti.getSubType());
		dti = dti.getSubType();
		Assert.assertNull(dti.getSubType());
		Assert.assertEquals("varchar", dti.getName());
		Assert.assertEquals(new Integer(1073741823), dti.getPrecision());
		Assert.assertNull(dti.getScale());
		Assert.assertNull(dti.getElments());
	}

	@Test
	public void testgetCUBRIDDataTypeID() {
		String[][] testcases = new String[][]{{"smallint", "5" },
				{"int", "4" }, {"bigint", "-5" }, {"numeric", "2" },
				{"float", "7" }, {"double", "8" }, {"monetary", "30008" },
				{"char", "1" }, {"varchar", "12" }, {"nchar", "1" },
				{"nvarchar", "12" }, {"date", "91" }, {"time", "92" },
				{"timestamp", "93" }, {"datetime", "30093" }, {"bit", "-2" },
				{"bit varying", "-3" }, {"set", "31111" }, {"multiset", "41111" },
				{"sequence", "51111" }, {"object", "32000" },
				{"blob", "2004" }, {"clob", "2005" }, {"enum", "61111" } };
		for (String[] strs : testcases) {
			Assert.assertEquals(Integer.parseInt(strs[1]),
					dataTypeHelper.getCUBRIDDataTypeID(strs[0]));
		}
		testcases = new String[][]{{"SMALLINT", "5" }, {"short", "5" },
				{"INT", "4" }, {"BIGINT", "-5" }, {"NUMERIC(38,2)", "2" },
				{"decimal(38,2)", "2" }, {"dec(38,2)", "2" }, {"FLOAT", "7" },
				{"real", "7" }, {"DOUBLE", "8" }, {"MONETARY", "30008" },
				{"CHAR(1)", "1" }, {"CHARacter(1)", "1" },
				{"VARCHAR(10)", "12" }, {"character varying(10)", "12" },
				{"NCHAR(10)", "1" }, {"NVARCHAR(100)", "12" }, {"DATE", "91" },
				{"TIME", "92" }, {"TIMESTAMP", "93" }, {"DATETIME", "30093" },
				{"BIT(10)", "-2" }, {"VARBIT(11)", "-3" },
				{"SET(int)", "31111" }, {"MULTISET(varchar(2))", "41111" },
				{"SEQUENCE(numeric(38,2))", "51111" }, {"OBJECT", "32000" },
				{"BLOB", "2004" }, {"CLOB", "2005" },
				{"ENUM('a','A','b')", "61111" } }; //, {"string", "12" } 
		for (String[] strs : testcases) {
			Assert.assertEquals(Integer.parseInt(strs[1]),
					dataTypeHelper.getCUBRIDDataTypeID(strs[0]));
		}
	}
}
