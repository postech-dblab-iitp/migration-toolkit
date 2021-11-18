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
package com.cubrid.cubridmigration.core.common;

import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.TestUtil2;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.mysql.meta.MySQLSchemaFetcher;

/**
 * 
 * DbUtilTest
 * 
 * @author moulinwang
 * @author JessieHuang
 * @version 1.0 - 2009-9-18
 */
public class DbUtilsTest {

	/**
	 * testGetDateFormat
	 */
	@Test
	public final void testGetDateFormat() {
		DateFormat df = DBUtils.getDateFormat();
		Assert.assertNotNull(df);
	}

	/**
	 * test GetDatabaseProperties
	 * 
	 * @e
	 */
	@Test
	public final void testGetDatabaseProperties() {
		ConnParameters connParameters = TestUtil2.getMySQLConParam();

		Map<String, String> map = MySQLSchemaFetcher.getDatabaseProperties(connParameters);

		for (Entry<String, String> entry : map.entrySet()) {
			System.out.println(entry.getKey() + "------" + entry.getValue());
		}

		Assert.assertTrue(map.size() >= 0);

		try {
			String hostIp = "aaa";
			connParameters.setHost(hostIp);
			MySQLSchemaFetcher.getDatabaseProperties(connParameters);
		} catch (Exception e) {
			Assert.assertNotNull(e.getMessage());
		}
	}

	/**
	 * test GetMySQLProperties
	 * 
	 * @e
	 */
	@Test
	public final void testGetMySQLProperties() {
		ConnParameters connParameters = TestUtil2.getMySQLConParam();

		Map<String, String> map = MySQLSchemaFetcher.getDatabaseProperties(connParameters);

		for (Entry<String, String> entry : map.entrySet()) {
			System.out.println(entry.getKey() + "------" + entry.getValue());
		}

		Assert.assertTrue(map.size() > 0);
	}

	/**
	 * test QueryMySQLParameter
	 * 
	 * @e
	 * @throws ClassNotFoundException e
	 */
	@Test
	public final void testQueryMySQLParameter() throws ClassNotFoundException {
		ConnParameters connParameters = TestUtil2.getMySQLConParam();
		Map<String, String> map = MySQLSchemaFetcher.getDatabaseProperties(connParameters);

		for (Entry<String, String> entry : map.entrySet()) {
			System.out.println(entry.getKey() + "------" + entry.getValue());
		}

		Assert.assertTrue(map.size() > 0);

		String str = map.get("yearIsDateType");
		System.out.println("yearIsDateType****" + str);
		Assert.assertEquals("true", str);
		Assert.assertNotNull(str);
	}

	/**
	 * test Reader2String
	 * 
	 * @throws IOException e
	 */
	@Test
	public final void testReader2String() throws IOException {
		String str = "hello world!";
		StringReader re = new StringReader(str);

		String abc = DBUtils.reader2String(re);

		Assert.assertEquals(str, abc);
	}

	/**
	 * testGetBitString
	 */
	@Test
	public final void testGetBitString() {
		String str = "hello world!";
		byte[] bt = str.getBytes();

		String ret = DBUtils.getBitString(bt, bt.length);

		Assert.assertNotNull(ret);
	}

	/**
	 * testParsePartitionFunc
	 */
	@Test
	public final void testParsePartitionFunc() {
		String exp = "DAY(abc)";
		String ret = DBUtils.parsePartitionFunc(exp);
		Assert.assertEquals("DAY", ret);

		exp = "(DAY(abc)";
		ret = DBUtils.parsePartitionFunc(exp);
		Assert.assertNull(ret);

		exp = "DAY(abc";
		ret = DBUtils.parsePartitionFunc(exp);
		Assert.assertNull(ret);

		exp = "DAYabc";
		ret = DBUtils.parsePartitionFunc(exp);
		Assert.assertNull(ret);

		exp = "DAYab)c";
		ret = DBUtils.parsePartitionFunc(exp);
		Assert.assertNull(ret);
	}

	/**
	 * testGetPartitionColumns
	 */
	@Test
	public final void testParsePartitionColumns() {
		Table table = new Table();
		Column col1 = new Column(table);
		col1.setName("col1");
		Column col2 = new Column(table);
		col2.setName("col2");
		Column col3 = new Column(table);
		col3.setName("col3");
		Column col4 = new Column(table);
		col4.setName("col4");
		Column col5 = new Column(table);
		col5.setName("date1");

		table.addColumn(col1);
		table.addColumn(col2);
		table.addColumn(col3);
		table.addColumn(col4);
		table.addColumn(col5);

		String exp = "COLUMNS(col1, col3)";
		List<Column> list1 = DBUtils.parsePartitionColumns(table, exp);
		Assert.assertEquals(2, list1.size());

		exp = "COLUMNS(col1)";
		List<Column> list2 = DBUtils.parsePartitionColumns(table, exp);
		Assert.assertEquals(1, list2.size());

		exp = "EXTRACT(unit  FROM date1)";
		List<Column> list3 = DBUtils.parsePartitionColumns(table, exp);
		Assert.assertEquals(1, list3.size());
		Assert.assertEquals("date1", list3.get(0).getName());

		exp = "COLUMNS([col1])";
		List<Column> list4 = DBUtils.parsePartitionColumns(table, exp);
		Assert.assertEquals(1, list4.size());

		exp = "COLUMNS([col1],[col5])";
		List<Column> list5 = DBUtils.parsePartitionColumns(table, exp);
		Assert.assertEquals(1, list5.size());
	}

	@Test
	public void testsupportedCubridPartition() {
		Assert.assertFalse(DBUtils.supportedCubridPartition(null));
		PartitionInfo cubridPartition = new PartitionInfo();
		cubridPartition.setDDL("partition by ...");
		Assert.assertTrue(DBUtils.supportedCubridPartition(cubridPartition));
		cubridPartition.setDDL("");
		Assert.assertFalse(DBUtils.supportedCubridPartition(cubridPartition));
	}
}
