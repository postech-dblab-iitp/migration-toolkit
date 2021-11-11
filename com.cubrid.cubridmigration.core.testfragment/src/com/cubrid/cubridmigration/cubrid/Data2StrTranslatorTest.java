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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.Data2StrTranslator;

/**
 * 
 * CUBRIDDataTypeTest
 * 
 * @author JessieHuang
 * @version 1.0 - 2010-01-13
 */
public class Data2StrTranslatorTest {
	CUBRIDDataTypeHelper dtHelper = CUBRIDDataTypeHelper.getInstance(null);
	Data2StrTranslator importHelper;
	{
		try {
			MigrationConfiguration mySQLConfig = TemplateParserTest.getMySQLConfig();
			importHelper = new Data2StrTranslator("", mySQLConfig,
					mySQLConfig.getDestType());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * testExportToCUBRIDUnLoadFile
	 * 
	 * @throws ParseException
	 */
	@Test
	public final void testExportToCUBRIDUnLoadFile() throws ParseException {
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
		List<String> lobFiles = new ArrayList<String>();
		Table table = new Table();
		table.setName("test");

		Object data1 = "NULL";
		Column targetColumn1 = new Column();
		targetColumn1.setTableOrView(table);
		targetColumn1.setDataType("character");
		targetColumn1.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn1.getDataType()));
		targetColumn1.setScale(0);

		String res = importHelper.stringValueOf(data1, targetColumn1, lobFiles);
		Assert.assertEquals("'NULL'", res);

		data1 = null;
		targetColumn1 = new Column();
		targetColumn1.setTableOrView(table);
		targetColumn1.setDataType("character");
		targetColumn1.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn1.getDataType()));
		res = importHelper.stringValueOf(data1, targetColumn1, lobFiles);
		Assert.assertEquals("NULL", res);

		String data2 = "2";
		Column targetColumn2 = new Column();
		targetColumn2.setTableOrView(table);
		targetColumn2.setDataType("monetary");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("2", res);

		targetColumn2.setDataType("integer");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("2", res);

		targetColumn2.setDataType("smallint");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("2", res);

		targetColumn2.setDataType("BIGINT");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("2", res);

		targetColumn2.setDataType("FLOAT");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("2", res);

		targetColumn2.setDataType("NUMERIC");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		targetColumn2.setScale(0);
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("2.", res);

		targetColumn2.setDataType("NUMERIC");
		targetColumn2.setScale(1);
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("2.", res);

		targetColumn2.setDataType("NUMERIC");
		targetColumn2.setPrecision(10);
		targetColumn2.setScale(3);
		targetColumn1.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn1.getDataType()));
		res = importHelper.stringValueOf("2.0", targetColumn2, lobFiles);
		Assert.assertEquals("2.", res);

		data2 = "23.3";
		targetColumn2.setDataType("NUMERIC");
		targetColumn2.setScale(1);
		targetColumn1.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn1.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("23.3", res);

		data2 = "23.3";
		targetColumn2.setDataType("DOUBLE");
		targetColumn2.setScale(1);
		targetColumn1.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn1.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("23.3", res);

		data2 = "-Infinity";
		targetColumn2.setDataType("DOUBLE");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("-1.7976931348623157e+308", res);

		data2 = "Infinity";
		targetColumn2.setDataType("DOUBLE");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("1.7976931348623157e+308", res);

		byte[] data3 = "abc".getBytes();
		targetColumn2.setDataType("BIT");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data3, targetColumn2, lobFiles);
		Assert.assertEquals("X'616263'", res);

		targetColumn2.setDataType("BIT VARYING");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data3, targetColumn2, lobFiles);
		Assert.assertEquals("X'616263'", res);

		data2 = "23.3";
		targetColumn2.setDataType("NCHAR");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data2, targetColumn2, lobFiles);
		Assert.assertEquals("'23.3'", res);

		Time data4 = Time.valueOf("10:09:08");
		targetColumn2.setDataType("TIME");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data4, targetColumn2, lobFiles);
		Assert.assertEquals("time'10:09:08'", res);

		Date data5 = Date.valueOf("2010-01-20");
		targetColumn2.setDataType("DATE");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data5, targetColumn2, lobFiles);
		Assert.assertEquals("date'01/20/2010'", res);

		DateFormat dateFormatter = CUBRIDTimeUtil.getDateFormat(
				"yyyy-mm-dd HH:mm:ss", Locale.ENGLISH,
				TimeZone.getTimeZone("GMT+8:00"));

		Timestamp data6 = new Timestamp(dateFormatter.parse(
				"2010-01-20 10:09:08").getTime());
		targetColumn2.setDataType("TIMESTAMP");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data6, targetColumn2, lobFiles);
		Assert.assertEquals("timestamp'10:09:08 AM 01/20/2010'", res);

		targetColumn2.setDataType("DATETIME");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		res = importHelper.stringValueOf(data6, targetColumn2, lobFiles);
		Assert.assertEquals("datetime'2010-01-20 10:09:08.000'", res);

		targetColumn2.setDataType("SET");
		targetColumn2.setSubDataType("INTEGER");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		targetColumn2.setJdbcIDOfSubDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getSubDataType()));
		List<String> list = new ArrayList<String>();
		list.add("1");
		list.add("2");
		res = importHelper.stringValueOf(list, targetColumn2, lobFiles);
		Assert.assertEquals("{1,2}", res);

		targetColumn2.setDataType("SET");
		targetColumn2.setSubDataType("INTEGER");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		targetColumn2.setJdbcIDOfSubDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getSubDataType()));
		res = importHelper.stringValueOf("1,2", targetColumn2, lobFiles);
		Assert.assertEquals("{1,2}", res);

		targetColumn2.setDataType("SET");
		targetColumn2.setSubDataType("INTEGER");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		targetColumn2.setJdbcIDOfSubDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getSubDataType()));
		res = importHelper.stringValueOf("1,2", targetColumn2, lobFiles);
		Assert.assertEquals("{1,2}", res);

		targetColumn2.setDataType("MULTISET");
		targetColumn2.setSubDataType("INTEGER");
		targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
		targetColumn2.setJdbcIDOfSubDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getSubDataType()));
		res = importHelper.stringValueOf(null, targetColumn2, lobFiles);
		Assert.assertEquals("NULL", res);

		try {
			targetColumn2.setDataType("SEQUENCE");
			targetColumn2.setSubDataType("INTEGER");
			targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
			targetColumn2.setJdbcIDOfSubDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getSubDataType()));
			res = importHelper.stringValueOf("ab", targetColumn2, lobFiles);

		} catch (Exception e) {
			Assert.assertEquals(
					"Can not format data \"ab\" to data type \"SEQUENCE\"",
					e.getMessage());
		}
		try {
			targetColumn2.setDataType("SEQUENCE2");
			targetColumn2.setJdbcIDOfDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getDataType()));
			targetColumn2.setJdbcIDOfSubDataType(dtHelper.getCUBRIDDataTypeID(targetColumn2.getShownDataType()));
			res = importHelper.stringValueOf("123", targetColumn2, lobFiles);
			Assert.assertEquals("NULL", res);
		} catch (Exception e) {
			Assert.assertEquals(
					"UnSupported CUBRID data type:SEQUENCE2".toLowerCase(),
					e.getMessage().toLowerCase());
		}

	}

	/**
	 * testGetRecordString
	 */
	@Test
	public void testGetRecordString() {
		List<String> columnDataList = new ArrayList<String>();
		List<ColumnValue> columnList = new ArrayList<ColumnValue>();

		Column col1 = new Column();
		col1.setDataType("integer");
		col1.setName("a1");
		columnList.add(new ColumnValue(col1, ""));
		String c1 = "1";
		columnDataList.add(c1);

		Column col2 = new Column();
		col2.setDataType("character");
		col2.setName("a2");
		columnList.add(new ColumnValue(col2, ""));
		String c2 = "NULL";
		columnDataList.add(c2);

		Column col3 = new Column();
		col3.setDataType("character");
		col3.setName("a3");
		columnList.add(new ColumnValue(col3, ""));
		String c3 = "1aa";
		columnDataList.add(c3);

		Column col4 = new Column();
		col4.setDataType("character varying");
		col4.setName("a4");
		columnList.add(new ColumnValue(col4, ""));
		String c4 = "1aadsffffffffffffffffwerqrqrqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
				+ "1aadsffffffffffffffffwerqrqrqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqssssss"
				+ "1aadsffffffffffffffffwerqrqrqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqssssss"
				+ "1aadsffffffffffffffffwerqrqrqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqssssss";
		columnDataList.add(c4);

		Column col5 = new Column();
		col5.setDataType("bit varying");
		col5.setName("a5");
		columnList.add(new ColumnValue(col5, ""));
		String c5 = "B'1aa'";
		columnDataList.add(c5);

		Column col6 = new Column();
		col6.setDataType("bit");
		col6.setName("a6");
		columnList.add(new ColumnValue(col6, ""));
		String c6 = "B'1'";
		columnDataList.add(c6);

		String res = importHelper.getRecordString(columnList, columnDataList);

		Assert.assertNotNull(res);

	}

}
