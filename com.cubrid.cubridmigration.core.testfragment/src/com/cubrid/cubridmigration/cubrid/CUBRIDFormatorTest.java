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

import java.text.ParseException;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;

public class CUBRIDFormatorTest {

	private static final String B_0 = "B'0'";
	String datatype = null;
	String subDataType = null;
	int scale = 0;
	String attdeft = null;
	String retattdeft = null;
	int precision = 10;
	DataTypeInstance dti = new DataTypeInstance();

	@Test
	public void mock() {

	}

	@Test
	public void testTime() throws ParseException {
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
		datatype = "time";
		dti.setName(datatype);
		attdeft = "1245";
		retattdeft = "TIME'00:00:01'";

		Assert.assertEquals("TIME'08:00:01'",
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "am 09:53:06";
		retattdeft = "TIME'09:53:06'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53:06 am";
		retattdeft = "TIME'09:53:06'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "19:53:06";
		retattdeft = "TIME'19:53:06'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "am 09:53";
		retattdeft = "TIME'09:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53 am";
		retattdeft = "TIME'09:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "19:53";
		retattdeft = "TIME'19:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "sysTime";
		retattdeft = "systime";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "currentTime";
		retattdeft = "current_time";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "";
		retattdeft = "";

		Assert.assertEquals(null,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "ddd";
		retattdeft = "ddd";

		attdeft = "TIME'19:53:00'";
		retattdeft = "TIME'19:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "TIME'19:53:00";
		retattdeft = null;

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());
	}

	@Test
	public void testDate() throws ParseException {
		datatype = "Date";
		dti.setName(datatype);
		attdeft = "02/23/2009";
		retattdeft = "DATE'02/23/2009'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009/02/23";
		retattdeft = "DATE'02/23/2009'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009-02-23";
		retattdeft = "DATE'02/23/2009'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "02/23";
		retattdeft = "DATE'02/23'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "sysDate";
		retattdeft = "sysdate";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "currentdATe";
		retattdeft = "current_date";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "";
		retattdeft = "";

		Assert.assertEquals(null,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "date'";
		retattdeft = null;

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "DATE'02/23'";
		retattdeft = "DATE'02/23'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "DAT02/23";
		retattdeft = null;
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

	}

	@Test
	public void testSet() throws ParseException {
		datatype = "set_of";
		subDataType = "tImestamp";
		dti.setName(datatype);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(subDataType);
		attdeft = "2009/02/23 am 09:53:08,2009-02-23 am 09:53:09,2009/02/23 09:53:10";
		retattdeft = "{TIMESTAMP'02/23/2009 09:53:08',"
				+ "TIMESTAMP'02/23/2009 09:53:09',"
				+ "TIMESTAMP'02/23/2009 09:53:10'}";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		datatype = "set_of";
		subDataType = "datetime";
		dti.setName(datatype);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(subDataType);
		attdeft = "2009/02/23 am 09:53:08.333," + "2009/02/23 am 09:53:09.333,"
				+ "2009/02/23 am 09:53:10.333";
		retattdeft = "{DATETIME'2009-02-23 09:53:08.333',"
				+ "DATETIME'2009-02-23 09:53:09.333',"
				+ "DATETIME'2009-02-23 09:53:10.333'}";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());
	}

	@Test
	public void testTimestamp() throws ParseException {
		datatype = "tImestamp";
		dti.setName(datatype);

		attdeft = "2009/02/23 am 09:53:08";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:08'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009-02-23 am 09:53:08";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:08'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009/02/23 09:53:08";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:08'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009-02-23 09:53:08";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:08'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53:08 am 02/23/2009";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:08'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53:08 02/23/2009";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:08'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009/02/23 am 09:53";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009-02-23 am 09:53";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009/02/23 09:53";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009-02-23 09:53";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53 am 02/23/2009";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53 02/23/2009";
		retattdeft = "TIMESTAMP'02/23/2009 09:53:00'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "systImestamp";
		retattdeft = "systimestamp";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "currenttImestamp";
		retattdeft = "current_timestamp";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53:05 02/23";
		retattdeft = "TIMESTAMP'09:53:05 02/23'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53:05 pm 02/23";
		retattdeft = "TIMESTAMP'09:53:05 pm 02/23'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "02/23 09:53:05";
		retattdeft = "TIMESTAMP'02/23 09:53:05'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "02/23 09:53:05 am";
		retattdeft = "TIMESTAMP'02/23 09:53:05 am'";

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "";
		retattdeft = "";

		Assert.assertEquals(null,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "timestamp";
		retattdeft = null;

		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "TIMESTAMP'02/23 09:53:05 am'";
		retattdeft = "TIMESTAMP'02/23 09:53:05 am'";
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "100000000";
		retattdeft = "TIMESTAMP'"
				+ CUBRIDTimeUtil.formatTimestampLong(100000000,
						"MM/dd/yyyy HH:mm:ss", TimeZone.getDefault()) + "'";
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "TIMESTAM02/23 09:53:05 am";
		retattdeft = null;
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

	}

	@Test
	public void test() {
		attdeft = "09:53:08.333 02/23/2009";
		boolean t = CUBRIDTimeUtil.validateDateString(attdeft,
				"HH:mm:ss.SSS MM/dd/yyyy");
		System.out.println(t);
	}

	@Test
	public void testFormatValue() {
		attdeft = "10";
		datatype = "numeric";
		subDataType = null;
		scale = 1;
		retattdeft = "10";

		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "aa";
		retattdeft = null;
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		//		
		//				
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "DATETIM2'2009-02-23 09:53:08.300'";
		datatype = "datetime";
		retattdeft = null;
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "10";
		datatype = "multiset_of";
		subDataType = "numeric";
		scale = 1;
		retattdeft = "{10}";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(subDataType);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "10";
		datatype = "sequence_of";
		retattdeft = "{10}";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(subDataType);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "10,11";
		datatype = "multiset_of";
		retattdeft = "{10,11}";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(subDataType);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "{10,11}";
		retattdeft = "{10,11}";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());
	}

	@Test
	public void testFormatDatetime() {
		attdeft = "2009/02/23 am 09:53:08.0";
		retattdeft = "2009/02/23 AM 09:53:08.000";
		Assert.assertEquals(retattdeft, CUBRIDTimeUtil.formatDateTime(attdeft,
				"yyyy/MM/dd a hh:mm:ss.SSS", TimeZone.getDefault()));

		attdeft = "2009/02/23 am 09:53:08.3";
		retattdeft = "2009/02/23 AM 09:53:08.300";
		Assert.assertEquals(retattdeft, CUBRIDTimeUtil.formatDateTime(attdeft,
				"yyyy/MM/dd a hh:mm:ss.SSS", TimeZone.getDefault()));

		attdeft = "2009/02/23 am 09:53:08.33";
		retattdeft = "2009/02/23 AM 09:53:08.330";
		Assert.assertEquals(retattdeft, CUBRIDTimeUtil.formatDateTime(attdeft,
				"yyyy/MM/dd a hh:mm:ss.SSS", TimeZone.getDefault()));

	}

	@Test
	public void testDatetime() throws ParseException {
		datatype = "datetime";

		attdeft = "2009/02/23 am 09:53:08.3";
		retattdeft = "DATETIME'2009-02-23 09:53:08.300'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009/02/23 am 09:53:08.333";
		retattdeft = "DATETIME'2009-02-23 09:53:08.333'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009-02-23 am 09:53:08.333";
		retattdeft = "DATETIME'2009-02-23 09:53:08.333'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009/02/23 09:53:08.333";
		retattdeft = "DATETIME'2009-02-23 09:53:08.333'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009-02-23 09:53:08.333";
		retattdeft = "DATETIME'2009-02-23 09:53:08.333'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53:08.333 am 02/23/2009";
		retattdeft = "DATETIME'2009-02-23 09:53:08.333'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53:08.333 02/23/2009";
		retattdeft = "DATETIME'2009-02-23 09:53:08.333'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009/02/23 am 09:53";
		retattdeft = "DATETIME'2009-02-23 09:53:00.000'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009-02-23 am 09:53";
		retattdeft = "DATETIME'2009-02-23 09:53:00.000'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009/02/23 09:53";
		retattdeft = "DATETIME'2009-02-23 09:53:00.000'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "2009-02-23 09:53";
		retattdeft = "DATETIME'2009-02-23 09:53:00.000'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53 am 02/23/2009";
		retattdeft = "DATETIME'2009-02-23 09:53:00.000'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53 02/23/2009";
		retattdeft = "DATETIME'2009-02-23 09:53:00.000'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "sysDatetime";
		retattdeft = "sysdatetime";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "sys_Datetime";
		retattdeft = "sysdatetime";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "current_Datetime";
		retattdeft = "current_datetime";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53:05 02/23";
		retattdeft = "DATETIME'09:53:05 02/23'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "09:53:05 pm 02/23";
		retattdeft = "DATETIME'09:53:05 pm 02/23'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "02/23 09:53:05";
		retattdeft = "DATETIME'02/23 09:53:05'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "02/23 09:53:05 am";
		retattdeft = "DATETIME'02/23 09:53:05 am'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "";
		retattdeft = "";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(null,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "DATETIME'";
		retattdeft = "";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);

		attdeft = "DATETIME''";
		retattdeft = null;
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "DATETIME'02/23 09:53:05 am'";
		retattdeft = "DATETIME'02/23 09:53:05 am'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "100000000";
		retattdeft = "DATETIME'"
				+ CUBRIDTimeUtil.formatTimestampLong(100000000,
						"yyyy-MM-dd HH:mm:ss.SSS", TimeZone.getDefault()) + "'";// "DATETIME'1970-01-02 03:46:40.000'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "DATETIME'02/23 09:53:05 am";
		retattdeft = null;
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());
	}

	@Test
	public void testChar() throws ParseException {
		datatype = "char";

		attdeft = "'abc'";
		retattdeft = "'abc'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "'ab''c'";
		retattdeft = "'ab''c'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "abc";
		retattdeft = "'abc'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "abc ";
		retattdeft = "'abc '";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "ab'c";
		retattdeft = "'ab''c'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "";
		retattdeft = null;
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals("''",
				CUBRIDFormator.format(dti, attdeft).getFormatResult());
	}

	@Test
	public void testVarchar() throws ParseException {
		datatype = "varchar";

		attdeft = "'abc'";
		retattdeft = "'abc'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "'ab''c'";
		retattdeft = "'ab''c'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "abc";
		retattdeft = "'abc'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "abc ";
		retattdeft = "'abc '";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "ab'c";
		retattdeft = "'ab''c'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "";
		retattdeft = "''";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());
	}

	//	@Test
	//	public void testNchar() throws ParseException {
	//		datatype = "national character";
	//
	//		attdeft = "N'����bc'";
	//		retattdeft = "'N''����bc'''";
	//		
	//				
	//		Assert.assertEquals(
	//				retattdeft,
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//
	//		attdeft = "N'����b''c'";
	//		retattdeft = "N'����b''c'";
	//		
	//				
	//		Assert.assertEquals(
	//				retattdeft,
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//
	//		attdeft = "abc";
	//		retattdeft = "N'abc'";
	//		
	//				
	//		Assert.assertEquals(
	//				retattdeft,
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//
	//		attdeft = "ab'c";
	//		retattdeft = "N'ab''c'";
	//		
	//				
	//		Assert.assertEquals(
	//				retattdeft,
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//
	//		attdeft = "";
	//		retattdeft = null;
	//		
	//				
	//		Assert.assertEquals(
	//				"",
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//	}
	//
	//	@Test
	//	public void testNcharVar() throws ParseException {
	//		datatype = "national character varying";
	//
	//		attdeft = "N'����bc'";
	//		retattdeft = "N'����bc'";
	//		
	//				
	//		Assert.assertEquals(
	//				retattdeft,
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//
	//		attdeft = "N'����b''c'";
	//		retattdeft = "N'����b''c'";
	//		
	//				
	//		Assert.assertEquals(
	//				retattdeft,
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//
	//		attdeft = "abc";
	//		retattdeft = "N'abc'";
	//		
	//				
	//		Assert.assertEquals(
	//				retattdeft,
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//
	//		attdeft = "ab'c";
	//		retattdeft = "N'ab''c'";
	//		
	//				
	//		Assert.assertEquals(
	//				retattdeft,
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//
	//		attdeft = "";
	//		retattdeft = null;
	//		
	//				
	//		Assert.assertEquals(
	//				"",
	//				CUBRIDFormator.format(datatype, subDataType, scale,
	//						attdeft).getFormatResult());
	//	}

	@Test
	public void testBit() throws ParseException {
		datatype = "bit";

		attdeft = "B'001'";
		retattdeft = "B'001'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "X'001'";
		retattdeft = "X'001'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "001";
		retattdeft = "X'001'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "";
		retattdeft = null;
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(B_0,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());
	}

	@Test
	public void testBitVar() throws ParseException {
		datatype = "bit varying";

		attdeft = "B'001'";
		retattdeft = "B'001'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "X'001'";
		retattdeft = "X'001'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "001";
		retattdeft = "X'001'";
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(retattdeft,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());

		attdeft = "";
		retattdeft = null;
		dti.setName(datatype);
		dti.setPrecision(precision);
		dti.setScale(scale);
		Assert.assertEquals(B_0,
				CUBRIDFormator.format(dti, attdeft).getFormatResult());
	}

	/**
	 * testGetCollectionValues
	 * 
	 * @throws NumberFormatException e
	 * @throws ParseException e
	 */
	@Test
	public final void testGetCollectionValues() throws NumberFormatException,
			ParseException {
		String type = "INTEGER";
		String value = "{1,2,3}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);

		Object[] obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 3);

		type = "BIGINT";
		value = "{1,2,3}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 3);

		type = "NUMERIC";
		value = "{1.0,2.1,3}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(1);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 3);

		type = "FLOAT";
		value = "{1,2,3}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 3);

		type = "DOUBLE";
		value = "{1,2,3}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 3);

		type = "monetary";
		value = "{1,2,3}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 3);

		type = "CHARACTER";
		value = "{'A','B','C'}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 3);

		type = "char";
		value = "{\"A\",\"B\",\"C\"}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 3);

		type = "numeric";
		value = "{1.2,1.3}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(1);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 2);

		type = "numeric";
		value = "{10,2}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 2);

		type = "time";
		value = "{\"12:00:00\"}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 1);

		type = "date";
		value = "{\"2012-01-01\",\"2012-01-01\"}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 2);

		type = "datetime";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		value = "{\"2012-01-01 12:00:00.001\",\"2012-01-01 12:00:00.001\"}";
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 2);

		type = "timestamp";
		value = "{\"2012-01-01 12:00:00\",\"2012-01-02 12:00:00\"}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 2);
		try {
			type = "date";
			value = "{\"12-12-2008\"}";
			dti.setName("set");
			dti.setPrecision(precision);
			dti.setScale(0);
			dti.setSubType(new DataTypeInstance());
			dti.getSubType().setName(type);
			obj = CUBRIDFormator.getCollectionValues(dti, value);
			Assert.assertTrue(obj.length == 1);

			type = "timestamp";
			value = "{\"2008/12/13 13:00:00\"}";
			dti.setName("set");
			dti.setPrecision(precision);
			dti.setScale(0);
			dti.setSubType(new DataTypeInstance());
			dti.getSubType().setName(type);
			obj = CUBRIDFormator.getCollectionValues(dti, value);
			Assert.assertTrue(obj.length == 1);

			type = "datetime";
			value = "{\"2008-12-12 12:00:00.333\"}";
			dti.setName("set");
			dti.setPrecision(precision);
			dti.setScale(0);
			dti.setSubType(new DataTypeInstance());
			dti.getSubType().setName(type);
			obj = CUBRIDFormator.getCollectionValues(dti, value);
			Assert.assertTrue(obj.length == 1);

			type = "datetime";
			value = "{\"2008-12-12 12:00:222\",\"2008-12-12 12:00:223\"}";
			dti.setName("set");
			dti.setPrecision(precision);
			dti.setScale(0);
			dti.setSubType(new DataTypeInstance());
			dti.getSubType().setName(type);
			obj = CUBRIDFormator.getCollectionValues(dti, value);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		type = "bit varying";
		value = "{0001,01000}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 2);

		type = "bit";
		value = "{0001,01000}";
		dti.setName("set");
		dti.setPrecision(precision);
		dti.setScale(0);
		dti.setSubType(new DataTypeInstance());
		dti.getSubType().setName(type);
		obj = CUBRIDFormator.getCollectionValues(dti, value);
		Assert.assertTrue(obj.length == 2);

	}

	/**
	 * testFormatValue
	 */
	@Test
	public final void testFormatValue2() {
		String type = "DATE";
		String subType = null;
		String value = "sysdate";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		String obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("sysdate", obj);

		type = "DATE";
		value = "sys_date";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("sysdate", obj);

		value = "currentdate";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("current_date", obj);

		value = "current_date";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("current_date", obj);

		type = "DATE";
		value = "";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals(null, obj);

		type = "DATE";
		value = "2010-01-13";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("DATE'01/13/2010'", obj);

		value = "DATE'01/13/2010'";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("DATE'01/13/2010'", obj);

		value = "01/13";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("DATE'01/13'", obj);

		type = "DATETIME";
		value = "current_datetime";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("current_datetime", obj);

		value = "DATETIME'2010-01-13 14:22:22.000'";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("DATETIME'2010-01-13 14:22:22.000'", obj);

		value = "2010-01-13 14:22:22";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("DATETIME'2010-01-13 14:22:22.000'", obj);

		value = "14:22:22 12/13";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("DATETIME'14:22:22 12/13'", obj);

		type = "DATETIME";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "a2010-01-13 14:22:22";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertNull(obj);

		type = "DATETIME";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = String.valueOf(new java.util.Date().getTime());
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertNotNull(obj);

		type = "DATETIME";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		type = "DATETIME";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "sysdatetime";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("sysdatetime", obj);

		type = "timestamp";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "systimestamp";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("systimestamp", obj);

		type = "timestamp";
		value = "sys_timestamp";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("systimestamp", obj);

		type = "timestamp";
		value = "currenttimestamp";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("current_timestamp", obj);

		type = "timestamp";
		value = "current_timestamp";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("current_timestamp", obj);

		type = "timestamp";
		value = "10:10:10 AM 01/01";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("TIMESTAMP'10:10:10 AM 01/01'", obj);

		type = "timestamp";
		value = "12/15 09:10:11 PM";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("TIMESTAMP'12/15 09:10:11 PM'", obj);

		value = "";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals(null, obj);

		value = "timestamp'2010-01-13 14:22:22'";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("TIMESTAMP'01/13/2010 14:22:22'", obj);

		value = "10000000000";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals(
				"TIMESTAMP'"
						+ CUBRIDTimeUtil.formatTimestampLong(10000000000l,
								"MM/dd/yyyy HH:mm:ss", TimeZone.getDefault())
						+ "'", obj);

		value = "2010-01-13 14:22:22";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("TIMESTAMP'01/13/2010 14:22:22'", obj);

		value = "01/13 14:22:22";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("TIMESTAMP'01/13 14:22:22'", obj);

		type = "time";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "systime";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("systime", obj);

		value = "";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals(null, obj);

		type = "time";
		value = "sys_time";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("systime", obj);

		type = "time";
		value = "currenttime";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("current_time", obj);

		type = "time";
		value = "current_time";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("current_time", obj);

		value = "time'12:00:00'";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("TIME'12:00:00'", obj);

		value = "1233";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals(
				"TIME'"
						+ CUBRIDTimeUtil.formatDateTime(1233, "HH:mm:ss",
								TimeZone.getDefault()) + "'", obj);

		value = "12:33";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("TIME'12:33:00'", obj);

		type = "character";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "abc";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("'abc'", obj);

		type = "varchar";
		value = "abc";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("'abc'", obj);

		type = "varchar";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "'abc'";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("'abc'", obj);

		type = "integer";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "2";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("2", obj);

		type = "smallint";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "2";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("2", obj);

		type = "bigint";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "2";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("2", obj);

		type = "numeric";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "2";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("2", obj);

		type = "numeric";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(1);
		value = "2.1";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("2.1", obj);

		type = "float";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "2.1";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("2.1", obj);

		type = "double";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "2.1";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("2.1", obj);

		type = "monetary";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "2.1";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("2.1", obj);

		//		type = "national character";
		//		value = "abc";
		//		obj = CUBRIDFormator.format(type, subType, 0, value).getFormatResult();
		//
		//		Assert.assertEquals("N'abc'", obj);
		//
		//		type = "national character";
		//		value = "N'abc'";
		//		obj = CUBRIDFormator.format(type, subType, 0, value).getFormatResult();
		//
		//		Assert.assertEquals("N'abc'", obj);
		//
		//		type = "national character varying";
		//		value = "abc";
		//		obj = CUBRIDFormator.format(type, subType, 0, value).getFormatResult();
		//
		//		Assert.assertEquals("N'abc'", obj);
		//
		//		type = "national character varying";
		//		value = "N'abc'";
		//		obj = CUBRIDFormator.format(type, subType, 0, value).getFormatResult();
		//
		//		Assert.assertEquals("N'abc'", obj);

		type = "bit";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "B'1'";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("B'1'", obj);

		type = "bit varying";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "B'1'";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("B'1'", obj);

		type = "bit";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "X'1'";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("X'1'", obj);

		type = "bit varying";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "X'1'";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("X'1'", obj);

		type = "bit";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals(B_0, obj);

		type = "bit varying";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals(B_0, obj);

		type = "bit";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "abc";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();
		Assert.assertEquals("X'abc'", obj);

		type = "bit varying";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);
		value = "eee";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("X'eee'", obj);

		type = "set_of";
		subType = "integer";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);

		DataTypeInstance subDti = new DataTypeInstance();
		subDti.setName(subType);
		dti.setSubType(subDti);
		value = "{1,2,3}";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("{1,2,3}", obj);

		type = "multiset_of";
		subType = "integer";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);

		subDti = new DataTypeInstance();
		subDti.setName(subType);
		dti.setSubType(subDti);
		value = "{1,2,3}";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("{1,2,3}", obj);

		type = "sequence_of";
		subType = "integer";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);

		subDti = new DataTypeInstance();
		subDti.setName(subType);
		dti.setSubType(subDti);
		value = "{1,2,3}";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("{1,2,3}", obj);

		type = "sequence_of";
		subType = "integer";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);

		subDti = new DataTypeInstance();
		subDti.setName(subType);
		dti.setSubType(subDti);
		value = "";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals(null, obj);

		type = "sequence_of";
		subType = "integer";
		dti.setName(type);
		dti.setPrecision(precision);
		dti.setScale(0);

		subDti = new DataTypeInstance();
		subDti.setName(subType);
		dti.setSubType(subDti);
		value = "1,2,3";
		obj = CUBRIDFormator.format(dti, value).getFormatResult();

		Assert.assertEquals("{1,2,3}", obj);

		try {
			type = "varcharacter";

			dti.setName(type);
			dti.setPrecision(precision);
			dti.setScale(0);

			value = "abc";
			obj = CUBRIDFormator.format(dti, value).getFormatResult();
		} catch (Exception e) {
			Assert.assertEquals("Unsupported CUBRID data type:varcharacter",
					e.getMessage());
		}
	}
}
