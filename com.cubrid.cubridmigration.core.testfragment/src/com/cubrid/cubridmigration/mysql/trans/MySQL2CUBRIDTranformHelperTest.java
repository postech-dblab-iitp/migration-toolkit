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
package com.cubrid.cubridmigration.mysql.trans;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.TestUtil2;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.core.mapping.model.VerifyInfo;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.core.trans.MigrationTransFactory;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.trans.ToCUBRIDDataConverterFacade;
import com.cubrid.cubridmigration.mysql.meta.MySQLSchemaFetcher;

public class MySQL2CUBRIDTranformHelperTest extends
		TestCase {
	private static final String MIGTESTFORHUDSON = "migtestforhudson";
	private static ToCUBRIDDataConverterFacade convertFactory = ToCUBRIDDataConverterFacade.getIntance();
	private static DBTransformHelper tranformHelper = MigrationTransFactory.getTransformHelper(
			DatabaseType.MYSQL, DatabaseType.CUBRID);
	private MigrationConfiguration config = new MigrationConfiguration();
	ConnParameters sourceConn = ConnParameters.getConParam("src", "", 8080, "",
			DatabaseType.MYSQL, "", "", "", "", null);
	ConnParameters targetConn = ConnParameters.getConParam("tar", "", 8080, "",
			DatabaseType.CUBRID, "", "", "", "", null);
	CUBRIDDataTypeHelper dtHelper = CUBRIDDataTypeHelper.getInstance(null);

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		sourceConn.setTimeZone("Default");
		targetConn.setTimeZone("Default");

		sourceConn.setCharset("UTF-8");
		targetConn.setCharset("UTF-8");

		config.setSourceConParams(sourceConn);
		config.setTargetConParams(targetConn);

		config = TemplateParserTest.getMySQLConfig();
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void testConvertJdbcObjectToCubridObject() throws ParseException {
		Column targetColumn = new Column();
		Object obj = null;

		dtHelper.setColumnDataType("smallint", targetColumn);
		Object res = convertFactory.convert(obj,
				targetColumn.getDataTypeInstance(), config);
		Assert.assertNull(res);

		dtHelper.setColumnDataType("smallint", targetColumn);
		obj = Short.MAX_VALUE;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Short.MAX_VALUE, res);
		obj = Short.MAX_VALUE;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Short.MAX_VALUE, res);

		dtHelper.setColumnDataType("integer", targetColumn);
		obj = (Integer) 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals((Integer) 1, res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals((Integer) 1, res);

		dtHelper.setColumnDataType("bigint", targetColumn);
		obj = (long) 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals((long) 1, res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals((long) 1, res);

		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setScale(0);
		obj = BigInteger.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(BigInteger.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(BigInteger.valueOf(1), res);

		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setScale(1);
		obj = BigDecimal.valueOf(1.3);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(BigDecimal.valueOf(1.3), res);
		targetColumn.setPrecision(1);
		obj = 1.3;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(BigDecimal.valueOf(1.3), res);

		dtHelper.setColumnDataType("float", targetColumn);
		obj = Float.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Float.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Float.valueOf(1), res);

		dtHelper.setColumnDataType("double", targetColumn);
		obj = Double.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Double.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Double.valueOf(1), res);

		dtHelper.setColumnDataType("monetary", targetColumn);
		obj = Double.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Double.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Double.valueOf(1), res);

		dtHelper.setColumnDataType("character", targetColumn);
		obj = Boolean.TRUE;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("y", res);
		obj = "abc";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("character varying", targetColumn);
		obj = "abc";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("string", targetColumn);
		obj = "abc";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("string", targetColumn);
		obj = "abc".toCharArray();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("string", targetColumn);
		obj = "abc".getBytes();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("national character", targetColumn);
		obj = "abc".toCharArray();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("national character varying", targetColumn);
		obj = "abc".getBytes();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("national character varying", targetColumn);
		res = convertFactory.convert(1, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("1", res);

		dtHelper.setColumnDataType("time", targetColumn);
		obj = Time.valueOf("10:09:08");
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Time.valueOf("10:09:08"), res);

		dtHelper.setColumnDataType("time", targetColumn);
		java.util.Date time = new java.util.Date();
		obj = time;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(time, res);

		Calendar ca = Calendar.getInstance();
		obj = ca;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(new Time(((Calendar) obj).getTime().getTime()), res);

		obj = "2010-02-24 18:00:46:968 CST";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Time.valueOf("18:00:46").toString(), res.toString());

		obj = "10:09:08";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Time.valueOf("10:09:08"), res);

		dtHelper.setColumnDataType("date", targetColumn);
		java.util.Date date = new java.util.Date(System.currentTimeMillis());
		obj = date;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(date.getTime(), ((java.util.Date) res).getTime());

		ca = Calendar.getInstance();
		obj = ca;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(ca.getTime().getTime(),
				((java.util.Date) res).getTime());

		obj = "2010-02-24 18:00:46:968 CST";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(1267005646968L, ((java.util.Date) res).getTime());

		obj = "2010-02-24 18:00:46:968 GMT";
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS z");
		Timestamp timeStamp = new Timestamp(
				format.parse(obj.toString()).getTime());
		timeStamp.setNanos(55);
		dtHelper.setColumnDataType("timestamp", targetColumn);
		obj = timeStamp;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(timeStamp.getTime(), ((Timestamp) res).getTime());
		Assert.assertEquals(timeStamp.getNanos(), ((Timestamp) res).getNanos());

		dtHelper.setColumnDataType("datetime", targetColumn);
		obj = ca;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);

		dtHelper.setColumnDataType("bit", targetColumn);
		byte[] bs = "abc".getBytes();
		obj = bs;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(bs, res);

		dtHelper.setColumnDataType("bit varying", targetColumn);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(obj, res);

		dtHelper.setColumnDataType("set_of(integer)", targetColumn);
		obj = "{1,2,3}";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("{1,2,3}", res); // no colum type map to set_of

		dtHelper.setColumnDataType("multiset_of(integer)", targetColumn);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);

		Assert.assertEquals("{1,2,3}", res); // no colum type map to multiset_of

		dtHelper.setColumnDataType("sequence_of(integer)", targetColumn);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);

		Assert.assertEquals("{1,2,3}", res); // no colum type map to sequence_of

	}

	@Test
	public final void testConvertStringToObject() {

		Column targetColumn = new Column();
		dtHelper.setColumnDataType("smallint", targetColumn);

		String str = "true";
		Object obj = convertFactory.convert(str,
				targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("1", obj.toString());

		str = "false";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("0", obj.toString());

		str = "2";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("2", obj.toString());

		targetColumn = new Column();
		dtHelper.setColumnDataType("TIMESTAMP", targetColumn);
		str = "2009-11-12 23:00:00:000 GMT";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(1258066800000L, ((Timestamp) obj).getTime());

		targetColumn = new Column();
		dtHelper.setColumnDataType("DATETIME", targetColumn);
		str = "2009-11-12 23:00:00:777 CST";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(1258038000777L, ((Timestamp) obj).getTime());

		try {
			targetColumn = new Column();
			dtHelper.setColumnDataType("DATETIME", targetColumn);
			str = "2009-11-12 23:00:00,777 CST";
			obj = convertFactory.convert(str,
					targetColumn.getDataTypeInstance(), config);
			Assert.assertEquals(1258038000777L, ((Time) obj).getTime());
		} catch (Exception e) {

		}
	}

	@Test
	public void testVerifyColumnDataType() throws Exception {
		Column sourceColumn = null, targetColumn = null;
		VerifyInfo verifyInfo = null;
		/** bit(1) **/
		sourceColumn = new Column();
		sourceColumn.setPrecision(1);
		sourceColumn.setDataType("bit");
		// bit to smallint
		targetColumn = new Column();
		dtHelper.setColumnDataType("smallint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// bit1 to biti,this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("bit", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NO_MATCH, verifyInfo.getResult());

		/** bit(5) **/
		sourceColumn = new Column();
		sourceColumn.setPrecision(5);
		sourceColumn.setDataType("bit");
		//bit(5) to bit(5)
		targetColumn = new Column();
		dtHelper.setColumnDataType("bit", targetColumn);
		targetColumn.setPrecision(5);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//bit(5) to bit(4)
		targetColumn = new Column();
		dtHelper.setColumnDataType("bit", targetColumn);
		targetColumn.setPrecision(4);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		//bit to small int,this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("smallint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NO_MATCH, verifyInfo.getResult());

		/** tinyint to smallint */
		sourceColumn = new Column();
		sourceColumn.setDataType("tinyint");
		// test tinyint to smallint
		targetColumn = new Column();
		dtHelper.setColumnDataType("smallint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test tinyint to int
		targetColumn = new Column();
		dtHelper.setColumnDataType("int", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test tinyint to numeric(3,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(3);
		targetColumn.setScale(0);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//test tinyint to varchar(4)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(4);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** tinyint unsigned **/
		sourceColumn = new Column();
		sourceColumn.setDataType("tinyint unsigned");
		// test tinyint unsigned to smallint
		targetColumn = new Column();
		dtHelper.setColumnDataType("smallint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test tinyint to int
		targetColumn = new Column();
		dtHelper.setColumnDataType("int", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test tinyint to numeric(3,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(3);
		targetColumn.setScale(0);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//test tinyint to varchar(3)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(3);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** bool **/
		sourceColumn = new Column();
		sourceColumn.setDataType("bool");
		// test bool to smallint
		targetColumn = new Column();
		dtHelper.setColumnDataType("smallint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** boolean(1) **/
		sourceColumn = new Column();
		sourceColumn.setDataType("boolean");
		sourceColumn.setPrecision(1);
		// test boolean(1) to smallint
		targetColumn = new Column();
		dtHelper.setColumnDataType("smallint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** int **/
		sourceColumn = new Column();
		sourceColumn.setDataType("int");
		// test int to int
		targetColumn = new Column();
		dtHelper.setColumnDataType("int", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to bigint
		targetColumn = new Column();
		dtHelper.setColumnDataType("bigint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to numeric(10,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to varchar(11)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(11);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** smallint **/
		sourceColumn = new Column();
		sourceColumn.setDataType("smallint");
		// test smallint to smallint
		targetColumn = new Column();
		dtHelper.setColumnDataType("smallint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test smallint to int
		targetColumn = new Column();
		dtHelper.setColumnDataType("int", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to numeric(5,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(5);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//test smallint to varchar(6)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(6);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** smallint unsigned **/
		sourceColumn = new Column();
		sourceColumn.setDataType("smallint unsigned");
		// test smallint to integer
		targetColumn = new Column();
		dtHelper.setColumnDataType("int", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to numeric(5,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(5);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//test smallint unsigned to varchar(6)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(5);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** mediumint **/
		sourceColumn = new Column();
		sourceColumn.setDataType("mediumint");
		// test mediumint unsigned to integer
		targetColumn = new Column();
		dtHelper.setColumnDataType("integer", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test mediumint to numeric(7,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(7);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//test mediumint unsigned to varchar(8)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(8);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** mediumint unsigned **/
		sourceColumn = new Column();
		sourceColumn.setDataType("mediumint unsigned");
		// test mediumint unsigned to integer
		targetColumn = new Column();
		dtHelper.setColumnDataType("integer", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test mediumint unsigned to numeric(7,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(8);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//test mediumint unsigned unsigned to varchar(7)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(8);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** int **/
		sourceColumn = new Column();
		sourceColumn.setDataType("int");
		// test int to int
		targetColumn = new Column();
		dtHelper.setColumnDataType("int", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to integer
		targetColumn = new Column();
		dtHelper.setColumnDataType("integer", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to bigint
		targetColumn = new Column();
		dtHelper.setColumnDataType("bigint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to numeric(10,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to numeric(9,0),this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(9);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		// test int to varchar(11)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(11);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to varchar(10),this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());

		/** int unsigned **/
		sourceColumn = new Column();
		sourceColumn.setDataType("int unsigned");
		// test int unsigned to int,this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("int", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NO_MATCH, verifyInfo.getResult());
		// test int to bigint
		targetColumn = new Column();
		dtHelper.setColumnDataType("bigint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to numeric(10,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to varchar(10)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** bigint **/
		sourceColumn = new Column();
		sourceColumn.setDataType("bigint");
		// bigint to bigint
		targetColumn = new Column();
		dtHelper.setColumnDataType("bigint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// bigint to numeric(19,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(19);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// bigint to varchar(20)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(20);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** bigint unsigned **/
		sourceColumn = new Column();
		sourceColumn.setDataType("bigint unsigned");
		// bigint unsigned to numeric(20,0)
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(20);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// bigint unsigned to varchar(20)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(20);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** float ***/
		sourceColumn = new Column();
		sourceColumn.setDataType("float");
		// test float to float
		targetColumn = new Column();
		dtHelper.setColumnDataType("float", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test float to double,this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("double", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** float unsigned **/
		sourceColumn = new Column();
		sourceColumn.setDataType("float unsigned");
		// test float unsigned to float
		targetColumn = new Column();
		dtHelper.setColumnDataType("float", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test float unsigned to double,this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("double", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** double ***/
		sourceColumn = new Column();
		sourceColumn.setDataType("double");
		// test double to double
		targetColumn = new Column();
		dtHelper.setColumnDataType("double", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** double unsigned **/
		sourceColumn = new Column();
		sourceColumn.setDataType("double unsigned");
		// test double unsigned to double
		targetColumn = new Column();
		dtHelper.setColumnDataType("double", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** decimal **/
		sourceColumn = new Column();
		sourceColumn.setDataType("decimal");
		sourceColumn.setPrecision(10);
		sourceColumn.setScale(2);
		// test decimal to numeric
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(10);
		targetColumn.setScale(2);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// should failed
		// test decimal to numeric
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(15);
		targetColumn.setScale(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		// test decimal to numeric
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(15);
		targetColumn.setScale(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		//test decimal(2,1)to varchar(4)
		sourceColumn = new Column();
		sourceColumn.setDataType("decimal");
		sourceColumn.setPrecision(2);
		sourceColumn.setScale(1);

		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(4);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//test decimal(2,0)
		sourceColumn = new Column();
		sourceColumn.setDataType("decimal");
		sourceColumn.setPrecision(2);
		//to varchar(3)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(3);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** decimal unsigned **/
		sourceColumn = new Column();
		sourceColumn.setDataType("decimal unsigned");
		sourceColumn.setPrecision(10);
		sourceColumn.setScale(2);
		// test decimal to numeric
		targetColumn = new Column();
		dtHelper.setColumnDataType("decimal", targetColumn);
		targetColumn.setPrecision(10);
		targetColumn.setScale(2);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// should failed
		// test decimal to numeric
		targetColumn = new Column();
		dtHelper.setColumnDataType("decimal", targetColumn);
		targetColumn.setPrecision(15);
		targetColumn.setScale(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		//test decimal(2,1)
		sourceColumn = new Column();
		sourceColumn.setDataType("decimal unsigned");
		sourceColumn.setPrecision(2);
		sourceColumn.setScale(1);
		//to varchar(3)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(3);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//to varchar(2),this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(2);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());

		//test decimal(2,0)
		sourceColumn = new Column();
		sourceColumn.setDataType("decimal");
		sourceColumn.setPrecision(2);
		//to varchar(3)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(3);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//to varchar(2),this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(2);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());

		/** numeric **/
		sourceColumn = new Column();
		sourceColumn.setDataType("numeric");
		sourceColumn.setPrecision(10);
		sourceColumn.setScale(2);
		// test numeric to numeric
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(10);
		targetColumn.setScale(2);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// should failed
		// test numeric to numeric
		targetColumn = new Column();
		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(15);
		targetColumn.setScale(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		//test numeric(2,1)to varchar(4)
		sourceColumn = new Column();
		sourceColumn.setDataType("numeric");
		sourceColumn.setPrecision(2);
		sourceColumn.setScale(1);

		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(4);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//to varchar(3),this should failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(3);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());

		//test numeric(2,0)to varchar(3)
		sourceColumn = new Column();
		sourceColumn.setDataType("numeric");
		sourceColumn.setPrecision(2);

		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(3);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** date **/
		sourceColumn = new Column();
		sourceColumn.setDataType("date");
		// test date to date
		targetColumn = new Column();
		dtHelper.setColumnDataType("date", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** datetime **/
		sourceColumn = new Column();
		sourceColumn.setDataType("datetime");
		// test datetime to datetime
		targetColumn = new Column();
		dtHelper.setColumnDataType("datetime", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** timestamp **/
		sourceColumn = new Column();
		sourceColumn.setDataType("timestamp");
		// test timestamp to timestamp
		targetColumn = new Column();
		dtHelper.setColumnDataType("timestamp", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** time **/
		sourceColumn = new Column();
		sourceColumn.setDataType("time");
		// test timestamp to timestamp
		targetColumn = new Column();
		dtHelper.setColumnDataType("time", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** year **/
		sourceColumn = new Column();
		sourceColumn.setDataType("year");
		// test year to varchar(4)
		targetColumn = new Column();
		dtHelper.setColumnDataType("character", targetColumn);
		targetColumn.setPrecision(4);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** --------------char----------------- **/
		sourceColumn = new Column();
		sourceColumn.setDataType("char");
		sourceColumn.setPrecision(10);
		sourceColumn.setCharset("UTF-8");
		// to cubrid character(10),this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("character", targetColumn);
		targetColumn.setPrecision(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		// to cubrid character(30),this should be success
		targetColumn = new Column();
		dtHelper.setColumnDataType("character", targetColumn);
		targetColumn.setPrecision(30);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//to cubrid varchar(30)
		targetColumn = new Column();
		dtHelper.setColumnDataType("varchar", targetColumn);
		targetColumn.setPrecision(30);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** varchar **/
		sourceColumn = new Column();
		sourceColumn.setDataType("varchar");
		sourceColumn.setPrecision(3);
		sourceColumn.setCharset("UTF-8");
		// to cubrid character varying(5),this should be failed
		targetColumn = new Column();
		dtHelper.setColumnDataType("character varying", targetColumn);
		targetColumn.setPrecision(5);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		// to cubrid character varying(9)
		targetColumn = new Column();
		dtHelper.setColumnDataType("character varying", targetColumn);
		targetColumn.setPrecision(9);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** tinytext **/
		sourceColumn = new Column();
		sourceColumn.setDataType("tinytext");
		// to cubrid character varying(255)
		targetColumn = new Column();
		dtHelper.setColumnDataType("character varying", targetColumn);
		targetColumn.setPrecision(255);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** text **/
		sourceColumn = new Column();
		sourceColumn.setDataType("text");
		// to cubrid character varying(65535)
		targetColumn = new Column();
		dtHelper.setColumnDataType("character varying", targetColumn);
		targetColumn.setPrecision(65535);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** mediumtext **/
		sourceColumn = new Column();
		sourceColumn.setDataType("mediumtext");
		// to cubrid character varying(16277215)
		targetColumn = new Column();
		dtHelper.setColumnDataType("character varying", targetColumn);
		targetColumn.setPrecision(16277215);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** longtext **/
		sourceColumn = new Column();
		sourceColumn.setDataType("longtext");
		// to cubrid character varying(1073741823)
		targetColumn = new Column();
		dtHelper.setColumnDataType("character varying", targetColumn);
		targetColumn.setPrecision(1073741823);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** tinyblob **/
		sourceColumn = new Column();
		sourceColumn.setDataType("tinyblob");
		// to cubrid varbit(255)
		targetColumn = new Column();
		dtHelper.setColumnDataType("bit varying", targetColumn);
		targetColumn.setPrecision(2040);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// to cubrid blob
		targetColumn = new Column();
		dtHelper.setColumnDataType("blob", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** blob **/
		sourceColumn = new Column();
		sourceColumn.setDataType("blob");
		// to cubrid bit varying(65535)
		targetColumn = new Column();
		dtHelper.setColumnDataType("bit varying", targetColumn);
		targetColumn.setPrecision(65535 * 8);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// to cubrid blob
		targetColumn = new Column();
		dtHelper.setColumnDataType("blob", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** mediumblob **/
		sourceColumn = new Column();
		sourceColumn.setDataType("mediumblob");
		// to cubrid bit varying(16277215)
		targetColumn = new Column();
		dtHelper.setColumnDataType("bit varying", targetColumn);
		targetColumn.setPrecision(1073741823);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// to cubrid blob
		targetColumn = new Column();
		dtHelper.setColumnDataType("blob", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** longblob **/
		sourceColumn = new Column();
		sourceColumn.setDataType("longblob");
		// to cubrid bit varying(1073741823)
		targetColumn = new Column();
		dtHelper.setColumnDataType("bit varying", targetColumn);
		targetColumn.setPrecision(1073741823);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// to cubrid blob
		targetColumn = new Column();
		dtHelper.setColumnDataType("blob", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** binary **/
		sourceColumn = new Column();
		sourceColumn.setDataType("binary");
		sourceColumn.setPrecision(3);
		// to cubrid bit(3)
		targetColumn = new Column();
		dtHelper.setColumnDataType("bit", targetColumn);
		targetColumn.setPrecision(3);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** varbinary **/
		sourceColumn = new Column();
		sourceColumn.setDataType("varbinary");
		sourceColumn.setPrecision(3);
		// to cubrid bit(3)
		targetColumn = new Column();
		dtHelper.setColumnDataType("bit varying", targetColumn);
		targetColumn.setPrecision(3);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** enum **/
		sourceColumn = new Column();
		sourceColumn.setDataType("enum");
		// to cubrid character varying(255)
		targetColumn = new Column();
		dtHelper.setColumnDataType("character varying", targetColumn);
		targetColumn.setPrecision(255);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** set ***/
		sourceColumn = new Column();
		sourceColumn.setDataType("set");
		// to cubrid character varying(765)
		targetColumn = new Column();
		dtHelper.setColumnDataType("set(varchar(255))", targetColumn);
		//targetColumn.setPrecision(255);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		// to cubrid varchar(255)
		targetColumn = new Column();
		dtHelper.setColumnDataType("set_of(varchar(255))", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
	}

	@Test
	public void testCloneView() throws CloneNotSupportedException {
		View sourceView = new View();
		sourceView.setName("sourceView");
		sourceView.setQuerySpec("Select * from game;");

		View targetView = tranformHelper.getCloneView(sourceView, config);
		Assert.assertEquals(sourceView.getName(), targetView.getName());
	}

	@Test
	public void testGetToCUBRIDDDL() {
		String sql = "Select * from `game`;";
		String targetSql = tranformHelper.getFitTargetFormatSQL(sql);

		Assert.assertEquals("Select * from \"game\";", targetSql);
		Assert.assertEquals("", tranformHelper.getFitTargetFormatSQL(null));
	}

	@Test
	public void testAdjustPrecision() throws SQLException {
		Column sourceColumn = new Column();
		Table table = new Table();
		table.addColumn(sourceColumn);

		Connection conn = TestUtil2.getMySQL5520Conn();
		Catalog catalog = new MySQLSchemaFetcher().buildCatalog(conn,
				TestUtil2.getMySQLConParam(), null);

		Schema schema = new Schema(catalog);
		schema.setName(MIGTESTFORHUDSON);
		schema.addTable(table);

		//char
		sourceColumn.setDataType("char");
		sourceColumn.setPrecision(20);
		Column targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn,
				config);
		Assert.assertEquals(new Integer(20), targetColumn.getPrecision());

		//year
		sourceColumn.setDataType("year");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(4), targetColumn.getPrecision());

		//numeric(10,5)
		sourceColumn.setDataType("numeric");
		sourceColumn.setPrecision(10);
		sourceColumn.setScale(5);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(10), targetColumn.getPrecision());
		Assert.assertEquals(new Integer(5), targetColumn.getScale());

		//numeric(63,5)
		sourceColumn.setDataType("numeric");
		sourceColumn.setPrecision(63);
		sourceColumn.setScale(5);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(38), targetColumn.getPrecision());
		Assert.assertEquals(new Integer(5), targetColumn.getScale());

		//bit(1)
		sourceColumn.setDataType("bit");
		sourceColumn.setPrecision(1);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("short", targetColumn.getDataType());
		//bit(3)
		sourceColumn.setDataType("bit");
		sourceColumn.setPrecision(3);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("bit", targetColumn.getDataType());
		Assert.assertEquals(new Integer(8), targetColumn.getPrecision());

		//varbinary
		sourceColumn.setDataType("varbinary");
		sourceColumn.setPrecision(1073741823);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(DataTypeConstant.CUBRID_MAXSIZE),
				targetColumn.getPrecision());

		sourceColumn.setDataType("int");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("int", targetColumn.getDataType());

		sourceColumn.setDataType("DATETIME");
		sourceColumn.setDefaultValue("0000-00-00 00:00:00.000");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("datetime", targetColumn.getDataType());
		sourceColumn.setDefaultValue("2000-01-01 00:00:01.000");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);

		sourceColumn.setDataType("DATE");
		sourceColumn.setDefaultValue("0000-00-00");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("date", targetColumn.getDataType());
		sourceColumn.setDefaultValue("2000-01-01");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("date", targetColumn.getDataType());

		sourceColumn.setDataType("TIMESTAMP");
		sourceColumn.setDefaultValue("0000-00-00 00:00:00");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("timestamp", targetColumn.getDataType());
		sourceColumn.setDefaultValue("2000-00-00 00:00:01");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("timestamp", targetColumn.getDataType());

		sourceColumn.setDataType("TIME");
		sourceColumn.setDefaultValue("00:00:00.000");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("time", targetColumn.getDataType());
		sourceColumn.setDefaultValue("999:99:99.000");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("time", targetColumn.getDataType());

		/** set ***/
		sourceColumn = new Column();
		sourceColumn.setDataType("set");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("set", targetColumn.getDataType());
	}

}
