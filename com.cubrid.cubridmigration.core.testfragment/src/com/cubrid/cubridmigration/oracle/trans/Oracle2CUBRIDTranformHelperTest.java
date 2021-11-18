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
package com.cubrid.cubridmigration.oracle.trans;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;
import com.cubrid.cubridmigration.core.mapping.model.VerifyInfo;
import com.cubrid.cubridmigration.core.trans.MigrationTransFactory;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.trans.ToCUBRIDDataConverterFacade;

public class Oracle2CUBRIDTranformHelperTest extends
		TestCase {
	static ToCUBRIDDataConverterFacade convertFactory = ToCUBRIDDataConverterFacade.getIntance();
	static Oracle2CUBRIDTranformHelper tranformHelper = (Oracle2CUBRIDTranformHelper) MigrationTransFactory.getTransformHelper(
			DatabaseType.ORACLE, DatabaseType.CUBRID);
	MigrationConfiguration config = new MigrationConfiguration();
	//private static final String MIGTESTFORHUDSON = "MIGTESTFORHUDSON";

	ConnParameters sourceConn = ConnParameters.getConParam("src", "", 8080, "",
			DatabaseType.ORACLE, "", "", "", "", null);
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
		config.setDestType(MigrationConfiguration.DEST_ONLINE);

		config = TemplateParserTest.getOracleConfig();
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * testConvertJdbcObjectToCubridObject
	 * 
	 * @throws ParseException
	 * 
	 * @e
	 */
	@Test
	public void testConvertJdbcObjectToCubridObject() throws ParseException {
		Column targetColumn = new Column();
		Object obj = null;

		dtHelper.setColumnDataType("smallint", targetColumn);
		Object res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertNull(res);

		dtHelper.setColumnDataType("smallint", targetColumn);
		obj = Short.MAX_VALUE;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Short.MAX_VALUE, res);
		obj = Short.MAX_VALUE;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Short.MAX_VALUE, res);

		dtHelper.setColumnDataType("integer", targetColumn);
		obj = (Integer) 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals((Integer) 1, res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals((Integer) 1, res);

		dtHelper.setColumnDataType("bigint", targetColumn);
		obj = (long) 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals((long) 1, res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals((long) 1, res);

		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setScale(0);
		obj = BigInteger.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(BigInteger.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(BigInteger.valueOf(1), res);

		dtHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setScale(1);
		obj = BigDecimal.valueOf(1.3);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(BigDecimal.valueOf(1.3), res);
		targetColumn.setPrecision(1);
		obj = 1.3;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(BigDecimal.valueOf(1.3), res);

		dtHelper.setColumnDataType("float", targetColumn);
		obj = Float.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Float.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Float.valueOf(1), res);

		dtHelper.setColumnDataType("double", targetColumn);
		obj = Double.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Double.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Double.valueOf(1), res);

		dtHelper.setColumnDataType("monetary", targetColumn);
		obj = Double.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Double.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Double.valueOf(1), res);

		dtHelper.setColumnDataType("character", targetColumn);
		obj = Boolean.TRUE;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("y", res);
		obj = "abc";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("character varying", targetColumn);
		obj = "abc";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("string", targetColumn);
		obj = "abc";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("string", targetColumn);
		obj = "abc".toCharArray();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("string", targetColumn);
		obj = "abc".getBytes();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("national character", targetColumn);
		obj = "abc".toCharArray();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("national character varying", targetColumn);
		obj = "abc".getBytes();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("abc", res);

		dtHelper.setColumnDataType("national character varying", targetColumn);
		res = convertFactory.convert(1, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("1", res);

		dtHelper.setColumnDataType("time", targetColumn);
		obj = Time.valueOf("10:09:08");
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Time.valueOf("10:09:08"), res);

		dtHelper.setColumnDataType("time", targetColumn);
		java.util.Date time = new java.util.Date();
		obj = time;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(time, res);

		Calendar ca = Calendar.getInstance();
		obj = ca;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(new Time(((Calendar) obj).getTime().getTime()), res);

		obj = "2010-02-24 18:00:46:968 CST";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Time.valueOf("18:00:46").toString(), res.toString());

		obj = "10:09:08";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(Time.valueOf("10:09:08"), res);

		dtHelper.setColumnDataType("date", targetColumn);
		java.util.Date date = new java.util.Date(System.currentTimeMillis());
		obj = date;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(date.getTime(), ((java.util.Date) res).getTime());

		ca = Calendar.getInstance();
		obj = ca;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(ca.getTime().getTime(), ((java.util.Date) res).getTime());

		obj = "2010-02-24 18:00:46:968 CST";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(1267005646968L, ((java.util.Date) res).getTime());

		obj = "2010-02-24 18:00:46:968 GMT";
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS z");
		Timestamp timeStamp = new Timestamp(format.parse(obj.toString()).getTime());
		timeStamp.setNanos(55);
		dtHelper.setColumnDataType("timestamp", targetColumn);
		obj = timeStamp;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(timeStamp.getTime(), ((Timestamp) res).getTime());
		Assert.assertEquals(timeStamp.getNanos(), ((Timestamp) res).getNanos());

		dtHelper.setColumnDataType("datetime", targetColumn);
		obj = ca;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);

		dtHelper.setColumnDataType("bit", targetColumn);
		byte[] bs = "abc".getBytes();
		obj = bs;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(bs, res);

		dtHelper.setColumnDataType("bit varying", targetColumn);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(obj, res);

		dtHelper.setColumnDataType("set_of(integer)", targetColumn);
		obj = "{1,2,3}";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("{1,2,3}", res); // no colum type map to set_of

		dtHelper.setColumnDataType("multiset_of(integer)", targetColumn);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);

		Assert.assertEquals("{1,2,3}", res); // no colum type map to multiset_of

		dtHelper.setColumnDataType("sequence_of(integer)", targetColumn);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(), config);

		Assert.assertEquals("{1,2,3}", res); // no colum type map to sequence_of

	}

	@Test
	public final void testConvertStringToObject() {

		Column targetColumn = new Column();
		dtHelper.setColumnDataType("smallint", targetColumn);

		String str = "true";
		Object obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("1", obj.toString());

		str = "false";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("0", obj.toString());

		str = "2";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals("2", obj.toString());

		targetColumn = new Column();
		dtHelper.setColumnDataType("TIMESTAMP", targetColumn);
		str = "2009-11-12 23:00:00:000 GMT";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(1258066800000L, ((Timestamp) obj).getTime());

		targetColumn = new Column();
		dtHelper.setColumnDataType("DATETIME", targetColumn);
		str = "2009-11-12 23:00:00:777 CST";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(), config);
		Assert.assertEquals(1258038000777L, ((Timestamp) obj).getTime());

		try {
			targetColumn = new Column();
			dtHelper.setColumnDataType("DATETIME", targetColumn);
			str = "2009-11-12 23:00:00,777 CST";
			obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(), config);
			Assert.assertEquals(1258038000777L, ((Time) obj).getTime());
		} catch (Exception e) {

		}
	}

	@Test
	public void testVerifyColumnDataType() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getOracleConfig();
		Table st = config.getSrcTableSchema(null, "TEST_STRING");
		Table tt = config.getTargetTableSchema("test_string");

		for (Column sc : st.getColumns()) {
			Column tc = tt.getColumnByName(sc.getName().toLowerCase(Locale.US));
			VerifyInfo result = tranformHelper.verifyColumnDataType(sc, tc, config);
			Assert.assertNotNull(result);
		}
		st = config.getSrcTableSchema(null, "TEST_NUMBER");
		tt = config.getTargetTableSchema("test_number");

		for (Column sc : st.getColumns()) {
			Column tc = tt.getColumnByName(sc.getName().toLowerCase(Locale.US));
			VerifyInfo result = tranformHelper.verifyColumnDataType(sc, tc, config);
			Assert.assertNotNull(result);
		}

		Column sc = st.getColumnByName("F1");
		sc.setDataType("NUMBER");
		sc.setShownDataType("NUMBER(38,0)");
		sc.setPrecision(38);
		sc.setScale(0);
		Column tc = tt.getColumnByName("f1");
		tc.setDataType("varchar");
		tc.setShownDataType("varchar(39)");
		tc.setPrecision(39);
		tc.setScale(0);
		VerifyInfo result = tranformHelper.verifyColumnDataType(sc, tc, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, result.getResult());

		sc.setDataType("NUMBER");
		sc.setShownDataType("NUMBER(38,2)");
		sc.setPrecision(38);
		sc.setScale(2);
		tc.setDataType("varchar");
		tc.setShownDataType("varchar(40)");
		tc.setPrecision(40);
		tc.setScale(0);
		result = tranformHelper.verifyColumnDataType(sc, tc, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, result.getResult());

		sc.setDataType("NUMBER");
		sc.setShownDataType("NUMBER(38,-2)");
		sc.setPrecision(38);
		sc.setScale(-2);
		tc.setDataType("varchar");
		tc.setShownDataType("varchar(41)");
		tc.setPrecision(41);
		tc.setScale(0);
		result = tranformHelper.verifyColumnDataType(sc, tc, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, result.getResult());

		sc.setDataType("NUMBER");
		sc.setShownDataType("NUMBER(38,40)");
		sc.setPrecision(38);
		sc.setScale(40);
		tc.setDataType("varchar");
		tc.setShownDataType("varchar(43)");
		tc.setPrecision(43);
		tc.setScale(0);
		result = tranformHelper.verifyColumnDataType(sc, tc, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, result.getResult());

		tc.setDataType("varchar");
		tc.setShownDataType("varchar(42)");
		tc.setPrecision(42);
		tc.setScale(0);
		result = tranformHelper.verifyColumnDataType(sc, tc, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH, result.getResult());

		tc.setDataType("blob");
		tc.setShownDataType("blob");
		tc.setPrecision(0);
		tc.setScale(0);
		result = tranformHelper.verifyColumnDataType(sc, tc, config);
		Assert.assertEquals(VerifyInfo.TYPE_NO_MATCH, result.getResult());
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
		String sql = "Select * from game;";
		String targetSql = tranformHelper.getFitTargetFormatSQL(sql);

		Assert.assertEquals(sql, targetSql);
	}

	@Test
	public void testAdjustPrecision() throws SQLException {
		Table table = new Table();
		table.setName("TEST");
		Column sourceColumn = new Column(table);
		sourceColumn.setName("TEST");
		sourceColumn.setDataType("CHAR");
		sourceColumn.setPrecision(20);
		sourceColumn.setShownDataType("CHAR(20)");
		table.addColumn(sourceColumn);

		//char
		sourceColumn.setDataType("CHAR");

		sourceColumn.setPrecision(20);
		sourceColumn.setDefaultValue("default");
		sourceColumn.setCharUsed("B");
		Column targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(20), targetColumn.getPrecision());
		sourceColumn.setCharUsed("C");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);

		sourceColumn.setDataType("VARCHAR2");
		sourceColumn.setPrecision(20);
		sourceColumn.setDefaultValue("default");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(20), targetColumn.getPrecision());

		sourceColumn.setDataType("NVARCHAR2");
		sourceColumn.setPrecision(20);
		sourceColumn.setDefaultValue("default");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(20), targetColumn.getPrecision());

		sourceColumn.setDataType("NCHAR");
		sourceColumn.setPrecision(20);
		sourceColumn.setDefaultValue("default");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(20), targetColumn.getPrecision());

		//NUMBER(10,5)
		sourceColumn.setDataType("NUMBER");
		sourceColumn.setPrecision(10);
		sourceColumn.setScale(5);
		sourceColumn.setDefaultValue("0.0");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(10), targetColumn.getPrecision());
		Assert.assertEquals(new Integer(5), targetColumn.getScale());

		//NUMBER(40,5)
		sourceColumn.setDataType("NUMBER");
		sourceColumn.setPrecision(40);
		sourceColumn.setScale(5);
		sourceColumn.setDefaultValue("100");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(42), targetColumn.getPrecision());
		Assert.assertEquals("varchar", targetColumn.getDataType());
		//Assert.assertEquals(new Integer(5), targetColumn.getScale());

		//NUMBER(40,-2)
		sourceColumn.setDataType("NUMBER");
		sourceColumn.setPrecision(40);
		sourceColumn.setScale(-2);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(43), targetColumn.getPrecision());
		Assert.assertEquals("varchar", targetColumn.getDataType());

		//NUMBER(12,-2)
		sourceColumn.setDataType("NUMBER");
		sourceColumn.setPrecision(12);
		sourceColumn.setScale(-2);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(14), targetColumn.getPrecision());
		Assert.assertEquals(new Integer(0), targetColumn.getScale());

		//NUMBER(38,39)
		sourceColumn.setDataType("NUMBER");
		sourceColumn.setPrecision(38);
		sourceColumn.setScale(39);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(42), targetColumn.getPrecision());
		Assert.assertEquals("varchar", targetColumn.getDataType());

		//NUMBER(8,10)
		sourceColumn.setDataType("NUMBER");
		sourceColumn.setPrecision(8);
		sourceColumn.setScale(10);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(10), targetColumn.getPrecision());
		Assert.assertEquals(new Integer(10), targetColumn.getScale());
		//BIT
		sourceColumn.setDataType("RAW");
		sourceColumn.setPrecision(2000);
		sourceColumn.setScale(null);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(2000 * 8), targetColumn.getPrecision());

		sourceColumn.setDataType("TIMESTAMP");
		sourceColumn.setPrecision(null);
		sourceColumn.setScale(null);
		sourceColumn.setDefaultValue("2012-01-01 01:01:01");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("2012-01-01 01:01:01", targetColumn.getDefaultValue());

		sourceColumn.setDataType("DATE");
		sourceColumn.setPrecision(null);
		sourceColumn.setScale(null);
		sourceColumn.setDefaultValue("2012-01-01 01:01:01");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("2012-01-01 01:01:01", targetColumn.getDefaultValue());

		sourceColumn.setDataType("TIMESTAMP");
		sourceColumn.setPrecision(null);
		sourceColumn.setScale(null);
		sourceColumn.setDefaultValue("sysdate");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("CURRENT_TIMESTAMP", targetColumn.getDefaultValue());

		sourceColumn.setDataType("DATE");
		sourceColumn.setPrecision(null);
		sourceColumn.setScale(null);
		sourceColumn.setDefaultValue("sysdate");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("CURRENT_TIMESTAMP", targetColumn.getDefaultValue());

		sourceColumn.setDataType("RAW");
		sourceColumn.setPrecision(100);
		sourceColumn.setScale(null);
		sourceColumn.setDefaultValue(null);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(800), targetColumn.getPrecision());

		sourceColumn.setDataType("RAW");
		sourceColumn.setPrecision(DataTypeConstant.CUBRID_MAXSIZE);
		sourceColumn.setScale(null);
		sourceColumn.setDefaultValue(null);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(DataTypeConstant.CUBRID_MAXSIZE),
				targetColumn.getPrecision());

		sourceColumn.setDataType("UROWID");
		MapObject mapping = new MapObject();
		mapping.setDatatype("UROWID");
		mapping.setPrecision(null);
		mapping.setScale(null);
		tranformHelper.adjustPrecision(sourceColumn, targetColumn, config);
		sourceColumn.setDataType("ROWID");
		tranformHelper.adjustPrecision(sourceColumn, targetColumn, config);
		sourceColumn.setDataType("INTERVAL YEAR");
		tranformHelper.adjustPrecision(sourceColumn, targetColumn, config);
		sourceColumn.setDataType("INTERVAL DAY");
		tranformHelper.adjustPrecision(sourceColumn, targetColumn, config);

	}

	@Test
	public void testValidateChar() throws Exception {
		Oracle2CUBRIDTranformHelper helper = (Oracle2CUBRIDTranformHelper) tranformHelper;
		MigrationConfiguration cfg = TemplateParserTest.getOracleConfig();
		Column sourceColumn = cfg.getSrcColumnSchema(null, "CODE", "F_NAME");
		Column targetColumn = cfg.getTargetTableSchema("code").getColumnByName("f_name");
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				helper.validateChar(sourceColumn, targetColumn, config).getResult());
		targetColumn.setPrecision(targetColumn.getPrecision() * 3);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH,
				helper.validateChar(sourceColumn, targetColumn, config).getResult());

		targetColumn.setJdbcIDOfDataType(DataTypeConstant.CUBRID_DT_NCHAR);
		targetColumn.setDataType("nchar");
		targetColumn.setPrecision(sourceColumn.getPrecision());
		Assert.assertEquals(VerifyInfo.TYPE_MATCH,
				helper.validateChar(sourceColumn, targetColumn, config).getResult());

		targetColumn.setPrecision(sourceColumn.getPrecision() - 1);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				helper.validateChar(sourceColumn, targetColumn, config).getResult());
	}
}
