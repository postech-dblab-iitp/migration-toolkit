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
package com.cubrid.cubridmigration.cubrid.trans;

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
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.core.mapping.model.VerifyInfo;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.core.trans.MigrationTransFactory;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.meta.CUBRIDSchemaFetcher;

public class CUBRID2CUBRIDTranformHelperTest extends
		TestCase {
	static ToCUBRIDDataConverterFacade convertFactory = ToCUBRIDDataConverterFacade.getIntance();
	static DBTransformHelper tranformHelper = MigrationTransFactory.getTransformHelper(
			DatabaseType.CUBRID, DatabaseType.CUBRID);
	CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
	MigrationConfiguration config = new MigrationConfiguration();
	//private static final String MIGTESTFORHUDSON = "migtestforhudson";

	ConnParameters sourceConn = ConnParameters.getConParam("src", "", 8080, "",
			DatabaseType.CUBRID, "", "", "", "", null);
	ConnParameters targetConn = ConnParameters.getConParam("tar", "", 8080, "",
			DatabaseType.CUBRID, "", "", "", "", null);

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		sourceConn.setTimeZone("Default");
		targetConn.setTimeZone("Default");

		sourceConn.setCharset("UTF-8");
		targetConn.setCharset("UTF-8");

		config.setSourceConParams(sourceConn);
		config.setTargetConParams(targetConn);
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

		dataTypeHelper.setColumnDataType("smallint", targetColumn);
		Object res = convertFactory.convert(obj,
				targetColumn.getDataTypeInstance(), config);
		Assert.assertNull(res);

		dataTypeHelper.setColumnDataType("smallint", targetColumn);
		obj = Short.MAX_VALUE;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Short.MAX_VALUE, res);
		obj = Short.MAX_VALUE;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Short.MAX_VALUE, res);

		dataTypeHelper.setColumnDataType("integer", targetColumn);
		obj = (Integer) 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals((Integer) 1, res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals((Integer) 1, res);

		dataTypeHelper.setColumnDataType("bigint", targetColumn);
		obj = (long) 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals((long) 1, res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals((long) 1, res);

		dataTypeHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setScale(0);
		obj = BigInteger.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(BigInteger.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(BigInteger.valueOf(1), res);

		dataTypeHelper.setColumnDataType("numeric", targetColumn);
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

		dataTypeHelper.setColumnDataType("float", targetColumn);
		obj = Float.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Float.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Float.valueOf(1), res);

		dataTypeHelper.setColumnDataType("double", targetColumn);
		obj = Double.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Double.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Double.valueOf(1), res);

		dataTypeHelper.setColumnDataType("monetary", targetColumn);
		obj = Double.valueOf(1);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Double.valueOf(1), res);
		obj = 1;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Double.valueOf(1), res);

		dataTypeHelper.setColumnDataType("character", targetColumn);
		obj = Boolean.TRUE;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("y", res);
		obj = "abc";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dataTypeHelper.setColumnDataType("character varying", targetColumn);
		obj = "abc";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dataTypeHelper.setColumnDataType("string", targetColumn);
		obj = "abc";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dataTypeHelper.setColumnDataType("string", targetColumn);
		obj = "abc".toCharArray();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dataTypeHelper.setColumnDataType("string", targetColumn);
		obj = "abc".getBytes();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dataTypeHelper.setColumnDataType("national character", targetColumn);
		obj = "abc".toCharArray();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dataTypeHelper.setColumnDataType("national character varying",
				targetColumn);
		obj = "abc".getBytes();
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("abc", res);

		dataTypeHelper.setColumnDataType("national character varying",
				targetColumn);
		res = convertFactory.convert(1, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("1", res);

		dataTypeHelper.setColumnDataType("time", targetColumn);
		obj = Time.valueOf("10:09:08");
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(Time.valueOf("10:09:08"), res);

		dataTypeHelper.setColumnDataType("time", targetColumn);
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

		dataTypeHelper.setColumnDataType("date", targetColumn);
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
		dataTypeHelper.setColumnDataType("timestamp", targetColumn);
		obj = timeStamp;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(timeStamp.getTime(), ((Timestamp) res).getTime());
		Assert.assertEquals(timeStamp.getNanos(), ((Timestamp) res).getNanos());

		dataTypeHelper.setColumnDataType("datetime", targetColumn);
		obj = ca;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);

		dataTypeHelper.setColumnDataType("bit", targetColumn);
		byte[] bs = "abc".getBytes();
		obj = bs;
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(bs, res);

		dataTypeHelper.setColumnDataType("bit varying", targetColumn);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(obj, res);

		dataTypeHelper.setColumnDataType("set_of(integer)", targetColumn);
		obj = "{1,2,3}";
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals("{1,2,3}", res); // no colum type map to set_of

		dataTypeHelper.setColumnDataType("multiset_of(integer)", targetColumn);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);

		Assert.assertEquals("{1,2,3}", res); // no colum type map to multiset_of

		dataTypeHelper.setColumnDataType("sequence_of(integer)", targetColumn);
		res = convertFactory.convert(obj, targetColumn.getDataTypeInstance(),
				config);

		Assert.assertEquals("{1,2,3}", res); // no colum type map to sequence_of

	}

	@Test
	public final void testConvertStringToObject() {

		Column targetColumn = new Column();
		dataTypeHelper.setColumnDataType("smallint", targetColumn);

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
		dataTypeHelper.setColumnDataType("TIMESTAMP", targetColumn);
		str = "2009-11-12 23:00:00:000 GMT";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(1258066800000L, ((Timestamp) obj).getTime());

		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("DATETIME", targetColumn);
		str = "2009-11-12 23:00:00:777 CST";
		obj = convertFactory.convert(str, targetColumn.getDataTypeInstance(),
				config);
		Assert.assertEquals(1258038000777L, ((Timestamp) obj).getTime());

		try {
			targetColumn = new Column();
			dataTypeHelper.setColumnDataType("DATETIME", targetColumn);
			str = "2009-11-12 23:00:00,777 CST";
			obj = convertFactory.convert(str,
					targetColumn.getDataTypeInstance(), config);
			Assert.assertEquals(1258038000777L, ((Time) obj).getTime());
		} catch (Exception e) {

		}
	}

	@Test
	public void testVerifyColumnDataType() {
		Column sourceColumn = null, targetColumn = null;
		VerifyInfo verifyInfo = null;
		/** bit(1) **/
		sourceColumn = new Column();
		sourceColumn.setPrecision(1);
		sourceColumn.setDataType("bit");

		// bit1 to bit(1)
		targetColumn = new Column();
		targetColumn.setPrecision(1);
		dataTypeHelper.setColumnDataType("bit", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());

		//bit to small int,this should be failed
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("smallint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NO_MATCH, verifyInfo.getResult());

		/** int **/
		sourceColumn = new Column();
		sourceColumn.setDataType("int");
		// test int to int
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("int", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** smallint **/
		sourceColumn = new Column();
		sourceColumn.setDataType("smallint");
		// test smallint to smallint
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("smallint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** int **/
		sourceColumn = new Column();
		sourceColumn.setDataType("int");
		// test int to int
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("int", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// test int to integer
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("integer", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** bigint **/
		sourceColumn = new Column();
		sourceColumn.setDataType("bigint");
		// bigint to bigint
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("bigint", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** float ***/
		sourceColumn = new Column();
		sourceColumn.setDataType("float");
		// test float to float
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("float", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** double ***/
		sourceColumn = new Column();
		sourceColumn.setDataType("double");
		// test double to double
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("double", targetColumn);
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
		dataTypeHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(10);
		targetColumn.setScale(2);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// should failed
		// test decimal to numeric
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(15);
		targetColumn.setScale(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		// test decimal to numeric
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(15);
		targetColumn.setScale(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());
		//test decimal(2,0)
		sourceColumn = new Column();
		sourceColumn.setDataType("decimal");
		sourceColumn.setPrecision(2);
		// test decimal to numeric
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("decimal", targetColumn);
		targetColumn.setPrecision(10);
		targetColumn.setScale(2);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// should failed
		// test decimal to numeric
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("decimal", targetColumn);
		targetColumn.setPrecision(15);
		targetColumn.setScale(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** numeric **/
		sourceColumn = new Column();
		sourceColumn.setDataType("numeric");
		sourceColumn.setPrecision(10);
		sourceColumn.setScale(2);
		// test numeric to numeric
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(10);
		targetColumn.setScale(2);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// should failed
		// test numeric to numeric
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("numeric", targetColumn);
		targetColumn.setPrecision(15);
		targetColumn.setScale(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());

		/** date **/
		sourceColumn = new Column();
		sourceColumn.setDataType("date");
		// test date to date
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("date", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** datetime **/
		sourceColumn = new Column();
		sourceColumn.setDataType("datetime");
		// test datetime to datetime
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("datetime", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** timestamp **/
		sourceColumn = new Column();
		sourceColumn.setDataType("timestamp");
		// test timestamp to timestamp
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("timestamp", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** time **/
		sourceColumn = new Column();
		sourceColumn.setDataType("time");
		// test timestamp to timestamp
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("time", targetColumn);
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
		dataTypeHelper.setColumnDataType("character", targetColumn);
		targetColumn.setPrecision(10);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// to cubrid character(30),this should be success
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("character", targetColumn);
		targetColumn.setPrecision(30);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		//to cubrid varchar(30)
		//		targetColumn = new Column();
		//		dataTypeHelper.setColumnDataType("varchar", targetColumn);
		//		targetColumn.setPrecision(30);
		//		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
		//				targetColumn, config);
		//		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** varchar **/
		sourceColumn = new Column();
		sourceColumn.setDataType("varchar");
		sourceColumn.setPrecision(3);
		sourceColumn.setCharset("UTF-8");
		// to cubrid character varying(5),this should be failed
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("character varying", targetColumn);
		targetColumn.setPrecision(5);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());
		// to cubrid character varying(2)
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("character varying", targetColumn);
		targetColumn.setPrecision(2);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NOENOUGH_LENGTH,
				verifyInfo.getResult());

		/** blob **/
		sourceColumn = new Column();
		sourceColumn.setDataType("blob");
		// to cubrid bit varying(65535)
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("bit varying", targetColumn);
		targetColumn.setPrecision(65535);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_NO_MATCH, verifyInfo.getResult());
		// to cubrid blob
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("blob", targetColumn);
		verifyInfo = tranformHelper.verifyColumnDataType(sourceColumn,
				targetColumn, config);
		Assert.assertEquals(VerifyInfo.TYPE_MATCH, verifyInfo.getResult());

		/** bit varying **/
		sourceColumn = new Column();
		sourceColumn.setDataType("bit varying");
		sourceColumn.setPrecision(3);
		// to cubrid bit(3)
		targetColumn = new Column();
		dataTypeHelper.setColumnDataType("bit varying", targetColumn);
		targetColumn.setPrecision(3);
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
		String sql = "Select * from \"game\";";
		String targetSql = tranformHelper.getFitTargetFormatSQL(sql);

		Assert.assertEquals(sql, targetSql);
	}

	@Test
	public void testAdjustPrecision() throws SQLException {
		Table table = new Table();
		table.setName("test");
		Column sourceColumn = new Column(table);
		sourceColumn.setName("test");
		sourceColumn.setDataType("char");
		sourceColumn.setPrecision(20);
		sourceColumn.setShownDataType("char(20)");
		table.addColumn(sourceColumn);

		Connection conn = null;
		Catalog catalog = null;
		try {
			conn = TestUtil2.getCUBRIDConn();
			catalog = new CUBRIDSchemaFetcher().buildCatalog(conn,
					TestUtil2.getCUBRIDConnParam(), null);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			Closer.close(conn);
		}

		catalog.getSchemas().get(0).addTable(table);
		config.setSourceType(DatabaseType.CUBRID.getName());
		config.setSrcCatalog(catalog, true);
		//char

		sourceColumn.setDataType("char");
		sourceColumn.setPrecision(20);
		Column targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn,
				config);
		Assert.assertEquals(new Integer(20), targetColumn.getPrecision());

		//numeric(10,5)
		sourceColumn.setDataType("numeric");
		sourceColumn.setPrecision(10);
		sourceColumn.setScale(5);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(10), targetColumn.getPrecision());
		Assert.assertEquals(new Integer(5), targetColumn.getScale());

		//numeric(38)
		sourceColumn.setDataType("numeric");
		sourceColumn.setPrecision(38);
		sourceColumn.setScale(5);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals(new Integer(38), targetColumn.getPrecision());
		Assert.assertEquals(new Integer(5), targetColumn.getScale());

		//bit(1)
		sourceColumn.setDataType("bit");
		sourceColumn.setPrecision(1);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("bit", targetColumn.getDataType());
		//bit(3)
		sourceColumn.setDataType("bit");
		sourceColumn.setPrecision(3);
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("bit", targetColumn.getDataType());
		Assert.assertEquals(new Integer(3), targetColumn.getPrecision());

		sourceColumn.setDataType("int");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("int", targetColumn.getDataType());

		sourceColumn.setDataType("DATETIME");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("datetime", targetColumn.getDataType());

		sourceColumn.setDataType("TIMESTAMP");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("timestamp", targetColumn.getDataType());

		sourceColumn.setDataType("TIME");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("time", targetColumn.getDataType());

		sourceColumn.setDataType("clob");
		targetColumn = tranformHelper.getCUBRIDColumn(sourceColumn, config);
		Assert.assertEquals("clob", targetColumn.getDataType());
	}

	@Test
	public void testValidateCollection() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getCubridConfig();
		CUBRID2CUBRIDTranformHelper helper = (CUBRID2CUBRIDTranformHelper) tranformHelper;
		Column targetColumn = config.getSrcColumnSchema(null, "code", "f_name");
		targetColumn.setDataType("set");
		targetColumn.setSubDataType("integer");
		Column sourceColumn = config.getTargetTableSchema("code").getColumnByName(
				"f_name");
		sourceColumn.setDataType("set");
		sourceColumn.setSubDataType("integer");
		Assert.assertEquals(
				VerifyInfo.TYPE_MATCH,
				helper.validateCollection(sourceColumn, targetColumn, config).getResult());

		sourceColumn.setDataType("sequence");
		sourceColumn.setSubDataType("integer");
		Assert.assertEquals(
				VerifyInfo.TYPE_NO_MATCH,
				helper.validateCollection(sourceColumn, targetColumn, config).getResult());
	}
}
