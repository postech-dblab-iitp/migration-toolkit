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
package com.cubrid.cubridmigration.core.mapping;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.DocumentException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.mapping.model.MapItem;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;
import com.cubrid.cubridmigration.core.trans.MigrationTransFactory;

/**
 * 
 * DbUtilTest
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-18
 */
public class DataTypeMappingTest {
	MigrationConfiguration config = new MigrationConfiguration();

	/**
	 * setUp
	 * 
	 * @throws Exception e
	 */
	@Before
	public void setUp() throws Exception {

	}

	/**
	 * test DataTypeMapping
	 * 
	 * @throws MalformedURLException e
	 * @throws DocumentException e
	 */
	@Test
	public void testDataTypeMapping() throws MalformedURLException,
			DocumentException {

		String datatype = null;
		Integer precision = 0;
		Integer scale = 0;
		MapObject target = null;

		datatype = "bit";
		precision = 1;
		scale = 0;
		AbstractDataTypeMappingHelper dataTypeMappingHelper = MigrationTransFactory.getTransformHelper(
				DatabaseType.MYSQL, DatabaseType.CUBRID).getDataTypeMappingHelper();
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("short", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "bit";
		precision = 2;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("bit", target.getDatatype());
		Assert.assertEquals("n", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "date";
		precision = 10;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("date", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "datetime";
		precision = 19;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("datetime", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "timestamp";
		precision = 19;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("timestamp", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "time";
		precision = 8;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("time", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "year";
		precision = 4;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("char", target.getDatatype());
		Assert.assertEquals("4", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "year";
		precision = 2;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("char", target.getDatatype());
		Assert.assertEquals("4", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "char";
		precision = 2;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("char", target.getDatatype());
		Assert.assertEquals("n", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "varchar";
		precision = 4;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("varchar", target.getDatatype());
		Assert.assertEquals("n", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "binary";
		precision = 1;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("bit", target.getDatatype());
		Assert.assertEquals("n", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "binary";
		precision = 4;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("bit", target.getDatatype());
		Assert.assertEquals("n", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "bool";
		precision = 1;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("short", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "boolean";
		precision = 1;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("short", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "tinyint";
		precision = 1;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("short", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "smallint";
		precision = 3;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("short", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "smallint unsigned";
		precision = 5;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("int", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "mediumint";
		precision = 9;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("int", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "mediumint unsigned";
		precision = 9;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("int", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "int";
		precision = 11;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("int", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "int unsigned";
		precision = 11;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("bigint", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "bigint";
		precision = 20;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("bigint", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "bigint unsigned";
		precision = 20;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("numeric", target.getDatatype());
		Assert.assertEquals("20", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "float";
		precision = 10;
		scale = 1;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("float", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "double";
		precision = 10;
		scale = 1;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("double", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "decimal";
		precision = 10;
		scale = 1;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("numeric", target.getDatatype());
		Assert.assertEquals("p", target.getPrecision());
		Assert.assertEquals("s", target.getScale());

		datatype = "tinyblob";
		precision = 255;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("bit varying", target.getDatatype());
		Assert.assertEquals("2040", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "blob";
		precision = 65535;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("blob", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "mediumblob";
		precision = 16277215;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("blob", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "longblob";
		precision = 2147483647;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("blob", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "tinytext";
		precision = 255;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("varchar", target.getDatatype());
		Assert.assertEquals("255", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "text";
		precision = 65535;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("varchar", target.getDatatype());
		Assert.assertEquals("65535", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "mediumtext";
		precision = 16277215;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("varchar", target.getDatatype());
		Assert.assertEquals("16277215", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "longtext";
		precision = 2147483647;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("varchar", target.getDatatype());
		Assert.assertEquals("1073741823", target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "enum";
		precision = 2;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("enum", target.getDatatype());
		Assert.assertEquals(null, target.getPrecision());
		Assert.assertEquals(null, target.getScale());

		datatype = "set";
		precision = 2;
		scale = 0;
		target = dataTypeMappingHelper.getTargetFromPreference(datatype,
				precision, scale);
		Assert.assertEquals("set(varchar)", target.getDatatype());
		Assert.assertEquals("255", target.getPrecision());
		Assert.assertEquals(null, target.getScale());
	}

	//	/**
	//	 * test CloneAndMapping
	//	 * 
	//	 * @throws Exception e
	//	 */
	//	@Test
	//	public final void testCloneAndMapping() throws Exception {
	//		MYSQLSchemaFetcher helper = new MYSQLSchemaFetcher();
	//		//CUBRIDDDLUtil util = new CUBRIDDDLUtil();
	//		Connection conn = TestUtil2.getMySQL5520Conn();
	//		Catalog catalog = new Catalog();
	//		catalog.setName("migtestforhudson");
	//		//		Version version = new MYSQLSchemaFetcher().getVersion(conn);
	//		//		catalog.setVersion(version);
	//		Schema schema = new Schema(catalog);
	//		schema.setName("migtestforhudson");
	//
	//		Table table1 = new Table(schema);
	//		table1.setName("test_number");
	//		helper.buildTableColumns(conn, catalog, schema, table1);
	//
	//		Table table2 = new Table(schema);
	//		table2.setName("test_string");
	//		helper.buildTableColumns(conn, catalog, schema, table2);
	//
	//		Table table3 = new Table(schema);
	//		table3.setName("test_time");
	//		helper.buildTableColumns(conn, catalog, schema, table3);
	//
	//		Table table4 = new Table(schema);
	//		table4.setName("test_binary");
	//		helper.buildTableColumns(conn, catalog, schema, table4);
	//		//helper.buildTablePK(conn, catalog, schema, table4);
	//
	//		Closer.close(conn);
	//	}

	@Test
	public void testAbstractDataTypeMappingHelper() throws Exception {
		AbstractDataTypeMappingHelper dataTypeMappingHelper = MigrationTransFactory.getTransformHelper(
				DatabaseType.MYSQL, DatabaseType.CUBRID).getDataTypeMappingHelper();
		InputStream is = AbstractDataTypeMappingHelper.class.getResourceAsStream("/com/cubrid/cubridmigration/mysql/trans/MySQL2CUBRID.xml");
		InputStreamReader reader = new InputStreamReader(is, "utf-8");
		char[] buf = new char[1];
		StringBuffer sb = new StringBuffer();
		while (reader.read(buf) > 0) {
			sb.append(buf[0]);
		}
		dataTypeMappingHelper.loadFromPreference(sb.toString());

		dataTypeMappingHelper.getXmlConfigMapItem("varchar", "200", null);
		dataTypeMappingHelper.getXmlConfigMapItem("integer", null, null);
		dataTypeMappingHelper.getXmlConfigMapItem("number", "38", "2");
		dataTypeMappingHelper.getXmlConfigMap();
		dataTypeMappingHelper.getTargetFromPreference("varchar");
		//dataTypeMappingHelper.getSuggestTarget("varchar");
	}

	@Test
	public void testMapItem() {
		AbstractDataTypeMappingHelper dataTypeMappingHelper = MigrationTransFactory.getTransformHelper(
				DatabaseType.MYSQL, DatabaseType.CUBRID).getDataTypeMappingHelper();
		for (MapItem mi : dataTypeMappingHelper.getPreferenceConfigMap().values()) {
			MapItem mc = mi.clone();
			Assert.assertEquals(mc.getSource().getDatatype(),
					mi.getSource().getDatatype());
		}

		MapItem mi = new MapItem(dataTypeMappingHelper);
		Assert.assertNull(mi.getFirstTarget());
		List<MapObject> targetItems = new ArrayList<MapObject>();
		targetItems.add(new MapObject());
		mi.setAvailableTargetList(targetItems);
		Assert.assertNotNull(mi.getFirstTarget());

		mi.setSource(new MapObject());
		mi.setTarget(new MapObject());
		Assert.assertNotNull(mi.clone());
	}
}
