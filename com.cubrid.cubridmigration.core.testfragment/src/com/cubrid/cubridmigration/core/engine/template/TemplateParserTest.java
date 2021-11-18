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
package com.cubrid.cubridmigration.core.engine.template;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.TestUtil2;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.engine.MigrationProcessManagerTest;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.cubrid.meta.CUBRIDSchemaFetcher;
import com.cubrid.cubridmigration.mssql.meta.MSSQLSchemaFetcher;
import com.cubrid.cubridmigration.mysql.meta.MySQLSchemaFetcher;
import com.cubrid.cubridmigration.oracle.meta.OracleSchemaFetcher;

/**
 * 
 * TemplateParserTest Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-10-9 created by Kevin Cao
 */
public class TemplateParserTest {

	//private static final String MIGTESTFORHUDSON = "migtestforhudson";

	public static final String TEST_CASE_ROOT_PATH = getRootPath();

	static {
		TestUtil2.getJdbcPath();
	}

	public static MigrationConfiguration get_LF_CSV2CUBRIDConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_csv_2_cubrid.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		final List<SourceCSVConfig> csvConfigs = config.getCSVConfigs();
		for (SourceCSVConfig csvc : csvConfigs) {
			csvc.setName(getSourceFileFullName("csv/" + csvc.getName()));
		}
		return config;
	}

	public static MigrationConfiguration get_LF_SQL2CUBRIDConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_sql_2_cubrid.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		List<String> sqlFiles = new ArrayList<String>();
		List<String> sqlOldFiles = config.getSqlFiles();
		for (String sql : sqlOldFiles) {
			sqlFiles.add(getSourceFileFullName("sql/" + sql));
		}
		config.setSqlFiles(sqlFiles);
		return config;
	}

	public static MigrationConfiguration get_OF_MySQLXML2CUBRIDConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_mysqlxml_2_cubrid.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		//Source schema information
		MigrationConfiguration config2 = get_OL_MySQL2CUBRIDConfig();
		config.setSrcCatalog(config2.getSrcCatalog(), false);
		//Set source file information
		URL url = MigrationProcessManagerTest.class.getClassLoader().getResource(
				"com/cubrid/cubridmigration/mysqldumptest.xml");
		config.setSourceFileEncoding("utf-8");
		config.setSourceFileName(url.getFile());
		config.setSourceFileTimeZone("Default");
		config.setSourceType(MigrationConfiguration.XML);
		return config;
	}

	public static MigrationConfiguration get_OL_CUBRID2CSVConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_cubrid_2_csv.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		String fileRepositroyPath = TEST_CASE_ROOT_PATH + "output"
				+ File.separator;
		config.setFileRepositroyPath(fileRepositroyPath);
		config.setTargetDataFileName(fileRepositroyPath + "data.csv");
		config.setTargetSchemaFileName(fileRepositroyPath + "schema");
		config.setTargetIndexFileName(fileRepositroyPath + "index");
		config.setTargetFilePrefix("");

		CUBRIDSchemaFetcher builder = new CUBRIDSchemaFetcher();
		Catalog cl = builder.buildCatalog(
				config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static MigrationConfiguration get_OL_CUBRID2CUBRIDConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_cubrid_2_cubrid.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);

		CUBRIDSchemaFetcher builder = new CUBRIDSchemaFetcher();
		Catalog cl = builder.buildCatalog(
				config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static MigrationConfiguration get_OL_CUBRID2DumpConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_cubrid_2_dump.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);

		CUBRIDSchemaFetcher builder = new CUBRIDSchemaFetcher();
		Catalog cl = builder.buildCatalog(
				config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static MigrationConfiguration get_OL_CUBRID2SQLConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_cubrid_2_sql.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		String fileRepositroyPath = TEST_CASE_ROOT_PATH + "output"
				+ File.separator;
		config.setFileRepositroyPath(fileRepositroyPath);
		config.setTargetDataFileName(fileRepositroyPath + "data.sql");
		config.setTargetSchemaFileName(fileRepositroyPath + "schema");
		config.setTargetIndexFileName(fileRepositroyPath + "index");
		config.setTargetFilePrefix("");

		CUBRIDSchemaFetcher builder = new CUBRIDSchemaFetcher();
		Catalog cl = builder.buildCatalog(
				config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static MigrationConfiguration get_OL_CUBRID2XLSConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_cubrid_2_xls.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		String fileRepositroyPath = TEST_CASE_ROOT_PATH + "output"
				+ File.separator;
		config.setFileRepositroyPath(fileRepositroyPath);
		config.setTargetDataFileName(fileRepositroyPath + "data.xls");
		config.setTargetSchemaFileName(fileRepositroyPath + "schema");
		config.setTargetIndexFileName(fileRepositroyPath + "index");
		config.setTargetFilePrefix("");

		CUBRIDSchemaFetcher builder = new CUBRIDSchemaFetcher();
		Catalog cl = builder.buildCatalog(
				config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static MigrationConfiguration get_OL_MSSQL2CUBRIDConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_mssql_2_cubrid.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);

		MSSQLSchemaFetcher builder = new MSSQLSchemaFetcher();
		final ConnParameters sourceConParams = config.getSourceConParams();
		Catalog cl = builder.buildCatalog(sourceConParams.createConnection(),
				sourceConParams, null);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static MigrationConfiguration get_OL_MySQL2CUBRIDConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_mysql_2_cubrid.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);

		Catalog cl = getMySQLCatalog(config);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static MigrationConfiguration get_OL_Oracle2CUBRIDConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_oracle_2_cubrid.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);

		OracleSchemaFetcher builder = new OracleSchemaFetcher();
		Catalog cl = builder.buildCatalog(
				config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static MigrationConfiguration getCubridConfig() throws Exception {

		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_cubrid2.xml");
		if (is == null) {
			throw new Exception("Null template of cubrid");
		}
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		config.setSourceType(MigrationConfiguration.SOURCE_TYPE_CUBRID);
		config.setDestType(MigrationConfiguration.DEST_ONLINE);
		config.setImplicitEstimate(false);

		CUBRIDSchemaFetcher builder = new CUBRIDSchemaFetcher();
		Catalog cl = builder.buildCatalog(
				config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static Catalog getMySQLCatalog(MigrationConfiguration config) throws SQLException {
		MySQLSchemaFetcher builder = new MySQLSchemaFetcher();
		Catalog cl = builder.buildCatalog(
				config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		return cl;
	}

	public static MigrationConfiguration getMySQLConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_mysql.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		config.setSourceType(MigrationConfiguration.SOURCE_TYPE_MYSQL);
		config.setDestType(MigrationConfiguration.DEST_ONLINE);

		Catalog cl = getMySQLCatalog(config);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	public static MigrationConfiguration getOracleConfig() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_oracle.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		config.setSourceType(MigrationConfiguration.SOURCE_TYPE_ORACLE);
		config.setDestType(MigrationConfiguration.DEST_ONLINE);

		OracleSchemaFetcher builder = new OracleSchemaFetcher();
		Catalog cl = builder.buildCatalog(
				config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		config.setSrcCatalog(cl, false);
		config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
		return config;
	}

	private static String getRootPath() {
		URL url = ClassLoader.getSystemResource("./");
		String driverPath = PathUtils.getURLFilePath(url);
		driverPath += ".." + File.separator + ".." + File.separator
				+ "com.cubrid.cubridmigration.core.testfragment";
		try {
			return new File(driverPath).getCanonicalPath() + File.separator;
		} catch (IOException e) {
			return null;
		}
	}

	//	public static MigrationConfiguration getSQLServerConfig() throws Exception {
	//		InputStream is = MigrationTemplateParser.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_sqlserver.xml");
	//		MigrationConfiguration config = MigrationTemplateParser.parse(is);
	//		//updateDriverPath(config.getSourceConParams());
	//		//updateDriverPath(config.getTargetConParams());
	//
	//		SQLServerDBObjectBuilder builder = new SQLServerDBObjectBuilder();
	//		Catalog cl = builder.buildCatalog(
	//				config.getSourceConParams().createConnection(),
	//				MIGTESTFORHUDSON, null);
	//		Schema schema = cl.getSchemas().get(0);
	//		config.setSrcCatalog(schema);
	//		return config;
	//	}

	private static String getSourceFileFullName(String fileName) {
		URL url = ClassLoader.getSystemResource("com/cubrid/cubridmigration/sourcefiles/"
				+ fileName);
		return PathUtils.getURLFilePath(url);
	}

	@Test
	public void testCSV2CUBRIDConfig() throws Exception {
		MigrationConfiguration config = get_LF_CSV2CUBRIDConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_CSV);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_ONLINE);
	}

	@Test
	public void testCUBRID2CSVConfig() throws Exception {
		MigrationConfiguration config = get_OL_CUBRID2CSVConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_CUBRID);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_CSV);
	}

	@Test
	public void testCUBRID2CUBRIDConfig() throws Exception {
		MigrationConfiguration config = get_OL_CUBRID2CUBRIDConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_CUBRID);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_ONLINE);
	}

	@Test
	public void testCUBRID2DumpConfig() throws Exception {
		MigrationConfiguration config = get_OL_CUBRID2DumpConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_CUBRID);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_DB_UNLOAD);
	}

	@Test
	public void testCUBRID2SQLConfig() throws Exception {
		MigrationConfiguration config = get_OL_CUBRID2SQLConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_CUBRID);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_SQL);
	}

	@Test
	public void testCUBRID2XLSConfig() throws Exception {
		MigrationConfiguration config = get_OL_CUBRID2XLSConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_CUBRID);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_XLS);
	}

	@Test
	public void testCubridTemplate() throws Exception {
		MigrationConfiguration config = getCubridConfig();
		verifyConfig(config);
		Assert.assertEquals(config.getExpSerialCfg().size(), 1);
		Assert.assertEquals(config.getTargetSerialSchema().size(), 1);
	}

	//cmt_template_mssql_2_cubrid.xml
	@Test
	public void testMSSQL2CUBRIDTemplate() throws Exception {
		MigrationConfiguration config = get_OL_MSSQL2CUBRIDConfig();
		verifyConfig(config);
	}

	@Test
	public void testMySQL2CUBRIDConfig() throws Exception {
		MigrationConfiguration config = get_OL_MySQL2CUBRIDConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_MYSQL);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_ONLINE);
	}

	@Test
	public void testMysqlTemplate() throws Exception {
		MigrationConfiguration config = getMySQLConfig();

		verifyConfig(config);
		Assert.assertEquals(config.getExpSerialCfg().size(), 0);
		Assert.assertEquals(config.getTargetSerialSchema().size(), 0);
	}

	@Test
	public void testMySQLXML2CUBRIDConfig() throws Exception {
		MigrationConfiguration config = get_OF_MySQLXML2CUBRIDConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_XML_1);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_ONLINE);
	}

	@Test
	public void testOracle2CUBRIDConfig() throws Exception {
		MigrationConfiguration config = get_OL_Oracle2CUBRIDConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_ORACLE);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_ONLINE);
	}

	@Test
	public void testOracleTemplate() throws Exception {
		MigrationConfiguration config = getOracleConfig();
		verifyConfig(config);
		Assert.assertEquals(config.getExpSerialCfg().size(), 1);
		Assert.assertEquals(config.getTargetSerialSchema().size(), 1);
	}

	@Test
	public void testParse() throws Exception {
		InputStream is = TemplateParserTest.class.getResourceAsStream("/com/cubrid/cubridmigration/scripts/cmt_template_test.xml");
		MigrationConfiguration config = MigrationTemplateParser.parse(is);
		//System.out.println(config.getCmServer().getHost());
		MigrationTemplateParser.save(config, "cmt_template_test_out.xml", false);
		config = MigrationTemplateParser.parse("cmt_template_test_out.xml");
		//System.out.println(config.getExportSQLTables().get(0).getSql());
		new File("cmt_template_test_out.xml").delete();
	}

	@Test
	public void testSQL2CUBRIDConfig() throws Exception {
		MigrationConfiguration config = get_LF_SQL2CUBRIDConfig();
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getSourceType(),
				MigrationConfiguration.SOURCE_TYPE_SQL);
		Assert.assertEquals(config.getDestType(),
				MigrationConfiguration.DEST_ONLINE);
	}

	private void verifyConfig(MigrationConfiguration config) {
		Assert.assertNotNull(config);
		Assert.assertEquals(config.getExpViewCfg().size(), 2);
		Assert.assertEquals(config.getTargetViewSchema().size(), 2);

		Assert.assertNotNull(config.getSourceConParams());
		Assert.assertNotNull(config.getTargetConParams());
	}
}
