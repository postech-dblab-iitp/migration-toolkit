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
package com.cubrid.cubridmigration.core.engine.config;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;

public class MigrationConfigurationTest {

	@Test
	public void testMySQLConfig() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getMySQLConfig();
		test(config);
	}

	@Test
	public void testCubridConfig() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getCubridConfig();
		test(config);
	}

	@Test
	public void testOracleConfig() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getOracleConfig();
		config.buildRequiredSourceSchema();
		config.buildSourceSchemaForDataMigration();
		config.getSourceTableConfigByTarget("");
		final String path = "/home/cmt/output/";
		config.changeTargetFilePath(path);
		Assert.assertTrue(config.getTargetDataFileName().startsWith(path));
		Assert.assertTrue(config.getTargetSchemaFileName().startsWith(path));
		Assert.assertTrue(config.getTargetIndexFileName().startsWith(path));

		final SourceEntryTableConfig setc = config.getExpEntryTableCfg(null, "ATHLETE");
		final SourceColumnConfig scc = setc.getColumnConfigList().get(0);
		final String newTarget = scc.getTarget() + "1";
		config.changeTarget(scc, newTarget);
		Assert.assertEquals(scc.getTarget(), newTarget);
	}

	@Test
	public void testCSV2CUBRIDConfig() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_LF_CSV2CUBRIDConfig();
		final List<SourceCSVConfig> csvConfigs = config.getCSVConfigs();
		String acsv = "athlete.csv";
		for (SourceCSVConfig scc : csvConfigs) {
			if (scc.getName().indexOf("athlete.csv") >= 0) {
				acsv = scc.getName();
				break;
			}
		}
		SourceCSVConfig scc = config.getCSVConfigByFile(acsv);
		Assert.assertNotNull(scc);
		config.removeCSVFile(acsv);
		scc = config.getCSVConfigByFile(acsv);
		Assert.assertNull(scc);

		Schema ts = new Schema();
		config.addCSVFile(acsv, ts);
		scc = config.getCSVConfigByFile(acsv);
		config.parsingCSVFile(scc);

		config.changeCSVTarget(scc, "athlete1", ts, false);
		Assert.assertEquals(scc.getTarget(), "athlete1");

		SourceCSVColumnConfig sccc = scc.getColumnConfigs().get(0);
		Column tcol = config.getTargetTableSchema(scc.getTarget()).getColumnByName(sccc.getTarget());
		final String targetName = tcol.getName() + "1";
		config.changeCSVTarget(sccc, targetName, tcol);
		Assert.assertEquals(sccc.getTarget(), targetName);
		config.reparseCSVFiles(ts);
	}

	private void test(MigrationConfiguration config) throws Exception {
		try {
			config.addExpSerialCfg("", "test_sequence2", "test_sequence2");
		} catch (RuntimeException ex) {

		}
		try {
			SourceEntryTableConfig stc2 = new SourceEntryTableConfig();
			stc2.setName("yyy");
			stc2.setTarget("yyy");
			config.addExpEntryTableCfg(stc2);
		} catch (RuntimeException ex) {

		}
		try {
			config.addExpSerialCfg("", "test_sequence1", "test_sequence1");
		} catch (RuntimeException ex) {

		}
		try {
			config.addExpTriggerCfg("test_trigger1");
		} catch (RuntimeException ex) {

		}
		try {
			config.addExpFunctionCfg("test_function1");
		} catch (RuntimeException ex) {

		}
		try {
			config.addExpFunctionCfg("xxx");
		} catch (RuntimeException ex) {

		}
		try {
			config.addExpProcedureCfg("test_procedure1");
		} catch (RuntimeException ex) {

		}
		try {
			config.addExpViewCfg("", "test_view", "test_view");
		} catch (RuntimeException ex) {

		}
		try {
			View view = new View();
			view.setName("test_view");
			config.addTargetViewSchema(view);
		} catch (RuntimeException ex) {

		}
		config.setCommitCount(1);
		config.setDeleteTempFile(true);
		config.setExportNoSupportObjects(true);
		config.setExportThreadCount(1);
		config.setImportThreadCount(1);
		config.setFileRepositroyPath("");
		//config.setImportThreadCount(1);
		config.setSourceType(DatabaseType.MYSQL.getName());
		config.setSourceFileEncoding("utf-8");
		config.setSourceFileName("test.xml");
		config.setSourceFileTimeZone("GMT+8");
		config.setSourceFileVersion("5.0");
		config.setTargetDBVersion("8.4.1");
		config.setTargetSchemaFileName("schema.txt");
		config.setTargetIndexFileName("index.txt");
		config.setTargetDataFileName("data.txt");
		config.setSourceFileEncoding(null);
		config.setTargetFileTimeZone("Default");

		config.setAll(false);
		Assert.assertFalse(config.getExpEntryTableCfg(null, "game").isCreateNewTable());
		config.setAll(true);
		Assert.assertTrue(config.hasObjects2Export());

		SourceSQLTableConfig sstc = new SourceSQLTableConfig();
		sstc.setName("sql_1");
		sstc.setSql("select * from code cc");
		sstc.setTarget("sql_1");
		config.addExpSQLTableCfgWithST(sstc);
		config.replaceSQL(sstc, "sql_1", "select * from code cd");
		config.changeTarget(sstc, "code");
		config.changeTarget(sstc, "sql_2");
		config.changeTarget(sstc, "sql_1");
		config.validateExpSQLConfig("select * from code");
		try {
			config.validateExpSQLConfig("  ");
			Assert.assertTrue(false);
		} catch (RuntimeException ex) {
			Assert.assertTrue(true);
		}
		try {
			config.validateExpSQLConfig(null);
			Assert.assertTrue(false);
		} catch (RuntimeException ex) {
			Assert.assertTrue(true);
		}
		config.rmSQLConfig(sstc);

		testGettingMethods(config);
		testGetRecordCountPerThread(config);

		Assert.assertFalse(config.isTargetSerialNameInUse("serial"));
		config.cleanNoUsedConfigForStart();
	}

	private void testGetRecordCountPerThread(MigrationConfiguration config) {
		config.setCommitCount(1000);
		Assert.assertEquals(1000, config.getCommitCount());

		config.setCommitCount(1000);
		Assert.assertEquals(1000, config.getCommitCount());

		config.setCommitCount(1000);
		Assert.assertEquals(1000, config.getCommitCount());

		config.setImplicitEstimate(false);
		Assert.assertFalse(config.isImplicitEstimate());

		config.setImplicitEstimate(true);
		Assert.assertTrue(config.isImplicitEstimate());
	}

	private void testGettingMethods(MigrationConfiguration config) {
		String schema = config.getSrcCatalog().getSchemas().get(0).getName();
		config.getExpObjCount();
		config.getCharsetFactor();
		config.getTargetDatabaseTimeZone();
		config.getSourceDatabaseTimeZone();
		config.getTargetDataFileName();
		config.getTargetSchemaFileName();
		config.getTargetIndexFileName();
		config.getSrcSerialSchema();
		config.getCommitCount();
		config.getSrcColumnSchema(schema, "code", "s_name");
		config.getSrcColumnSchema(schema, "code", "name");
		config.getSrcColumnSchema(schema, "code1", "s_name");
		config.getExpEntryTableCfg(null, "code");
		config.getExpEntryTableCfg();
		config.getSrcFKSchema(schema, "game", "fk_game_event_code");
		config.getSrcFKSchema(schema, "game", "");
		config.getSrcFKSchema(schema, "name", "");
		config.getSrcFKSchema(schema, "", "");
		config.getSrcFkSchemaByTable(schema, "game");
		config.getExpFunctionCfg("test_function");
		config.getExpFunctionCfg("test_function1");
		config.getExpFunctionCfg();
		config.getSrcIdxSchema(schema, "game", "idx_game_game_date");
		config.getExpProcedureCfg("test_procedure");
		config.getExpProcedureCfg("test_procedure1");
		config.getExpProcedureCfg();
		config.getExpSerialCfg(null, "test_sequence");
		config.getExpSerialCfg();

		SourceSQLTableConfig sstc = new SourceSQLTableConfig();
		sstc.setName("sql_1");
		sstc.setSql("select * from code cc");
		sstc.setTarget("sql_1");
		config.addExpSQLTableCfgWithST(sstc);
		Assert.assertNotNull(config.getSrcSQLSchemaBySql("select * from code cc"));
		Assert.assertNull(config.getSrcSQLSchemaBySql("  "));
		Assert.assertNull(config.getSrcSQLSchemaBySql(null));

		config.getExpSQLCfgBySql("select * from game");
		config.getExpSQLCfgByName("game");
		config.getExpSQLCfg();
		config.getExportThreadCount();
		config.getExpTriggerCfg("test_trigger");
		config.getExpTriggerCfg();
		config.getExpViewCfg(null, "game_view");
		config.getExpViewCfg(null, "game_view1");
		config.getExpViewCfg();
		config.getFileRepositroyPath();
		config.getImportThreadCount();
		config.getSrcSerialSchema(schema, "test_sequence");
		config.getExpColumnCfg(null, "game", "event_code");
		config.getExpColumnCfg(null, "game", "xxx");
		config.getExpColumnCfg(null, "xxx", "xxx");
		config.getSourceConParams();
		config.sourceIsOnline();
		config.getSourceDBType();
		config.getSourceFileEncoding();
		config.getSourceFileName();
		config.getSourceFileTimeZone();
		config.getSourceFileVersion();
		config.getExpFKCfg(null, "game", "fk_game_event_code");
		config.getExpIdxCfg(null, "game", "idx_game_game_date");
		config.getSrcTableSchema(schema, "game");
		config.getSrcViewSchema(schema, "game_view");
		config.getTargetCharSet();
		config.getTargetConParams();
		config.getDetaRawOffset();
		config.getCharsetFactor();
		config.targetIsOnline();
		config.getTargetDBVersion();
		config.getTargetSerialSchema("test_sequence");
		config.getTargetSerialSchema("");
		config.getTargetSerialSchema();
		config.getTargetTableSchema();
		config.getTargetViewSchema("game_view");
		config.getTargetViewSchema();
		config.getTargetFileTimeZone();
		config.hasFKExports();
		config.hasIndexExports();
		config.hasObjects2Export();

		config.getExpSchemaNames();

		config.isDeleteTempFile();
		config.isExportNoSupportObjects();
		config.targetIsFile();

		Assert.assertTrue(config.isExportColumn(null, "game", "event_code"));
		testGetTargetColumnSchema(config);

		Assert.assertTrue(config.isTargetNameInUse("game"));
		Assert.assertFalse(config.isTargetNameInUse(""));
		Assert.assertFalse(config.isTargetNameInUse(" "));
		Assert.assertFalse(config.isTargetNameInUse(null));
		Assert.assertFalse(config.isTargetNameInUse("game1"));

		config.buildSourceSchema();
		config.setDestTypeName("csv");
		config.getSrcSQLSchema2Exp();
		config.clearAllSQLTables();
		config.getOfflineSrcCatalog();

		config.removeExpSchema("schema");
		config.setReportLevel(0);
		config.isSourceOfflineMode();
		config.isTargetOfflineMode();
		config.hasObjects2Export();

		MigrationConfiguration.getDataFileFormatExts();

		config.setExp2FileOuput("", PathUtils.getDefaultBaseTempDir(), "utf-8");
		config.getTargetDataFileFormatLabel();
		//TODO:
		//config.repareN21MigrationSetting();
		config.setOfflineSrcCatalog(null);
		config.addExpSQLTableSchema(null);

		SourceColumnConfig scc = config.getExpEntryTableCfg().get(0).getColumnConfigList().get(0);
		config.changeTarget(scc, scc.getTarget());
	}

	private void testGetTargetColumnSchema(MigrationConfiguration config) {
		//Test getTargetColumnSchema method
		Assert.assertNotNull(config.getTargetColumnSchema("game", "event_code"));
		Assert.assertNull(config.getTargetColumnSchema("game1", "event_code"));
		Assert.assertNull(config.getTargetColumnSchema("game", "event_code1"));
		Assert.assertNull(config.getTargetColumnSchema(null, "event_code"));
		Assert.assertNull(config.getTargetColumnSchema("game", null));
		Assert.assertNull(config.getTargetColumnSchema("", "event_code"));
		Assert.assertNull(config.getTargetColumnSchema("game", ""));
	}

}
