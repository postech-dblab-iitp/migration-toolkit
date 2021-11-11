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
package com.cubrid.cubridmigration.core.engine;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.mock.MigrationReporterMock;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;

/**
 * 
 * MigrationProcessManagerTester
 * 
 * LF: local files; OL: online DB; OF: DB dump file
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-10 created by Kevin Cao
 */
public class MigrationProcessManagerTest extends
		BaseMigrationEngineTester {

	public MigrationProcessManagerTest() {
	}

	/**
	 * Clear Target DB
	 * 
	 * @param config MigrationConfiguration
	 */
	private void clearTargetDB(MigrationConfiguration config) {
		try {
			if (config.targetIsOnline()) {
				Connection con = config.getTargetConParams().createConnection();
				con.setAutoCommit(true);
				Statement stmt = con.createStatement();
				try {
					stmt.execute("DROP TABLE game;");
				} catch (Exception e) {
					//e.printStackTrace();
				}
				try {
					stmt.execute("DROP TABLE participant;");
				} catch (Exception e) {
					//e.printStackTrace();
				}
				try {
					stmt.execute("DROP TABLE athlete;");
				} catch (Exception e) {
					//e.printStackTrace();
				}
				try {
					stmt.execute("DROP TABLE test_sequence;");
				} catch (Exception e) {
					//e.printStackTrace();
				}
				stmt.close();
				con.close();
			} else if (config.targetIsFile()) {
				File root = new File(TemplateParserTest.TEST_CASE_ROOT_PATH
						+ "output");
				for (File ff : root.listFiles()) {
					if (ff.isFile()) {
						ff.delete();
					}
				}
			}
			//			else if (config.targetIsOffline()) {
			//				CSQLTask task = new CSQLTask(getCMServer(config));
			//				task.setErrorContinue(true);
			//				task.setDbUser(config.getOfflineTargetDBInfo().getUser());
			//				task.setPassword(config.getOfflineTargetDBInfo().getPassword());
			//				task.setDbName(config.getOfflineTargetDBInfo().getName());
			//				task.setCommand("DROP TABLE game;");
			//				task.execute();
			//				System.out.println(task.getErrorMsg());
			//				task.setCommand("DROP TABLE participant;commit;");
			//				task.execute();
			//				task.setCommand("DROP TABLE athlete;");
			//				task.execute();
			//				System.out.println(task.getErrorMsg());
			//				task.setCommand("drop serial test_sequence;");
			//				task.execute();
			//				System.out.println(task.getErrorMsg());
			//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//	private CMSInfo getCMServer(MigrationConfiguration config) {
	//		CMServerConfig cmServer = config.getCmServer();
	//		CMSInfo server = CMSManager.getInstance().findServer(
	//				cmServer.getHost(), cmServer.getPort(), cmServer.getUser());
	//		if (server == null) {
	//			server = CMSManager.getInstance().createServer(cmServer.getHost(),
	//					cmServer.getPort(), cmServer.getUser());
	//			CMSManager.getInstance().addCMS(server);
	//		}
	//		server.setCubridManagerUser(cmServer.getUser());
	//		server.setCubridManagerPassword("1111");
	//		server.connect();
	//		return server;
	//	}

	/**
	 * Process configuration
	 * 
	 * @param config MigrationConfiguration
	 */
	private void process(MigrationConfiguration config) throws Exception {
		config.setExportThreadCount(1);
		config.setImportThreadCount(1);
		clearTargetDB(config);
		startMigration(config);

		//clear output and report directory
		CUBRIDIOUtils.clearFileOrDir("./output");
		CUBRIDIOUtils.clearFileOrDir("./report");
	}

	/**
	 * Start migration with configuration
	 * 
	 * @param config MigrationConfiguration
	 */
	private void startMigration(MigrationConfiguration config) {
		//Start process
		MigrationReporterMock reporter = new MigrationReporterMock(config, 0);
		MigrationProcessManager mpm = MigrationProcessManager.getInstance(
				config, monitor, reporter);
		mpm.startMigration();
		try {
			mpm.startMigration();
		} catch (RuntimeException ex) {
			Assert.assertNotNull(ex);
		}
		ThreadUtils.threadSleep(2000, null);
		Assert.assertTrue(MigrationProcessManager.isRunning());
		while (MigrationProcessManager.isRunning()) {
			ThreadUtils.threadSleep(2000, null);
		}
		ThreadUtils.threadSleep(1000, null);
		Assert.assertFalse(MigrationProcessManager.isRunning());
		//Assert.assertFalse(reporter.hasError());
	}

	@Test
	public void test_LF_CSV2CUBRID() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_LF_CSV2CUBRIDConfig();
		process(config);
	}

	@Test
	public void test_LF_SQL2CUBRID() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_LF_SQL2CUBRIDConfig();
		process(config);
	}

	@Test
	public void test_OF_MySQLXML2CUBRID() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OF_MySQLXML2CUBRIDConfig();
		process(config);
	}

	@Test
	public void test_OL_CUBRID2CSV() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_CUBRID2CSVConfig();
		process(config);
	}

	@Test
	public void test_OL_CUBRID2CUBRID() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_CUBRID2CUBRIDConfig();
		process(config);
	}

	@Test
	public void test_OL_CUBRID2CUBRIDDump() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_CUBRID2DumpConfig();
		process(config);
	}

	@Test
	public void test_OL_CUBRID2SQL() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_CUBRID2SQLConfig();
		process(config);
	}

	@Test
	public void test_OL_CUBRID2XLS() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_CUBRID2XLSConfig();
		process(config);
	}

	@Test
	public void test_OL_MySQL2CUBRID() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_MySQL2CUBRIDConfig();
		process(config);
	}

	@Test
	public void test_OL_Oracle2CUBRID() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_Oracle2CUBRIDConfig();
		config.setWriteErrorRecords(true);
		process(config);
	}

	@Test
	public void test_OL_MSSQL2CUBRID() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_MSSQL2CUBRIDConfig();
		config.setWriteErrorRecords(true);
		process(config);
	}

}
