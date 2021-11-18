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

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.TestUtil2;
import com.cubrid.cubridmigration.core.connection.JDBCUtil;
import com.cubrid.cubridmigration.core.engine.JDBCConManager;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;

/**
 * JDBCConnectionManagerTester
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-10 created by Kevin Cao
 */
public class JDBCConnectionManagerTest {

	static {
		JDBCUtil.initialJdbcByPath(TestUtil2.getJdbcPath());
	}

	@Test
	public void testCubrid() throws Exception {
		MigrationConfiguration cubridConfig = TemplateParserTest.getCubridConfig();
		JDBCConManager cm = new JDBCConManager(cubridConfig);

		testSource(cm);
		testTarget(cm);

		cm.dispose();
	}

	@Test
	public void testMySQL() throws Exception {
		MigrationConfiguration mysqlConfig = TemplateParserTest.getMySQLConfig();
		JDBCConManager cm = new JDBCConManager(mysqlConfig);

		testSource(cm);
		testTarget(cm);

		cm.dispose();
	}

	@Test
	public void testOracle() throws Exception {
		MigrationConfiguration oracleConfig = TemplateParserTest.getOracleConfig();
		JDBCConManager cm = new JDBCConManager(oracleConfig);

		testSource(cm);
		testTarget(cm);

		cm.dispose();
	}

	private void testSource(JDBCConManager cm) throws SQLException {
		Connection cons = cm.getSourceConnection();
		Assert.assertNotNull(cons);
		Assert.assertTrue(!cons.isClosed());
		cm.closeSrc(cons);
		cm.closeSrc(null);

	}

	private void testTarget(JDBCConManager cm) throws SQLException {
		Connection cont = cm.getTargetConnection();
		Assert.assertNotNull(cont);
		Assert.assertTrue(!cont.isClosed());
		cm.closeTar(cont);
		cm.closeTar(null);
	}
}
