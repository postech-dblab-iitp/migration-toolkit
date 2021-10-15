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
package com.cubrid.cubridmigration.cubrid.meta;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.TestUtil2;
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;

/**
 * 
 * CUBRIDDBObjectBuilderTest
 * 
 * @author Jessie Huang
 * @version 1.0 - 2009-11-4
 */
public class CUBRIDSchemaFetcherTest {

	//	@Test
	//	public void testGetAvailableSqlTypes() throws Exception {
	//		Connection conn = TestUtil2.getCUBRID841Conn();
	//		DatabaseType.CUBRID.getMetaDataBuilder().getSupportedSqlTypes(conn);
	//	}

	@Test
	public void testFK() throws Exception {
		Connection conn = TestUtil2.getCUBRIDConn();
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = null;
		try {
			rs = meta.getExportedKeys(null, "migtestforhudson".toUpperCase(),
					"test2");

			while (rs.next()) {
				String pktable_name = rs.getString("pktable_name".toUpperCase());
				String pkcolumn_name = rs.getString("pkcolumn_name".toUpperCase());
				String fk_name = rs.getString("fk_name".toUpperCase());
				String pk_name = rs.getString("pk_name".toUpperCase());

				String fkTableName = rs.getString("FKTABLE_NAME");
				String fkColumnName = rs.getString("FKCOLUMN_NAME");
				int fkSequence = rs.getInt("KEY_SEQ");

				System.out.println("table name=" + pktable_name + "("
						+ pkcolumn_name + ")-->" + fkTableName + "("
						+ fkColumnName + ")");
				System.out.println("pk_name=" + pk_name);
				System.out.println("fk_name=" + fk_name);

				System.out.println("sequence=" + fkSequence);
			}
		} finally {
			Closer.close(rs);
			Closer.close(conn);
		}
	}

	/**
	 * testBuildCatalog
	 * 
	 * @throws MigrationException e
	 * @throws SQLException e
	 */
	@Test
	public final void testBuildCatalog() throws Exception {
		Connection conn = null;
		try {
			conn = TestUtil2.getCUBRIDConn();
			Catalog catalog = new CUBRIDSchemaFetcher().buildCatalog(conn,
					TestUtil2.getCUBRIDConnParam(), null);
			String json = TestUtil2.getCatalogJson(catalog);
			//System.out.println(json);
			String sb = TestUtil2.readStrFromFile("/com/cubrid/cubridmigration/cubrid/meta/schema.json");
			Assert.assertEquals(
					sb.replaceAll("\r\n", " ").replaceAll("\r", " ").replaceAll(
							"\n", " "),
					json.replaceAll("\r\n", " ").replaceAll("\r", " ").replaceAll(
							"\n", " "));
		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * testGetAllProcedures
	 * 
	 * @throws MigrationException e
	 * @throws SQLException e
	 */
	@Test
	public final void testGetAllProcedures() throws SQLException {
		Connection conn = TestUtil2.getCUBRIDConn();
		try {
			Catalog catalog = new CUBRIDSchemaFetcher().buildCatalog(conn,
					TestUtil2.getCUBRIDConnParam(), null);
			List<Procedure> procList = catalog.getSchemas().get(0).getProcedures();
			for (Procedure proc : procList) {
				System.out.println(proc.getDDL());
			}
			Assert.assertTrue(procList.size() >= 0);

			List<Function> funcList = catalog.getSchemas().get(0).getFunctions();

			for (Function func : funcList) {
				System.out.println(func.getDDL());
			}
			Assert.assertTrue(funcList.size() >= 0);
		} finally {
			Closer.close(conn);
		}

	}

	/**
	 * testGetAllTriggers
	 * 
	 * @throws MigrationException e
	 * @throws SQLException e
	 */
	@Test
	public final void testGetAllTriggers() throws SQLException {
		Connection conn = TestUtil2.getCUBRIDConn();
		try {
			Catalog catalog = new CUBRIDSchemaFetcher().buildCatalog(conn,
					TestUtil2.getCUBRIDConnParam(), null);
			List<Trigger> list = catalog.getSchemas().get(0).getTriggers();
			for (Trigger trig : list) {
				System.out.println(trig.getDDL());
			}

			Assert.assertTrue(list.size() >= 0);
		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * testBuildTableColumns
	 * 
	 * @throws MigrationException e
	 * @throws SQLException e
	 */
	@Test
	public final void testBuildTableColumns() throws SQLException {
		Connection conn = TestUtil2.getCUBRIDConn();
		try {
			Catalog catalog = new CUBRIDSchemaFetcher().buildCatalog(conn,
					TestUtil2.getCUBRIDConnParam(), null);
			List<Table> list = catalog.getSchemas().get(0).getTables();

			Assert.assertTrue(list.size() > 0);
		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * testBuildViews
	 * 
	 * @throws MigrationException e
	 * @throws SQLException e
	 * 
	 */
	@Test
	public final void testBuildViews() throws SQLException {
		Connection conn = TestUtil2.getCUBRIDConn();
		try {
			Catalog catalog = new Catalog();
			catalog.setName("migtestforhudson");
			Schema schema = new Schema(catalog);
			schema.setName("migtestforhudson");
			new CUBRIDSchemaFetcher().buildViews(conn, catalog, schema, null);
			List<View> list = schema.getViews();

			Assert.assertTrue(list.size() >= 0);
		} finally {
			Closer.close(conn);
		}
	}

	@Test
	public final void testBuildTables() throws SQLException {
		Connection conn = TestUtil2.getCUBRIDConn();
		Catalog catalog = new Catalog();
		catalog.setName("migtestforhudson");
		Schema schema = new Schema(catalog);
		schema.setName("migtestforhudson");
		new CUBRIDSchemaFetcher().buildTables(conn, catalog, schema, null);
		Assert.assertTrue(catalog != null);
	}

	@Test
	public void testBuildSQLTables() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getCubridConfig();
		final Table sqlTable = DatabaseType.CUBRID.getMetaDataBuilder().buildSQLTableSchema(
				config.getSourceConParams(), "select s_name from code");
		Assert.assertNotNull(sqlTable);
		Assert.assertEquals(1, sqlTable.getColumns().size());
	}
}
