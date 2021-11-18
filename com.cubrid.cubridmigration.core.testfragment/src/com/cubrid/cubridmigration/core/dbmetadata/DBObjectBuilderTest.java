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
package com.cubrid.cubridmigration.core.dbmetadata;

import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.TestUtil2;
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Version;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.cubrid.meta.CUBRIDSchemaFetcher;

/**
 * 
 * DBObjectBuilderTest
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-18
 */
public class DBObjectBuilderTest {

	/**
	 * test GetVersion
	 * 
	 * @throws MigrationException e
	 * @throws SQLException e
	 */
	@Test
	public final void testGetVersion() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			Version version = DatabaseType.MYSQL.getMetaDataBuilder().getVersion(
					conn);
			Assert.assertNotNull(version.getDbProductName());
			Assert.assertNotNull(version.getDbProductVersion());
			Assert.assertNotNull(version.getDriverName());
			Assert.assertNotNull(version.getDriverVersion());
			Assert.assertNotNull(version);
		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * test GetCharSet
	 * 
	 * @throws MigrationException e
	 * @throws SQLException e
	 */
	@Test
	public final void testGetCharSet() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			String charset = DatabaseType.MYSQL.getMetaDataBuilder().getCharSet(
					conn);

			Assert.assertNull(charset);
		} finally {
			Closer.close(conn);
		}
	}

	//	@Test
	//	public final void testGetMySQLSchema() throws Exception {
	//		Connection conn = TestUtil2.getMySQL5520Conn();
	//		try {
	//			final AbstractJDBCSchemaFetcher dbObjectBuilder = new MYSQLSchemaFetcher();
	//			List<String> list = dbObjectBuilder.getSchemata(conn);
	//
	//			Assert.assertNotNull(list);
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	//	@Test
	//	public final void testGetCUBRIDSchemata() throws Exception {
	//		Connection conn = TestUtil2.getCUBRID841Conn();
	//		try {
	//			List<String> list = new CUBRIDSchemaFetcher().getSchemata(conn);
	//
	//			Assert.assertNotNull(list);
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	//	@Test
	//	public final void testGetMySQLCatalog() throws Exception {
	//		Connection conn = TestUtil2.getMySQL5520Conn();
	//		try {
	//			List<String> list = new MYSQLSchemaFetcher().getCatalogs(conn);
	//
	//			Assert.assertNotNull(list);
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	//	@Test
	//	public final void testGetCUBRIDCatalog() throws Exception {
	//		Connection conn = TestUtil2.getCUBRID841Conn();
	//		try {
	//			List<String> list = new CUBRIDSchemaFetcher().getCatalogs(conn);
	//
	//			Assert.assertNotNull(list);
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	//	public static String toString(Column column) {
	//		StringBuffer bf = new StringBuffer();
	//		bf.append("Name:").append(column.getName()).append("\n");
	//		bf.append("DataType:").append(column.getDataType()).append("\n");
	//		bf.append("DefaultValue:").append(column.getDefaultValue()).append("\n");
	//		bf.append("Precision:").append(column.getPrecision()).append("\n");
	//		bf.append("Scale:").append(column.getScale()).append("\n");
	//		bf.append("Charset:").append((column).getCharset()).append("\n");
	//		bf.append("Length:").append(column.getByteLength()).append("\n\n");
	//		return bf.toString();
	//	}

	//	@Test
	//	public final void testBuildTableColumns() throws Exception {
	//		Connection conn = TestUtil2.getMySQL5520Conn();
	//		try {
	//			Catalog catalog = new Catalog();
	//			catalog.setName("migtestforhudson");
	//			Version version = new MYSQLSchemaFetcher().getVersion(conn);
	//			catalog.setVersion(version);
	//			Schema schema = new Schema(catalog);
	//			schema.setName("migtestforhudson");
	//			Table table = new Table(schema);
	//			table.setName("test_string");
	//			MYSQLSchemaFetcher dbObjectBuilder = new MYSQLSchemaFetcher();
	//			dbObjectBuilder.buildTableColumns(conn, catalog, schema, table);
	//			//			List<Column> list = table.getColumns();
	//			//			for (Column column : list) {
	//			//				System.out.println(toString(column));
	//			//			}
	//			//System.out.println("column count:" + table.getColumns().size());
	//
	//			Assert.assertTrue(table.getColumns().size() >= 0);
	//			Schema schema2 = new Schema(catalog);
	//			schema2.setName("migtestforhudson");
	//			Assert.assertEquals(schema2, schema);
	//
	//			Assert.assertNull(dbObjectBuilder.getCatalogName(null));
	//			Assert.assertNull(dbObjectBuilder.getSchemaName(null));
	//
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	@Test
	public void testgetSourcePartitionDLL() {
		Table sourceTable = new Table();
		String ss = "PARTITION BY test partition";
		sourceTable.setDDL("create table test() " + ss);
		Assert.assertEquals(ss,
				new CUBRIDSchemaFetcher().getSourcePartitionDDL(sourceTable));

		sourceTable.setDDL("create table test() ");
		Assert.assertEquals("",
				new CUBRIDSchemaFetcher().getSourcePartitionDDL(sourceTable));
	}
}
