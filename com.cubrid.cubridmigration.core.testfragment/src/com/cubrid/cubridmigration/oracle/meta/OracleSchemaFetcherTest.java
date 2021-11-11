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
package com.cubrid.cubridmigration.oracle.meta;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.TestUtil2;
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.oracle.meta.OracleSchemaFetcher;

public class OracleSchemaFetcherTest {

	//	@Test
	//	public void testGetAvailableSqlTypes() throws Exception {
	//		Connection conn = TestUtil2.getOracle10gConn();
	//		DatabaseType.ORACLE.getMetaDataBuilder().getSupportedSqlTypes(conn);
	//	}

	@Test
	public void testFK() throws Exception {
		Connection conn = TestUtil2.getOracle10gConn();
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = null;
		try {
			rs = meta.getExportedKeys(null, "MIGTESTFORHUDSON", null);
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
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Closer.close(rs);
			Closer.close(conn);
		}
	}

	@Test
	public void testFK2() throws Exception {
		Connection conn = TestUtil2.getOracle10gConn();
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = null;
		try {
			rs = meta.getImportedKeys(null, "MIGTESTFORHUDSON", null);
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
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Closer.close(rs);
			Closer.close(conn);
		}
	}

	@Test
	public void testGetDatabaseInformation() throws Exception {
		Connection conn = TestUtil2.getOracle10gConn();
		DatabaseMetaData meta = conn.getMetaData();

		try {
			int majorVersion = meta.getDatabaseMajorVersion();
			int minorVersion = meta.getDatabaseMinorVersion();
			System.out.println("majorVersion=" + majorVersion);
			System.out.println("minorVersion=" + minorVersion);

			String productName = meta.getDatabaseProductName();
			String productVersion = meta.getDatabaseProductVersion();
			System.out.println("productName=" + productName);
			System.out.println("productVersion=" + productVersion);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Closer.close(conn);
		}
	}

	@Test
	public void testGetAllDatabases() throws Exception {
		Connection conn = TestUtil2.getOracle10gConn();
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = null;
		try {
			rs = meta.getSchemas();
			while (rs.next()) {
				String tableSchema = rs.getString(1); // "TABLE_SCHEM"
				System.out.println("tableSchema=" + tableSchema);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Closer.close(rs);
			Closer.close(conn);
		}
	}

	@Test
	public void testGetAllTables() throws Exception {
		Connection conn = TestUtil2.getOracle10gConn();
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = null;
		try {
			rs = meta.getTables(null, "MIGTESTFORHUDSON", null,
					new String[]{"TABLE" });
			while (rs.next()) {
				String table = rs.getString(3); // "TABLE_SCHEM"
				System.out.println("table=" + table);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Closer.close(rs);
			Closer.close(conn);
		}
	}

	@Test
	public void testBestRowIdentifier() throws Exception {
		Connection conn = TestUtil2.getOracle10gConn();
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = null;
		try {

			// The '_' character represents any single character.
			// The '%' character represents any sequence of zero
			// or more characters.
			rs = meta.getBestRowIdentifier(null, "MIGTESTFORHUDSON", "JOBS",
					DatabaseMetaData.bestRowTransaction, false);

			while (rs.next()) {

				short actualScope = rs.getShort("SCOPE");
				String columnName = rs.getString("COLUMN_NAME");
				int dataType = rs.getInt("DATA_TYPE");
				String typeName = rs.getString("TYPE_NAME");
				int columnSize = rs.getInt("COLUMN_SIZE");
				short decimalDigits = rs.getShort("DECIMAL_DIGITS");
				short pseudoColumn = rs.getShort("PSEUDO_COLUMN");

				System.out.println("actualScope=" + actualScope);
				System.out.println("columnName=" + columnName);
				System.out.println("dataType=" + dataType);
				System.out.println("typeName=" + typeName);
				System.out.println("columnSize=" + columnSize);
				System.out.println("decimalDigits=" + decimalDigits);
				System.out.println("pseudoColumn=" + pseudoColumn);
				System.out.println();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Closer.close(rs);
			Closer.close(conn);
		}
	}

	@Test
	public void testBuildCatalog() throws Exception {
		try {
			Connection conn = TestUtil2.getOracle10gConn();

			Catalog cat = new OracleSchemaFetcher().buildCatalog(conn,
					TestUtil2.getOracle10gConnParam(), null);
			String json = TestUtil2.getCatalogJson(cat);
			//System.out.println(json);
			String sb = TestUtil2.readStrFromFile("/com/cubrid/cubridmigration/oracle/meta/schema.json");
			Assert.assertEquals(
					sb.replaceAll("\r\n", " ").replaceAll("\r", " ").replaceAll(
							"\n", " "),
					json.replaceAll("\r\n", " ").replaceAll("\r", " ").replaceAll(
							"\n", " "));
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@Test
	public void testGetObjectDDL() throws Exception {
		Connection conn = TestUtil2.getOracle10gConn();

		String objectName = "CODE";
		String schemaName = "MIGTESTFORHUDSON";
		String objectType = "TABLE";
		String ddl = new OracleSchemaFetcher().getObjectDDL(conn, schemaName,
				objectName, objectType);

		System.out.println(ddl);
	}

	@Test
	public void testBuildTableColumns_CHAR() throws Exception {
		Connection conn = TestUtil2.getOracle10gConn();
		OracleSchemaFetcher builder = new OracleSchemaFetcher();
		final Catalog catalog = builder.buildCatalog(conn,
				TestUtil2.getOracle10gConnParam(), null);

		try {
			Schema schema = new Schema();
			schema.setName("MIGTESTFORHUDSON");
			Table table = new Table();
			table.setName("TEST_STRING");

			builder.buildTableColumns(conn, catalog, schema, table);

			for (Column col1 : table.getColumns()) {
				System.out.println(getSQLColumn(col1));
			}
			Column col1 = table.getColumnByName("ID");
			Assert.assertEquals("VARCHAR2", col1.getDataType());
			Assert.assertEquals(new Integer(12), col1.getJdbcIDOfDataType());
			Assert.assertEquals(128, col1.getByteLength());
			Assert.assertEquals(false, col1.isNullable());
			Assert.assertEquals(new Integer(128), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals("B", col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F1");
			Assert.assertEquals("CHAR", col1.getDataType());
			Assert.assertEquals(new Integer(1), col1.getJdbcIDOfDataType());
			Assert.assertEquals(1, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(1), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals("B", col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F2");
			Assert.assertEquals("CHAR", col1.getDataType());
			Assert.assertEquals(new Integer(1), col1.getJdbcIDOfDataType());
			Assert.assertEquals(2, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(2), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals("B", col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F3");
			Assert.assertEquals("CHAR", col1.getDataType());
			Assert.assertEquals(new Integer(1), col1.getJdbcIDOfDataType());
			Assert.assertEquals(255, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(255), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals("B", col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F4");
			Assert.assertEquals("VARCHAR2", col1.getDataType());
			Assert.assertEquals(new Integer(12), col1.getJdbcIDOfDataType());
			Assert.assertEquals(1, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(1), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals("B", col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F5");
			Assert.assertEquals("VARCHAR2", col1.getDataType());
			Assert.assertEquals(new Integer(12), col1.getJdbcIDOfDataType());
			Assert.assertEquals(2, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(2), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals("B", col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F6");
			Assert.assertEquals("VARCHAR2", col1.getDataType());
			Assert.assertEquals(new Integer(12), col1.getJdbcIDOfDataType());
			Assert.assertEquals(4000, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(4000), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals("B", col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F7");
			Assert.assertEquals("CLOB", col1.getDataType());
			Assert.assertEquals(new Integer(2005), col1.getJdbcIDOfDataType());
			Assert.assertEquals(4000, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(0), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F8");
			Assert.assertEquals("NCLOB", col1.getDataType());
			Assert.assertEquals(new Integer(2005), col1.getJdbcIDOfDataType());
			Assert.assertEquals(4000, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(0), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F9");
			Assert.assertEquals("LONG", col1.getDataType());
			Assert.assertEquals(new Integer(2005), col1.getJdbcIDOfDataType());
			Assert.assertEquals(0, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(0), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

		} finally {
			Closer.close(conn);
		}

	}

	//	@Test
	//	public void testGetSQLTable_CHAR() throws Exception {
	//		Connection conn = TestUtil2.getOracleConn();
	//		Statement stmt = null;
	//		ResultSet rs = null;
	//
	//		try {
	//			String sql = "SELECT * FROM \"MIGTESTFORHUDSON\".\"TEST_STRING\"";
	//			stmt = conn.createStatement();
	//			rs = stmt.executeQuery(sql);
	//			SourceTable sourceTable = new OracleDBObjectBuilder().getSQLTable(
	//					sql, rs.getMetaData());
	//			Table table = sourceTable.getTable();
	//
	//			for (Column col1 : table.getColumns()) {
	//				System.out.println(getSQLColumn(col1));
	//			}
	//
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//			Closer.close(conn);
	//		}
	//
	//	}

	@Test
	public void testBuildTableColumns_NUMBER() throws Exception {
		Connection conn = TestUtil2.getOracle10gConn();
		OracleSchemaFetcher builder = new OracleSchemaFetcher();
		final Catalog catalog = builder.buildCatalog(conn,
				TestUtil2.getOracle10gConnParam(), null);

		try {
			Schema schema = new Schema();
			schema.setName("MIGTESTFORHUDSON");
			Table table = new Table();
			table.setName("TEST_NUMBER");
			builder.buildTableColumns(conn, catalog, schema, table);

			Column col1 = table.getColumnByName("F1");
			Assert.assertEquals("INTEGER", col1.getDataType());
			Assert.assertEquals(new Integer(4), col1.getJdbcIDOfDataType());
			Assert.assertEquals(22, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(0), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F2");
			Assert.assertEquals("FLOAT", col1.getDataType());
			Assert.assertEquals(new Integer(6), col1.getJdbcIDOfDataType());
			Assert.assertEquals(22, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(65), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F3");
			Assert.assertEquals("NUMBER", col1.getDataType());
			Assert.assertEquals(new Integer(-5), col1.getJdbcIDOfDataType());
			Assert.assertEquals(22, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(38), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F4");
			Assert.assertEquals("NUMBER", col1.getDataType());
			Assert.assertEquals(new Integer(2), col1.getJdbcIDOfDataType());
			Assert.assertEquals(22, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(38), col1.getPrecision());
			Assert.assertEquals(new Integer(2), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F5");
			Assert.assertEquals("NUMBER", col1.getDataType());
			Assert.assertEquals(new Integer(-5), col1.getJdbcIDOfDataType());
			Assert.assertEquals(22, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(38), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F6");
			Assert.assertEquals("NUMBER", col1.getDataType());
			Assert.assertEquals(new Integer(4), col1.getJdbcIDOfDataType());
			Assert.assertEquals(22, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(0), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

			col1 = table.getColumnByName("F7");
			Assert.assertEquals("FLOAT", col1.getDataType());
			Assert.assertEquals(new Integer(6), col1.getJdbcIDOfDataType());
			Assert.assertEquals(22, col1.getByteLength());
			Assert.assertEquals(true, col1.isNullable());
			Assert.assertEquals(new Integer(63), col1.getPrecision());
			Assert.assertEquals(new Integer(0), col1.getScale());
			Assert.assertEquals(null, col1.getCharUsed());
			Assert.assertEquals(null, col1.getDefaultValue());

		} finally {
			Closer.close(conn);
		}

	}

	//	@Test
	//	public void testGetSQLTable_NUMBER() throws Exception {
	//		Connection conn = TestUtil2.getOracleConn();
	//		Statement stmt = null;
	//		ResultSet rs = null;
	//		try {
	//			String sql = "SELECT * FROM \"MIGTESTFORHUDSON\".\"TEST_NUMBER\"";
	//			stmt = conn.createStatement();
	//			rs = stmt.executeQuery(sql);
	//			SourceTable sourceTable = new OracleDBObjectBuilder().getSQLTable(
	//					sql, rs.getMetaData());
	//			Table table = sourceTable.getTable();
	//
	//			for (Column col1 : table.getColumns()) {
	//				System.out.println(getSQLColumn(col1));
	//			}
	//
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//			Closer.close(conn);
	//		}
	//
	//	}

	//	@Test
	//	public void testBuildTableColumns_TIME() throws Exception {
	//		Connection conn = TestUtil2.getOracle10gConn();
	//		OracleSchemaFetcher builder = new OracleSchemaFetcher();
	//		final Catalog catalog = new Catalog();
	//		catalog.setSupportedDataType(builder.getSupportedSqlTypes(conn));
	//
	//		try {
	//			Schema schema = new Schema();
	//			schema.setName("MIGTESTFORHUDSON");
	//			Table table = new Table();
	//			table.setName("TIME_TABLE");
	//			builder.buildTableColumns(conn, catalog, schema, table);
	//
	//			for (Column col1 : table.getColumns()) {
	//				System.out.println(getSQLColumn(col1));
	//			}
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	private String getSQLColumn(Column col1) {
		StringBuffer bf = new StringBuffer();
		bf.append("Name: ").append(col1.getName()).append("\r\n");
		bf.append("DataType: ").append(col1.getDataType()).append("\r\n");
		bf.append("JdbcIDOfDataType: ").append(col1.getJdbcIDOfDataType()).append(
				"\r\n");
		bf.append("Nullable: ").append(col1.isNullable()).append("\r\n");
		bf.append("Precision: ").append(col1.getPrecision()).append("\r\n");
		bf.append("Scale: ").append(col1.getScale()).append("\r\n");
		bf.append("DefaultValue: ").append(col1.getDefaultValue()).append(
				"\r\n");
		bf.append("AddtionalInfo: ").append(col1.getCharUsed()).append("\r\n");
		bf.append("ByteLength: ").append(col1.getByteLength()).append("\r\n");

		return bf.toString();
	}

	//	@Test
	//	public void testGetSQLTable_TIME() throws Exception {
	//		Connection conn = TestUtil2.getOracleConn();
	//		Statement stmt = null;
	//		ResultSet rs = null;
	//		try {
	//			String sql = "SELECT * FROM \"MIGTESTFORHUDSON\".\"TEST_BINARY\"";
	//			stmt = conn.createStatement();
	//			rs = stmt.executeQuery(sql);
	//			SourceTable sourceTable = new OracleDBObjectBuilder().getSQLTable(
	//					sql, rs.getMetaData());
	//			Table table = sourceTable.getTable();
	//
	//			for (Column col1 : table.getColumns()) {
	//				System.out.println(getSQLColumn(col1));
	//			}
	//
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//			Closer.close(conn);
	//		}
	//	}

	//	@Test
	//	public void testBuildTableColumns_BFILE() throws Exception {
	//		Thread.sleep(5000);
	//		Connection conn = TestUtil2.getOracle10gConn();
	//		OracleSchemaFetcher builder = new OracleSchemaFetcher();
	//		final Catalog catalog = new Catalog();
	//		catalog.setSupportedDataType(builder.getSupportedSqlTypes(conn));
	//
	//		try {
	//			Schema schema = new Schema();
	//			schema.setName("MIGTESTFORHUDSON");
	//			Table table = new Table();
	//			table.setName("BFILE_TEST");
	//			builder.buildTableColumns(conn, catalog, schema, table);
	//
	//			for (Column col1 : table.getColumns()) {
	//				System.out.println(getSQLColumn(col1));
	//			}
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	//	@Test
	//	public void testGetSQLTable_BFILE() throws Exception {
	//		Thread.sleep(5000);
	//		Connection conn = TestUtil2.getOracleConn();
	//		Statement stmt = null;
	//		ResultSet rs = null;
	//		try {
	//			String sql = "SELECT * FROM \"MIGTESTFORHUDSON\".\"TEST_BINARY\"";
	//			stmt = conn.createStatement();
	//			rs = stmt.executeQuery(sql);
	//			SourceTable sourceTable = new OracleDBObjectBuilder().getSQLTable(
	//					sql, rs.getMetaData());
	//			Table table = sourceTable.getTable();
	//
	//			Column col1 = table.getColumnByName("F2");
	//			Assert.assertEquals("BFILE", col1.getDataType());
	//			Assert.assertEquals(true, col1.isNullable());
	//			Assert.assertEquals(new Integer(0), col1.getPrecision());
	//			Assert.assertEquals(new Integer(0), col1.getScale());
	//			Assert.assertEquals(null, col1.getAddtionalInfo());
	//			Assert.assertEquals(null, col1.getDefaultValue());
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//			Closer.close(conn);
	//		}
	//	}

	@Test
	public void testBuildSQLTable() throws Exception {
		OracleSchemaFetcher builder = (OracleSchemaFetcher) DatabaseType.ORACLE.getMetaDataBuilder();

		MigrationConfiguration config = TemplateParserTest.getOracleConfig();
		Connection con = config.getSourceConParams().createConnection();
		try {
			Assert.assertNotNull(builder.buildSQLTable(con.createStatement().executeQuery(
					"select * from code").getMetaData()));
		} finally {
			con.close();
		}

	}
}
