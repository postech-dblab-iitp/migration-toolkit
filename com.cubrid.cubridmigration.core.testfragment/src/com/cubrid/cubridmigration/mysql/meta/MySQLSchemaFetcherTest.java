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
package com.cubrid.cubridmigration.mysql.meta;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.TestUtil2;
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.dbobject.Version;
import com.cubrid.cubridmigration.mysql.export.MySQLExportHelper;
import com.cubrid.cubridmigration.mysql.meta.MySQLSchemaFetcher;

/**
 * 
 * DbUtilTest
 * 
 * @author moulinwang
 * @author JessieHuang
 * @version 1.0 - 2009-9-18
 */
public class MySQLSchemaFetcherTest {

	private static final String TEST_NUMBER = "test_number";
	private static final String MIGTESTFORHUDSON = "migtestforhudson";

	//	@Test
	//	public void testGetAvailableSqlTypes() throws Exception {
	//		Connection conn = TestUtil2.getMySQL5520Conn();
	//		DatabaseType.MYSQL.getMetaDataBuilder().getSupportedSqlTypes(conn);
	//	}

	@Test
	public void testSupportPartitionVersion() {

		final MySQLSchemaFetcher builder = new MySQLSchemaFetcher();
		Version version = new Version();
		version.setDbMajorVersion(4);
		version.setDbMinorVersion(9);
		Assert.assertFalse(builder.isSupportParitionVersion(version));
		version.setDbMajorVersion(5);
		version.setDbMinorVersion(0);
		Assert.assertFalse(builder.isSupportParitionVersion(version));
		version.setDbMajorVersion(5);
		version.setDbMinorVersion(1);
		Assert.assertTrue(builder.isSupportParitionVersion(version));
		version.setDbMajorVersion(6);
		version.setDbMinorVersion(0);
		Assert.assertTrue(builder.isSupportParitionVersion(version));
	}

	//	/**
	//	 * test GetSchemata
	//	 * 
	//	 * @e
	//	 * @throws SQLException e
	//	 */
	//	@Test
	//	public final void testGetSchemata() throws SQLException {
	//		Connection conn = TestUtil2.getMySQL5520Conn();
	//		try {
	//			List<String> list = new MYSQLSchemaFetcher().getSchemata(conn);
	//			System.out.println(list);
	//			Assert.assertTrue(list.size() > 0);
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	//	/**
	//	 * test GetCharSet
	//	 * 
	//	 * @e
	//	 * @throws SQLException e
	//	 */
	//	@Test
	//	public final void testGetCharSet() throws SQLException {
	//		Connection conn = TestUtil2.getMySQLConn();
	//		try {
	//			String dbName = MIGTESTFORHUDSON;
	//			String charSet = new MysqlDBObjectBuilder().getCharSet(conn, dbName);
	//			Assert.assertEquals("latin1", charSet);
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	/**
	 * testBuildProcedures
	 * 
	 * @e
	 * @throws SQLException e
	 */
	@Test
	public final void testBuildProcedures() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			Catalog catalog = new MySQLSchemaFetcher().buildCatalog(conn,
					TestUtil2.getMySQLConParam(), null);
			List<Procedure> procList = catalog.getSchemas().get(0).getProcedures();
			Assert.assertEquals(1, procList.size());

			List<Function> funcList = catalog.getSchemas().get(0).getFunctions();
			Assert.assertTrue(funcList.size() > 0);
		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * testBuildTriggers
	 * 
	 * @e
	 * @throws SQLException e
	 */
	@Test
	public final void testBuildTriggers() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			Catalog catalog = new MySQLSchemaFetcher().buildCatalog(conn,
					TestUtil2.getMySQLConParam(), null);
			List<Trigger> list = catalog.getSchemas().get(0).getTriggers();

			for (Trigger trig : list) {
				System.out.println(trig.getDDL());
			}

		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * 
	 * getAutoIncMaxValByTableName
	 * 
	 * @e
	 * @throws SQLException e
	 */
	@Test
	public final void testGetAutoIncMaxValByTableName() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			String tableName = TEST_NUMBER;
			Long maxVal = new MySQLSchemaFetcher().getAutoIncNextValByTableName(
					conn, tableName);
			Assert.assertTrue(maxVal >= 0);
		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * test GetTimezone
	 * 
	 * @e
	 * @throws SQLException e
	 */
	@Test
	public final void testGetTimezone() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			String timezone = new MySQLSchemaFetcher().getTimezone(conn);
			System.out.println("timezone: " + timezone);
			Assert.assertEquals("GMT+09:00", timezone);
		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * testBuildCatalog
	 * 
	 * @e
	 */
	@Test
	public final void testBuildCatalog() {
		Connection conn = null;
		try {
			conn = TestUtil2.getMySQL5520Conn();
			Catalog catalog = new MySQLSchemaFetcher().buildCatalog(conn,
					TestUtil2.getMySQLConParam(), null);

			String json = TestUtil2.getCatalogJson(catalog);
			
			String sb = TestUtil2.readStrFromFile("/com/cubrid/cubridmigration/mysql/meta/schema.json");
			Assert.assertEquals(
					sb.replaceAll("\r\n", " ").replaceAll("\r", " ").replaceAll(
							"\n", " "),
					json.replaceAll("\r\n", " ").replaceAll("\r", " ").replaceAll(
							"\n", " "));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Closer.close(conn);
		}

	}

	/**
	 * testBuildTableColumns
	 * 
	 * @e
	 * @throws SQLException e
	 */
	@Test
	public final void testBuildTableColumns() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			Catalog catalog = new Catalog();
			catalog.setName(MIGTESTFORHUDSON);
			Version version = new Version();
			version.setDbMajorVersion(4);
			catalog.setVersion(version);
			Schema schema = new Schema(catalog);
			schema.setName(MIGTESTFORHUDSON);
			Table table = new Table(schema);
			table.setName(TEST_NUMBER);
			new MySQLSchemaFetcher().buildTableColumns(conn, catalog, schema,
					table);
			System.out.println("column count:" + table.getColumns().size());
			Assert.assertTrue(table.getColumns().size() >= 0);
			version.setDbMajorVersion(5);
			new MySQLSchemaFetcher().buildTableColumns(conn, catalog, schema,
					table);
		} finally {
			Closer.close(conn);
		}
	}

	//	/**
	//	 * testBuildViews
	//	 * 
	//	 * @e
	//	 * @throws SQLException e
	//	 */
	//	@Test
	//	public final void testBuildViews() throws SQLException {
	//		Connection conn = TestUtil2.getMySQL5520Conn();
	//		try {
	//			Catalog catalog = new Catalog();
	//			catalog.setName(MIGTESTFORHUDSON);
	//			//			Version version = new MYSQLSchemaFetcher().getVersion(conn);
	//			//			version.setDbMajorVersion(4);
	//			//			catalog.setVersion(version);
	//			Schema schema = new Schema(catalog);
	//			new MYSQLSchemaFetcher().buildViews(conn, catalog, schema, null);
	//			List<View> list = schema.getViews();
	//			Assert.assertTrue(list.size() >= 0);
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	/**
	 * testBuildTableIndexes
	 * 
	 * @throws SQLException e
	 */
	@Test
	public final void testBuildTableIndexes() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			final MySQLSchemaFetcher mysqlDBObjectBuilder = new MySQLSchemaFetcher();
			Catalog catalog = mysqlDBObjectBuilder.buildCatalog(conn,
					TestUtil2.getMySQLConParam(), null);

			Table table = catalog.getSchemas().get(0).getTableByName(
					"participant");

			System.out.println("table.getIndexes().size()="
					+ table.getIndexes().size());
			Assert.assertTrue(table.getIndexes().size() >= 0);

			//Test getSourcePartitionDLL
			table.setDDL("create table participant () PARTITION BY f1 hash 4");
			mysqlDBObjectBuilder.getSourcePartitionDDL(table);

		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * test GetTableDDL
	 * 
	 * @e
	 * @throws SQLException e
	 */
	@Test
	public final void testGetTableDDL() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			String ddl = new MySQLSchemaFetcher().getTableDDL(conn, TEST_NUMBER);
			System.out.println(ddl);
			Assert.assertTrue(ddl != null && ddl.trim().length() > 0);

		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * test GetViewDDL
	 * 
	 * @e
	 * @throws SQLException e
	 */
	@Test
	public final void testGetViewDDL() throws SQLException {
		String viewName = "game_view";
		Connection conn = TestUtil2.getMySQL5520Conn();
		try {
			String ddl = new MySQLSchemaFetcher().getViewDDL(conn, viewName);
			System.out.println(ddl);
			Assert.assertTrue(ddl != null && ddl.trim().length() > 0);
		} finally {
			Closer.close(conn);
		}
	}

	/**
	 * testSelectTable
	 * 
	 * @e
	 * @throws SQLException e
	 */
	@Test
	public final void testSelectTable() throws SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		Statement stmt = null;
		ResultSet rs = null;

		try {
			String sql = "select * from " + TEST_NUMBER;
			stmt = conn.createStatement();

			rs = stmt.executeQuery(sql);

			ResultSetMetaData resultSetMeta = rs.getMetaData();

			for (int i = 1; i < resultSetMeta.getColumnCount() + 1; i++) {

				//				String tableName = resultSetMeta.getTableName(i);
				String columnName = resultSetMeta.getColumnName(i);
				String type = resultSetMeta.getColumnTypeName(i);

				System.out.print(columnName + "\t" + type + "\t");
				System.out.print("id=" + resultSetMeta.getColumnType(i) + "\t");
				System.out.print("size="
						+ resultSetMeta.getColumnDisplaySize(i) + "\t");

				int precision = resultSetMeta.getPrecision(i);
				int scale = resultSetMeta.getScale(i);

				System.out.print("p=" + precision + "\t" + "s=" + scale + "\t");
				System.out.print(resultSetMeta.isAutoIncrement(i) + "\t");
				System.out.print(resultSetMeta.isNullable(i) + "\t");

				System.out.println();
			}
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
			Closer.close(conn);
		}

	}

	//	/**
	//	 * test GetObject
	//	 * 
	//	 * @e
	//	 * @throws CloneNotSupportedException e
	//	 * @throws IOException e
	//	 * @throws SQLException e
	//	 */
	//	@Test
	//	public final void testGetObject() throws CloneNotSupportedException,
	//			IOException,
	//			SQLException {
	//		Connection conn = TestUtil2.getMySQLConn();
	//		Version version = new MysqlDBObjectBuilder().getVersion(conn);
	//
	//		MysqlDBObjectBuilder helper = new MysqlDBObjectBuilder();
	//		CUBRIDDDLUtil util = new CUBRIDDDLUtil();
	//
	//		File file = new File("./");
	//		File ddl = new File(file.getAbsolutePath() + "d3.txt");
	//		File dataFile = new File(file.getAbsolutePath() + "data3.txt");
	//
	//		Catalog catalog = new Catalog();
	//		catalog.setName(MIGTESTFORHUDSON);
	//		catalog.setVersion(version);
	//		Schema schema = new Schema(catalog);
	//		schema.setName(MIGTESTFORHUDSON);
	//
	//		String tableName = TEST_NUMBER;
	//		Table table1 = new Table(schema);
	//		table1.setName(tableName);
	//		helper.buildTableColumns(conn, catalog, schema, table1);
	//		helper.buildTablePK(conn, catalog, schema, table1);
	//
	//		List<Column> list = table1.getColumns();
	//
	//		MigrationConfiguration config = new MigrationConfiguration();
	//		SourceEntryTableConfig setc = new SourceEntryTableConfig();
	//		Table clone1 = DatabaseType.MYSQL.getTranformHelper().createCUBRIDTable(
	//				setc, table1, config);
	//		clone1.setName("datatype1");
	//
	//		StringBuffer bf = new StringBuffer();
	//		bf.append("%class ").append(clone1.getName());
	//		bf.append(" (");
	//
	//		List<Column> columns = clone1.getColumns();
	//
	//		for (int i = 0; i < columns.size(); i++) {
	//			Column column = columns.get(i);
	//
	//			if (i != 0) {
	//				bf.append(" ");
	//			}
	//
	//			bf.append(column.getName());
	//		}
	//
	//		bf.append(")").append("\n");
	//
	//		PreparedStatement stmt = null;
	//		ResultSet rs = null;
	//		try {
	//			String sql = "select * from " + tableName;
	//			stmt = conn.prepareStatement(sql);
	//			rs = stmt.executeQuery();
	//			while (rs.next()) {
	//				List<String> dataList = new ArrayList<String>();
	//
	//				for (int i = 0; i < list.size(); i++) {
	//					if (i == 9) {
	//						Reader reader = rs.getCharacterStream(i + 1);
	//						if (reader == null) {
	//							continue;
	//						}
	//						StringBuffer buffer = new StringBuffer();
	//						char[] buf = new char[1024];
	//
	//						int len = reader.read(buf);
	//						while (len != -1) {
	//							buffer.append(buf, 0, len);
	//							len = reader.read(buf);
	//						}
	//
	//						reader.close();
	//						System.out.println(buffer.toString());
	//					}
	//				}
	//
	//				bf.append(LoadDBFileUtils.getRecordString(columns, dataList));
	//			}
	//
	//			System.out.println(util.getTableDDL(clone1));
	//			System.out.println(bf.toString());
	//			IOUtils.writeLines(ddl, new String[]{util.getTableDDL(clone1) },
	//					null);
	//			IOUtils.writeLines(dataFile, new String[]{bf.toString() }, null);
	//
	//			boolean isDel1 = ddl.delete();
	//			boolean isDel2 = dataFile.delete();
	//
	//			System.out.println(isDel1);
	//			System.out.println(isDel2);
	//		} finally {
	//			Closer.close(rs);
	//			Closer.close(stmt);
	//			File file1 = new File("./d3.txt");
	//			file1.delete();
	//			file1 = new File("./data3.txt");
	//			file1.delete();
	//		}
	//	}

	/**
	 * test GetObject
	 * 
	 * @e
	 * @throws CloneNotSupportedException e
	 * @throws IOException e
	 * @throws SQLException e
	 */
	@Test
	public final void testGetValue() throws CloneNotSupportedException,
			IOException,
			SQLException {
		Connection conn = TestUtil2.getMySQL5520Conn();
		MySQLExportHelper mySQLExportHelper = new MySQLExportHelper();
		//ConnParameters params = TestUtil2.getMySQLConParam();
		//mySQLExportHelper.setDriverProperties(MysqlDBObjectBuilder.getDatabaseProperties(params));

		MySQLSchemaFetcher helper = new MySQLSchemaFetcher();

		Catalog catalog = helper.buildCatalog(conn,
				TestUtil2.getMySQLConParam(), null);
		Schema schema = catalog.getSchemas().get(0);

		for (Table table : schema.getTables()) {
			String tableName = table.getName();
			List<Column> list = table.getColumns();

			Statement stmt = null;
			ResultSet rs = null;
			try {
				String sql = "select * from `" + tableName + "`";
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);

				int m = 0;
				while (rs.next() && m < 10) {

					for (int i = 0; i < list.size(); i++) {
						Column column = list.get(i);

						Object obj = mySQLExportHelper.getJdbcObject(rs, column);

						System.out.println(obj == null ? null : obj.toString());

						System.out.println();

					}

					m++; //just get top 10
				}

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				Closer.close(rs);
				Closer.close(stmt);
			}
		}

	}

	//	/**
	//	 * testGetTableRowCntBySchemaName
	//	 * 
	//	 * @e
	//	 * @throws SQLException e
	//	 */
	//	@Test
	//	public final void testGetTableRowCntBySchemaName() throws SQLException {
	//		Connection conn = TestUtil2.getMySQL5520Conn();
	//		try {
	//			Map<String, String> map = new MYSQLSchemaFetcher().getTableRowCntBySchemaName(
	//					conn, MIGTESTFORHUDSON);
	//
	//			Assert.assertNotNull(map);
	//		} finally {
	//			Closer.close(conn);
	//		}
	//	}

	//	/**
	//	 * testGetSQLTable
	//	 * 
	//	 * @e
	//	 * @throws SQLException e
	//	 */
	//	@Test
	//	public final void testGetSQLTable() throws SQLException {
	//		Connection conn = TestUtil2.getMySQLConn();
	//		PreparedStatement rst = null;
	//		try {
	//			String sqlStr = "select * from " + TEST_NUMBER;
	//			rst = conn.prepareStatement(sqlStr);
	//			SourceTable table = new MysqlDBObjectBuilder().buildSQLTableSchema(rst.getMetaData());
	//			Assert.assertEquals(sqlStr, table.getSql());
	//		} catch (SQLException ex) {
	//			ex.printStackTrace();
	//		} finally {
	//			Closer.close(rst);
	//			Closer.close(conn);
	//		}
	//	}

	//@Test
	//	public void testGetCharset() {
	//		String ddl = "CREATE ALGORITHM=UNDEFINED DEFINER=`mydbadmin`@`192.168.63.34` SQL SECURITY DEFINER VIEW `tgtview` AS "
	//				+ "select `tgt`.`d` AS `d`,`tgt`.`gsdf` AS `gsdf`,`tgt`.`ff` AS `ff`,`tgt`.`aa` AS `aa`,`tgt`.`t1` AS `t1` from `tgt`";
	//		String charset = MysqlDBObjectBuilder.getCharset(ddl);
	//		Assert.assertNull(charset);
	//	}

	//	//@Test
	//	public void testInsertion() {
	//		String url = "jdbc:mysql://192.168.63.221:3306/large?user=yy&password=123";
	//		Connection connection = null;
	//		PreparedStatement statement = null;
	//		try {
	//			Class.forName("com.mysql.jdbc.Driver");
	//			connection = DriverManager.getConnection(url);
	//			connection.setAutoCommit(false);
	//			//                 assertNotNull(connection);
	//			File file = new File("/root/1.pdf");
	//			String insertion = "insert into tb_large_table(id,column1,column2,column3,column4,column5) values("
	//					+ "?,11111111,222222,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',now(),?)";
	//
	//			statement = connection.prepareStatement(insertion);
	//
	//			for (long i = 10350; i < 10450; i++) {
	//
	//				InputStream fileInputStream = new FileInputStream(file);
	//				statement.setLong(1, i);
	//				statement.setBinaryStream(2, fileInputStream,
	//						(int) file.length());
	//				statement.executeUpdate();
	//				if (i % 20 == 0) {
	//					System.out.println(i);
	//					connection.commit();
	//				}
	//				fileInputStream.close();
	//			}
	//		} catch (ClassNotFoundException e) {
	//			e.printStackTrace();
	//		} catch (SQLException e) {
	//			e.printStackTrace();
	//		} catch (FileNotFoundException e) {
	//			e.printStackTrace();
	//		} catch (IOException e) {
	//			e.printStackTrace();
	//		} finally {
	//			try {
	//				if (statement != null) {
	//					statement.close();
	//				}
	//				if (connection != null) {
	//					connection.close();
	//				}
	//			} catch (SQLException e) {
	//				e.printStackTrace();
	//			}
	//		}
	//	}
}
