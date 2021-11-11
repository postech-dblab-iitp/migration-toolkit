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
package com.cubrid.cubridmigration.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.connection.JDBCUtil;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.mysql.meta.MySQLSchemaFetcherTest;

public class TestUtil2 {

	private static final JdbcConfig CUBRID = new JdbcConfig();
	private static final JdbcConfig MYSQL = new JdbcConfig();
	private static final JdbcConfig ORACLE = new JdbcConfig();
	private static final JdbcConfig MSSQL = new JdbcConfig();
	private static Properties JDBC_CONFIG = new Properties();
	static {
		try {
			PathUtils.initPaths();
			JDBCUtil.initialJdbcByPath(getJdbcPath());
			JDBC_CONFIG.load(TestUtil2.class.getResourceAsStream("/com/cubrid/cubridmigration/jdbc.properties"));
			CUBRID.driverFile = JDBC_CONFIG.getProperty("CUBRID.driverFile");
			CUBRID.host = JDBC_CONFIG.getProperty("CUBRID.host");
			CUBRID.database = JDBC_CONFIG.getProperty("CUBRID.database");
			CUBRID.port = Integer.valueOf(JDBC_CONFIG.getProperty("CUBRID.port"));
			CUBRID.username = JDBC_CONFIG.getProperty("CUBRID.username");
			CUBRID.password = JDBC_CONFIG.getProperty("CUBRID.password");

			MYSQL.driverFile = JDBC_CONFIG.getProperty("MYSQL.driverFile");
			MYSQL.host = JDBC_CONFIG.getProperty("MYSQL.host");
			MYSQL.database = JDBC_CONFIG.getProperty("MYSQL.database");
			MYSQL.port = Integer.valueOf(JDBC_CONFIG.getProperty("MYSQL.port"));
			MYSQL.username = JDBC_CONFIG.getProperty("MYSQL.username");
			MYSQL.password = JDBC_CONFIG.getProperty("MYSQL.password");

			ORACLE.driverFile = JDBC_CONFIG.getProperty("ORACLE.driverFile");
			ORACLE.host = JDBC_CONFIG.getProperty("ORACLE.host");
			ORACLE.database = JDBC_CONFIG.getProperty("ORACLE.database");
			ORACLE.port = Integer.valueOf(JDBC_CONFIG.getProperty("ORACLE.port"));
			ORACLE.username = JDBC_CONFIG.getProperty("ORACLE.username");
			ORACLE.password = JDBC_CONFIG.getProperty("ORACLE.password");

			MSSQL.driverFile = JDBC_CONFIG.getProperty("MSSQL.driverFile");
			MSSQL.host = JDBC_CONFIG.getProperty("MSSQL.host");
			MSSQL.database = JDBC_CONFIG.getProperty("MSSQL.database");
			MSSQL.port = Integer.valueOf(JDBC_CONFIG.getProperty("MSSQL.port"));
			MSSQL.username = JDBC_CONFIG.getProperty("MSSQL.username");
			MSSQL.password = JDBC_CONFIG.getProperty("MSSQL.password");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static class JdbcConfig {
		private String driverFile;
		private String host;
		private String database;
		private int port;
		private String username;
		private String password;
	}

	public static String getJdbcPath() {
		//URL url = ClassLoader.getSystemResource(".");
		//System.out.println("Root:" + url.toString());
		String driverPath = System.getProperty("user.dir");// PathUtils.getURLFilePath(url);
		//driverPath += "../../com.cubrid.cubridmigration.core/jdbc/";
		driverPath += "/jdbc/";
		try {
			return new File(driverPath).getCanonicalPath() + "/";
		} catch (IOException e) {
			return null;
		}
	}

	@Test
	public void testGetJDBCPath() {
		getJdbcPath();
	}

	/**
	 * get a Connection of MySQL 5.5.20
	 * 
	 * @return Connection
	 * @throws MigrationException e
	 * @throws SQLException e
	 */
	public static Connection getMySQL5520Conn() throws SQLException {
		ConnParameters params = getMySQLConParam();
		return params.createConnection();
	}

	public static ConnParameters getMySQLConParam() {
		ConnParameters params = ConnParameters.getConParam("mysqlconnection", MYSQL.host,
				MYSQL.port, MYSQL.database, DatabaseType.MYSQL, "", MYSQL.username, MYSQL.password,
				MYSQL.driverFile, null);
		return params;
	}

	/**
	 * get a Connection of CUBRID 8.4.1
	 * 
	 * @return Connection
	 * @throws MigrationException e
	 * @throws SQLException e
	 * @throws URISyntaxException
	 */
	public static Connection getCUBRIDConn() throws SQLException {
		ConnParameters params = ConnParameters.getConParam("cubridconnection", CUBRID.host,
				CUBRID.port, CUBRID.database, DatabaseType.CUBRID, "UTF-8", CUBRID.username,
				CUBRID.password, CUBRID.driverFile, null);
		return params.createConnection();
	}

	/**
	 * get a Connection of CUBRID 8.4.1
	 * 
	 * @return ConnParameters
	 * @throws MigrationException e
	 * @throws SQLException e
	 * @throws URISyntaxException
	 */
	public static ConnParameters getCUBRIDConnParam() throws SQLException {
		ConnParameters params = ConnParameters.getConParam("cubridconnection", CUBRID.host,
				CUBRID.port, CUBRID.database, DatabaseType.CUBRID, "UTF-8", CUBRID.username,
				CUBRID.password, CUBRID.driverFile, null);
		return params;
	}

	/**
	 * get a Connection of Oracle 10g
	 * 
	 * @return Connection
	 * @throws MigrationException e
	 * @throws SQLException e
	 * @throws URISyntaxException
	 */
	public static Connection getOracle10gConn() throws SQLException {
		ConnParameters params = ConnParameters.getConParam("oracleconnection", ORACLE.host,
				ORACLE.port, ORACLE.database, DatabaseType.ORACLE, "", ORACLE.username,
				ORACLE.password, ORACLE.driverFile, null);

		return params.createConnection();
	}

	/**
	 * get a Connection of Oracle 10g
	 * 
	 * @return Connection
	 * @throws MigrationException e
	 * @throws SQLException e
	 * @throws URISyntaxException
	 */
	public static ConnParameters getOracle10gConnParam() throws SQLException {
		ConnParameters params = ConnParameters.getConParam("oracleconnection", ORACLE.host,
				ORACLE.port, ORACLE.database, DatabaseType.ORACLE, "", ORACLE.username,
				ORACLE.password, ORACLE.driverFile, null);

		return params;
	}

	/**
	 * get a Connection
	 * 
	 * @return Connection
	 * @throws MigrationException e
	 */
	public static ConnParameters getMSSQLConnParam() {
		//		String driverPath = getJdbcPath()
		//				+ (VMUtils.isJVM15() ? "sqljdbc.jar" : "sqljdbc4.jar");
		return ConnParameters.getConParam("mssqlConnection", MSSQL.host, MSSQL.port,
				MSSQL.database, DatabaseType.MSSQL, "", MSSQL.username, MSSQL.password,
				MSSQL.driverFile, null);
	}

	@Test
	public void pathTest() {
		System.out.println(Thread.currentThread().getContextClassLoader().getResource(""));

		System.out.println(MySQLSchemaFetcherTest.class.getClassLoader().getResource(""));
		System.out.println(ClassLoader.getSystemResource(""));
		System.out.println(MySQLSchemaFetcherTest.class.getResource(""));
		System.out.println(MySQLSchemaFetcherTest.class.getResource("/")); // Class�ļ�����·��
		System.out.println(new File("/").getAbsolutePath());
		System.out.println(System.getProperty("user.dir"));
	}

	public void equalConnectionParameters(ConnParameters connectionParameters1,
			ConnParameters connectionParameters2) {
		Assert.assertEquals(connectionParameters1.getCharset(), connectionParameters2.getCharset());
		Assert.assertEquals(connectionParameters1.getDatabaseType(),
				connectionParameters2.getDatabaseType());
		Assert.assertEquals(connectionParameters1.getDriverClass(),
				connectionParameters2.getDriverClass());
		Assert.assertEquals(connectionParameters1.getConPassword(),
				connectionParameters2.getConPassword());

		Assert.assertEquals(connectionParameters1.getUrl(), connectionParameters2.getUrl());
		Assert.assertEquals(connectionParameters1.getConUser(), connectionParameters2.getConUser());
	}

	//	public void equalSourceTable(SourceTable table1, SourceTable table2) {
	//		Assert.assertEquals(table1.getSql(), table2.getSql());
	//		Assert.assertEquals(table1.getTableType(), table2.getTableType());
	//		Assert.assertEquals(table1.isExportRecordsFlag(),
	//				table2.isExportRecordsFlag());
	//		Assert.assertEquals(table1.getFilter(), table2.getFilter());
	//
	//		equalTable(table1.getTable(), table2.getTable());
	//
	//	}

	public void equalView(View view1, View view2) {
		Assert.assertEquals(view1.getName(), view2.getName());
		Assert.assertEquals(view1.getDDL(), view2.getDDL());
		Assert.assertEquals(view1.getDDL(), view2.getDDL());

		List<Column> columns1 = view1.getColumns();
		List<Column> columns2 = view2.getColumns();
		equalColumnList(columns1, columns2);

	}

	public void equalColumnList(List<Column> columns1, List<Column> columns2) {
		Assert.assertEquals(columns1.size(), columns2.size());
		for (int i = 0; i < columns1.size(); i++) {
			Column column1 = columns1.get(i);
			Column column2 = columns2.get(i);
			equalColumn(column1, column2);
		}
	}

	public void equalTable(Table table1, Table table2) {
		Assert.assertEquals(table1.getName(), table2.getName());
		Assert.assertEquals(table1.getTableRowCount(), table2.getTableRowCount());
		Assert.assertEquals(table1.getDDL(), table2.getDDL());

		Assert.assertEquals(table1.getColumns().size(), table2.getColumns().size());
		for (int i = 0; i < table1.getColumns().size(); i++) {
			Column column1 = table1.getColumns().get(i);
			Column column2 = table2.getColumns().get(i);
			equalColumn(column1, column2);
		}

		List<Index> indexes1 = table1.getIndexes();
		List<Index> indexes2 = table2.getIndexes();
		equalIndexList(indexes1, indexes2);

		List<FK> fks1 = table1.getFks();
		List<FK> fks2 = table2.getFks();
		equalFKList(fks1, fks2);

		equalPK(table1.getPk(), table2.getPk());

	}

	public void equalFKList(List<FK> fks1, List<FK> fks2) {
		if (fks1 == null && fks2.size() == 0) {
			return;
		}
		Assert.assertEquals(fks1.size(), fks2.size());
		for (int i = 0; i < fks1.size(); i++) {
			FK fk1 = fks1.get(i);
			FK fk2 = fks2.get(i);
			equalFK(fk1, fk2);
		}
	}

	public void equalIndexList(List<Index> indexes1, List<Index> indexes2) {
		Assert.assertEquals(indexes1.size(), indexes2.size());
		for (int i = 0; i < indexes1.size(); i++) {
			Index index1 = indexes1.get(i);
			Index index2 = indexes2.get(i);
			equalIndex(index1, index2);
		}
	}

	public void equalPK(PK pk1, PK pk2) {
		if (pk1 == null && pk2 == null || pk1 == pk2) {
			return;
		}
		if (pk1 == null || pk2 == null) {
			return;
		}
		Assert.assertEquals(pk1.getName(), pk2.getName());

		Assert.assertEquals(pk1.getPkColumns().size(), pk2.getPkColumns().size());
		for (int i = 0; i < pk1.getPkColumns().size(); i++) {
			String column1 = pk1.getPkColumns().get(i);
			String column2 = pk2.getPkColumns().get(i);
			Assert.assertEquals(column1, column2);
		}

	}

	public void equalFK(FK fk1, FK fk2) {
		Assert.assertEquals(fk1.getName(), fk2.getName());
		//Assert.assertEquals(fk1.getDeferability(), fk2.getDeferability());
		Assert.assertEquals(fk1.getName(), fk2.getName());
		Assert.assertEquals(fk1.getReferencedTableName(), fk2.getReferencedTableName());
		Assert.assertEquals(fk1.getUpdateRule(), fk2.getUpdateRule());

		Assert.assertEquals(fk1.getCol2RefMapping().size(), fk2.getCol2RefMapping().size());
		for (int i = 0; i < fk1.getCol2RefMapping().size(); i++) {
			Assert.assertEquals(fk1.getCol2RefMapping().get(i), fk2.getCol2RefMapping().get(i));
		}

	}

	public void equalIndex(Index index1, Index index2) {
		Assert.assertEquals(index1.getName(), index2.getName());
		Assert.assertEquals(index1.isUnique(), index2.isUnique());
		Assert.assertEquals(index1.getIndexType(), index2.getIndexType());

		Assert.assertEquals(index1.getIndexColumns().size(), index2.getIndexColumns().size());
		for (int i = 0; i < index1.getIndexColumns().size(); i++) {
			String column1 = index1.getColumnNames().get(i);
			String column2 = index2.getColumnNames().get(i);
			Assert.assertEquals(column1, column2);
		}

		List<String> columnOrderRules = index1.getColumnOrderRulesString();
		List<String> columnOrderRules2 = index2.getColumnOrderRulesString();
		equalStringList(columnOrderRules, columnOrderRules2);

	}

	public void equalStringList(List<String> strs1, List<String> strs2) {
		Assert.assertEquals(strs1.size(), strs2.size());
		for (int i = 0; i < strs1.size(); i++) {
			String columnOrderRule1 = strs1.get(i);
			String columnOrderRule2 = strs2.get(i);
			Assert.assertEquals(columnOrderRule1, columnOrderRule2);
		}
	}

	public void equalColumn(Column column1, Column column2) {
		Assert.assertEquals(column1.getClass(), column2.getClass());

		Assert.assertEquals(column1.getName(), column2.getName());
		Assert.assertEquals(column1.getAutoIncSeedVal(), column2.getAutoIncSeedVal());
		Assert.assertEquals(column1.getDataType(), column2.getDataType());

		Assert.assertEquals(column1.getDefaultValue(), column2.getDefaultValue());
		Assert.assertEquals(column1.getCharLength(), column2.getCharLength());
		Assert.assertEquals(column1.getPrecision(), column2.getPrecision());
		Assert.assertEquals(column1.getScale(), column2.getScale());

		Assert.assertEquals(column1.getShownDataType(), column2.getShownDataType());

		Assert.assertEquals(column1.isAutoIncrement(), column2.isAutoIncrement());
		Assert.assertEquals(column1.isNullable(), column2.isNullable());
		Assert.assertEquals(column1.isUnique(), column2.isUnique());

		if (column1.getClass().equals(Column.class)) {
			Assert.assertEquals(((Column) column1).getCharset(), ((Column) column2).getCharset());
			Assert.assertEquals(((Column) column1).getShownDataType(),
					((Column) column2).getShownDataType());
			Assert.assertEquals(((Column) column1).getSubDataType(), (column2).getSubDataType());
		}
	}

	public static String getCatalogJson(Catalog catalog) {
		String level2 = "	";
		String level3 = "		";
		String level4 = "			";
		StringBuffer sb = new StringBuffer("{\n");
		sb.append("name:").append(catalog.getName()).append(",\n");
		sb.append("schemas:[");
		int ischema = 0;
		for (Schema schema : catalog.getSchemas()) {
			if (ischema > 0) {
				sb.append(",");
			}
			ischema++;
			sb.append("\n").append(level2);
			sb.append("{name:").append(schema.getName()).append(",\n");
			sb.append(level2).append("tables:[");
			int itable = 0;
			for (Table table : schema.getTables()) {
				if (itable > 0) {
					sb.append(",");
				}

				itable++;
				sb.append("\n").append(level3);
				sb.append("{name:").append(table.getName());
				sb.append(",owner:").append(table.getOwner());
				String pDDL = table.getPartitionInfo() == null ? "null"
						: table.getPartitionInfo().getDDL();
				sb.append(",partitionDDL:").append(pDDL);
				sb.append(",reuseOID:").append(table.isReuseOID()).append(",");
				sb.append("\n");
				sb.append(level3).append("columns:[\n");
				int icol = 0;
				for (Column col : table.getColumns()) {
					if (icol > 0) {
						sb.append(",\n");
					}
					icol++;
					sb.append(level4).append("{name:").append(col.getName());
					sb.append(",type_id:").append(col.getJdbcIDOfSubDataType());
					sb.append(",base_type:").append(col.getDataType());
					sb.append(",precision:").append(col.getPrecision());
					sb.append(",scale:").append(col.getScale());
					sb.append(",show_type:").append(col.getShownDataType());
					sb.append(",subtype:").append(col.getSubDataType());
					sb.append(",is_inc:").append(col.isAutoIncrement());
					sb.append(",inc_value:").append(col.getAutoIncIncrVal());
					sb.append(",inc_seed_value:").append(col.getAutoIncSeedVal());
					sb.append(",byte_length:").append(col.getByteLength());
					sb.append(",char_length:").append(col.getCharLength());
					sb.append(",charset:").append(col.getCharset());
					sb.append(",char_used:").append(col.getCharUsed());
					sb.append(",enum_values:").append(col.getEnumElements());
					sb.append(",shared:").append(col.isShared());
					sb.append(",shared_value:").append(col.getSharedValue());
					sb.append(",nullable:").append(col.isNullable());
					sb.append(",unique:").append(col.isUnique());
					sb.append(",default:").append(col.getDefaultValue());
					sb.append("}");
				}
				sb.append("],\n");
				PK pk = table.getPk();
				if (pk != null) {
					sb.append(level3).append("pk:{name:");
					sb.append(pk.getName());
					sb.append(",columns:[");
					int i = 0;
					for (String pkCol : pk.getPkColumns()) {
						if (i > 0) {
							sb.append(",");
						}
						i++;
						sb.append(pkCol);
					}
					sb.append("]},\n");
				}
				sb.append(level3).append("fks:[");
				int ifk = 0;
				for (FK fk : table.getFks()) {
					if (ifk > 0) {
						sb.append(",");
					}
					ifk++;
					sb.append("\n").append(level4);
					sb.append("{name:").append(fk.getName());
					sb.append(",reftable:").append(fk.getReferencedTableName());
					sb.append(",delrule:").append(fk.getDeleteRule());
					sb.append(",updaterule:").append(fk.getUpdateRule());
					sb.append(",columns:[");
					int i = 0;
					for (String col : fk.getColumnNames()) {
						if (i > 0) {
							sb.append(",");
						}
						sb.append(col);
						i++;
					}
					sb.append("],refcolumns:[");
					i = 0;
					for (String col : fk.getCol2RefMapping()) {
						if (i > 0) {
							sb.append(",");
						}
						sb.append(col);
						i++;
					}
					sb.append("]}");
				}
				sb.append("],\n").append(level3).append("indexes:[");
				int iidx = 0;
				for (Index idx : table.getIndexes()) {
					if (iidx > 0) {
						sb.append(",");
					}
					iidx++;
					sb.append("\n");
					sb.append(level4).append("{name:").append(idx.getName());
					sb.append(",type:").append(idx.getIndexType());
					sb.append(",reverse:").append(idx.isReverse());
					sb.append(",unique:").append(idx.isUnique());
					sb.append(",columns:[");
					int i = 0;
					for (String col : idx.getColumnNames()) {
						if (i > 0) {
							sb.append(",");
						}
						sb.append(col);
						i++;
					}
					sb.append("],rules:[");
					i = 0;
					for (String col : idx.getColumnOrderRulesString()) {
						if (i > 0) {
							sb.append(",");
						}
						sb.append(col);
						i++;
					}
					sb.append("]}");
				}
				sb.append("]}");
			}
			sb.append("]");
			sb.append(",\n");
			sb.append(level2).append("views:[");
			int ivw = 0;
			for (View vw : schema.getViews()) {
				if (ivw > 0) {
					sb.append(",");
				}
				ivw++;
				sb.append("\n").append(level3);
				sb.append("{name:").append(vw.getName());
				sb.append(",sql:").append(vw.getDDL()).append("}");
			}
			sb.append("],\n");
			sb.append(level2).append("serials:[");
			int iseq = 0;
			for (Sequence seq : schema.getSequenceList()) {
				if (iseq > 0) {
					sb.append(",");
				}
				iseq++;
				sb.append("\n").append(level3);
				sb.append("{name:").append(seq.getName());
				sb.append(",current:").append(seq.getCurrentValue());
				sb.append(",max:").append(seq.getMaxValue());
				sb.append(",min:").append(seq.getMinValue());
				sb.append(",inc:").append(seq.getIncrementBy());
				sb.append(",cache:").append(seq.getCacheSize());
				sb.append("}");
			}
			sb.append("],\n");
			sb.append(level2).append("procedures:[");
			int ipro = 0;
			for (Procedure pro : schema.getProcedures()) {
				if (ipro > 0) {
					sb.append(",");
				}
				ipro++;
				sb.append(pro.getName());
			}
			sb.append("],\n");
			sb.append(level2).append("functions:[");
			int ifun = 0;
			for (Function pro : schema.getFunctions()) {
				if (ifun > 0) {
					sb.append(",");
				}
				ifun++;
				sb.append(pro.getName());
			}
			sb.append("]}");
		}
		sb.append("\n]");
		sb.append("}");
		return sb.toString();
	}

	public static String readStrFromFile(String file) throws IOException {
		InputStream is = TestUtil2.class.getResourceAsStream(file);
		InputStreamReader isr = new InputStreamReader(is);
		try {
			StringBuffer sb = new StringBuffer();
			char[] cbuf = new char[256];
			int len = isr.read(cbuf);
			while (len > 0) {
				sb.append(cbuf, 0, len);
				len = isr.read(cbuf);
			}
			return sb.toString();
		} finally {
			isr.close();
		}
	}

	public static int compareStr(String s1, String s2) {
		for (int i = 0; i < s1.length(); i++) {
			if (s1.charAt(i) != s2.charAt(i)) {
				return i;
			}
		}
		return -1;
	}
}
