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
package com.cubrid.cubridmigration.core.engine.datatype.handler;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;

import org.junit.Test;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.cubrid.stmt.handler.BitHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.BlobHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.ClobHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.DateHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.DateTimeHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.DoubleHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.FloatHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.SetterHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.TimestampHandler;
import com.cubrid.cubridmigration.cubrid.stmt.handler.VarcharHandler;

public class DataTypeHandlerTest {

	private MigrationConfiguration config;
	{
		try {
			config = TemplateParserTest.getMySQLConfig();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testTimestampHandler() throws Exception {
		SetterHandler handler = new TimestampHandler();
		Connection con = config.getTargetConParams().createConnection();
		Table tt = config.getTargetTableSchema("test_time");
		Column col = tt.getColumnByName("f5");
		ColumnValue cv = new ColumnValue(col, new Timestamp(
				System.currentTimeMillis()));
		PreparedStatement stmt = con.prepareStatement("insert into test_time (f5) values(?)");
		handler.handle(stmt, 0, cv);

		cv = new ColumnValue(col, "");
		handler.handle(stmt, 0, cv);
		con.close();
	}

	@Test
	public void testBlobHandler() throws Exception {
		SetterHandler handler = new BlobHandler();
		Connection con = config.getTargetConParams().createConnection();
		try {
			Table tt = config.getTargetTableSchema("test_binary");
			Column col = tt.getColumnByName("f2");
			ColumnValue cv = new ColumnValue(col, new byte[]{1 });
			handleTable(con, "create table test_binary (f2 blob);");
			PreparedStatement stmt = con.prepareStatement("insert into test_binary (f2) values(?)");
			handler.handle(stmt, 0, cv);

			cv = new ColumnValue(col, "");
			handler.handle(stmt, 0, cv);
			stmt.execute("drop table test_binary;");
			stmt.close();
			handleTable(con, "drop table test_binary ;");
		} finally {
			con.close();
		}

	}

	@Test
	public void testDoubleHandler() throws Exception {
		SetterHandler handler = new DoubleHandler();
		Connection con = config.getTargetConParams().createConnection();
		Table tt = config.getTargetTableSchema("test_number");
		Column col = tt.getColumnByName("f7");
		ColumnValue cv = new ColumnValue(col, new Double("12.22"));
		PreparedStatement stmt = con.prepareStatement("insert into test_number (f7) values(?)");
		handler.handle(stmt, 0, cv);

		cv = new ColumnValue(col, "");
		handler.handle(stmt, 0, cv);
		con.close();
	}

	@Test
	public void testFloatHandler() throws Exception {
		SetterHandler handler = new FloatHandler();
		Connection con = config.getTargetConParams().createConnection();
		Table tt = config.getTargetTableSchema("test_number");
		Column col = tt.getColumnByName("f6");
		ColumnValue cv = new ColumnValue(col, new Float("12.22"));
		PreparedStatement stmt = con.prepareStatement("insert into test_number (f6) values(?)");
		handler.handle(stmt, 0, cv);

		cv = new ColumnValue(col, "");
		handler.handle(stmt, 0, cv);
		con.close();
	}

	@Test
	public void testVarcharHandler() throws Exception {
		SetterHandler handler = new VarcharHandler();
		Connection con = config.getTargetConParams().createConnection();
		Table tt = config.getTargetTableSchema("test_string");
		Column col = tt.getColumnByName("f5");
		ColumnValue cv = new ColumnValue(col, "12.22");
		PreparedStatement stmt = con.prepareStatement("insert into test_string (f5) values(?)");
		handler.handle(stmt, 0, cv);
	}

	//	@Test
	//	public void testSetHandler() throws Exception {
	//		SetterHandler handler = new SetHandler();
	//		Connection con = config.getTargetConParams().createConnection();
	//		Table tt = config.getTargetTableSchema("test_string");
	//		Column col = tt.getColumnByName("f5");
	//		ColumnValue cv = new ColumnValue(col, new Integer[]{1, 2, 3 });
	//		PreparedStatement stmt = con.prepareStatement("insert into test_binary (f5) values(?)");
	//		handler.handle(stmt, 0, cv);
	//
	//		cv = new ColumnValue(col, "");
	//		handler.handle(stmt, 0, cv);
	//		con.close();
	//	}

	@Test
	public void testClobHandler() throws Exception {
		SetterHandler handler = new ClobHandler("utf8", "utf8");
		Connection con = config.getTargetConParams().createConnection();
		Table tt = config.getTargetTableSchema("test_string");
		Column col = tt.getColumnByName("f8");
		ColumnValue cv = new ColumnValue(col, "12.22");
		PreparedStatement stmt = con.prepareStatement("insert into test_string (f8) values(?)");
		handler.handle(stmt, 0, cv);

		cv = new ColumnValue(col, "");
		handler.handle(stmt, 0, cv);
		con.close();
	}

	@Test
	public void testDateHandler() throws Exception {
		SetterHandler handler = new DateHandler();
		Connection con = config.getTargetConParams().createConnection();
		Table tt = config.getTargetTableSchema("test_time");
		Column col = tt.getColumnByName("f1");
		ColumnValue cv = new ColumnValue(col, new Date(
				System.currentTimeMillis()));
		PreparedStatement stmt = con.prepareStatement("insert into test_time (f1) values(?)");
		handler.handle(stmt, 0, cv);

		cv = new ColumnValue(col, "");
		handler.handle(stmt, 0, cv);
		con.close();
	}

	@Test
	public void testBitHandler() throws Exception {
		SetterHandler handler = new BitHandler();
		Connection con = config.getTargetConParams().createConnection();
		handleTable(con, "create table test_binary(f7 bit(128));");
		Table tt = config.getTargetTableSchema("test_binary");
		Column col = tt.getColumnByName("f7");
		ColumnValue cv = new ColumnValue(col, new byte[]{1, 2, 3 });
		PreparedStatement stmt = con.prepareStatement("insert into test_binary (f7) values(?)");
		handler.handle(stmt, 0, cv);

		cv = new ColumnValue(col, "");
		handler.handle(stmt, 0, cv);

		cv = new ColumnValue(col, con.createBlob());
		handler.handle(stmt, 0, cv);

		cv = new ColumnValue(col, con.createBlob().getBinaryStream());
		handler.handle(stmt, 0, cv);

		handleTable(con, "drop table test_binary;");
		con.close();
	}

	@Test
	public void testDateTimeHandler() throws Exception {
		SetterHandler handler = new DateTimeHandler();
		Connection con = config.getTargetConParams().createConnection();
		Table tt = config.getTargetTableSchema("test_time");
		Column col = tt.getColumnByName("f4");
		ColumnValue cv = new ColumnValue(col, new Timestamp(
				System.currentTimeMillis()));
		PreparedStatement stmt = con.prepareStatement("insert into test_time (f4) values(?)");
		handler.handle(stmt, 0, cv);

		cv = new ColumnValue(col, "");
		handler.handle(stmt, 0, cv);
		con.close();
	}

	//	@Test
	//	public void testNVarcharHandler() throws Exception {
	//		SetterHandler handler = new NVarcharHandler();
	//		Connection con = config.getTargetConParams().createConnection();
	//		TargetTable tt = config.getTargetTable("test_time");
	//		Column col = tt.getColumnByName("f4");
	//		ColumnValue cv = new ColumnValue(col, new Date());
	//		PreparedStatement stmt = con.prepareStatement("insert into test_binary (f4) values(?)");
	//		handler.handle(stmt, 0, cv);
	//		con.close();
	//	}

	private void handleTable(Connection con, String tableDDL) {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.execute(tableDDL);
		} catch (Exception ex) {
			Closer.close(stmt);
		}
	}
}
