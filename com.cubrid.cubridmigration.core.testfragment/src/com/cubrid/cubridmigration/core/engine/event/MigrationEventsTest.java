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
package com.cubrid.cubridmigration.core.engine.event;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;

public class MigrationEventsTest {

	private Table table = new Table();

	{
		table.setName("test");
		Column column = new Column();
		column.setName("f1");
		column.setDataType("integer");
		table.addColumn(column);
		column = new Column();
		column.setName("f2");
		column.setDataType("integer");
		table.addColumn(column);
	}

	@Test
	public void testCreateObjectFailEvent() {
		CreateObjectEvent event = new CreateObjectEvent(table,
				new RuntimeException("error"));
		Assert.assertNotNull(event.getError());
		event.toString();

		event = new CreateObjectEvent(table);
		Assert.assertNull(event.getError());
		event.toString();
	}

	@Test
	public void testCreateObjectStartEvent() {
		CreateObjectStartEvent event = new CreateObjectStartEvent(table);
		event.toString();
	}

	@Test
	public void testSingleRecordErrorEvent() {
		Record record = new Record();

		SingleRecordErrorEvent event;
		for (Column col : table.getColumns()) {
			record.addColumnValue(col, "1");
		}
		event = new SingleRecordErrorEvent(record, new RuntimeException(
				"exception"));
		event.toString();
	}

	@Test
	public void testMigrationErrorEvent() {
		MigrationErrorEvent event = new MigrationErrorEvent(
				new RuntimeException("exception"));
		Assert.assertTrue(event.isFatalError());
		event.getError();
		event.toString();

	}

	@Test
	public void testExportRecordsEvent() {
		SourceTableConfig sourceTable = new SourceTableConfig();
		sourceTable.setName("test");
		ExportRecordsEvent event = new ExportRecordsEvent(sourceTable, 0);
		System.out.println(event.toString());
		Assert.assertNotNull(event.getSourceTable());
		event = new ExportRecordsEvent(sourceTable, 100);
		System.out.println(event.toString());
		Assert.assertEquals(100, event.getRecordCount());
	}

	@Test
	public void testCreateObjectEvent() {
		Trigger obj = new Trigger();
		obj.setName("testtrigger");
		CreateObjectEvent event = new CreateObjectEvent(obj);
		Assert.assertEquals("Create trigger[testtrigger] successfully.", event.toString());

		Function fun = new Function();
		fun.setName("testfunction");
		event = new CreateObjectEvent(fun);
		Assert.assertEquals("Create function[testfunction] successfully.", event.toString());

		Procedure pro = new Procedure();
		pro.setName("testprocedure");
		event = new CreateObjectEvent(pro);
		Assert.assertEquals("Create procedure[testprocedure] successfully.", event.toString());

		Sequence seq = new Sequence();
		seq.setName("testsequence");
		event = new CreateObjectEvent(seq);
		Assert.assertEquals("Create sequence[testsequence] successfully.", event.toString());
	}

	@Test
	public void testImportRecordsEvent() {
		SourceTableConfig sourceTable = new SourceTableConfig();
		sourceTable.setName("test");
		sourceTable.setTarget("target");
		ImportRecordsEvent event = new ImportRecordsEvent(sourceTable, 100);
		Assert.assertTrue(event.toString().length() > 0);
		event = new ImportRecordsEvent(sourceTable, 100,
				new NormalMigrationException("test error"), null);
		Assert.assertTrue(event.toString().length() > 0);
		event = new ImportRecordsEvent(sourceTable, 0);
		Assert.assertTrue(event.toString().length() > 0);
	}

	@Test
	public void testMigrationNoSupportEvent() {
		MigrationNoSupportEvent event = new MigrationNoSupportEvent(null);
		Assert.assertEquals("NULL", event.toString());
		event = new MigrationNoSupportEvent(new Table());
		Assert.assertTrue(event.toString().length() > 0);
	}

	@Test
	public void testMigrationFinishedEvent() {
		MigrationFinishedEvent event = new MigrationFinishedEvent(true);
		Assert.assertTrue(event.toString().length() > 0);
		event = new MigrationFinishedEvent(false);
		Assert.assertTrue(event.toString().length() > 0);
	}
}
