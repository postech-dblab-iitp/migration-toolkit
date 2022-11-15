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
package com.cubrid.cubridmigration.core.engine.report;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.event.ExportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.mysql.meta.MySQLSchemaFetcher;

public class MigrationReportTest {

	@Test
	public void testMigrationReport() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getMySQLConfig();
		//get schema of source DB.
		MySQLSchemaFetcher builder = new MySQLSchemaFetcher();
		Catalog cl = builder.buildCatalog(config.getSourceConParams().createConnection(),
				config.getSourceConParams(), null);
		config.setSrcCatalog(cl, false);
		MigrationReport report = new MigrationReport();
		report.setBrief(new MigrationBriefReport());
		report.initReport(config, true);

		report.setConfigSummary("test config summary");
		Assert.assertNotNull(report.getConfigSummary());

		report.setTotalStartTime(System.currentTimeMillis());
		report.getTotalStartTime();
		Thread.sleep(2000);
		report.setTotalEndTime(System.currentTimeMillis());
		report.getTotalEndTime();
		Assert.assertTrue(report.getTotalEndTime() > report.getTotalStartTime());

		ExportRecordsEvent event = new ExportRecordsEvent(config.getExpEntryTableCfg(null, "game"),
				100);
		report.addExpMigRecResult(event);
		Assert.assertNotNull(report.getRecMigResults(null, "game", "game"));

		ImportRecordsEvent ire = new ImportRecordsEvent(config.getExpEntryTableCfg(null, "game"),
				100);
		report.addImpMigRecResult(ire);
		Assert.assertNotNull(report.getDBObjResult(config.getSrcTableSchema(null, "game")));

		List<DBObjMigrationResult> values = report.getDbObjectsResult();
		report.setDbObjectsResult(values);
		report.setRecMigResults(report.getRecMigResults());

		Assert.assertNotNull(report.getOverviewResults());

		//Last test
		report.setRecMigResults(null);
		Assert.assertTrue(report.getRecMigResults().isEmpty());
		report.setDbObjectsResult(null);
		Assert.assertTrue(report.getDbObjectsResult().isEmpty());
	}
}
