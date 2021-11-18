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
package com.cubrid.cubridmigration.ui.wizard.editor.controller;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.common.ui.swt.ProgressMonitorDialogRunnerMock;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.CreateObjectEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportSQLEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportSQLsEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.ui.BaseUITestCase;

/**
 * @author Kevin Cao
 * 
 */
public class MigrationProgressUIControllerTest extends
		BaseUITestCase {

	@Test
	public void testTableViewerUpdateStatusInMigration() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_CUBRID2CUBRIDConfig();
		MigrationProgressUIController controller = new MigrationProgressUIController();
		controller.setConfig(config);
		controller.setProgressMonitorDialogRunner(new ProgressMonitorDialogRunnerMock());
		controller.setReportEditorPartId("test");

		//To be tested
		controller.updateTableRowCount();
		String[][] tvInput = controller.getProgressTableInput();
		Assert.assertNotNull(tvInput);
		Assert.assertTrue(tvInput.length > 0);

		Table codeTable = config.getSrcTableSchema(null, "code");
		String[] item = controller.updateTableExpData(codeTable.getName(),
				codeTable.getTableRowCount());
		Assert.assertEquals(codeTable.getName(), item[0]);
		Assert.assertEquals(String.valueOf(codeTable.getTableRowCount()), item[1]);
		Assert.assertEquals(String.valueOf(codeTable.getTableRowCount()), item[2]);
		Assert.assertEquals("0", item[3]);
		Assert.assertEquals("50%", item[4]);

		item = controller.updateTableImpData(codeTable.getName(), codeTable.getTableRowCount());
		Assert.assertEquals(codeTable.getName(), item[0]);
		Assert.assertEquals(String.valueOf(codeTable.getTableRowCount()), item[1]);
		Assert.assertEquals(String.valueOf(codeTable.getTableRowCount()), item[2]);
		Assert.assertEquals(String.valueOf(codeTable.getTableRowCount()), item[3]);
		Assert.assertEquals("100%", item[4]);
	}

	@Test
	public void test_UpdateMigrationStatusWithTrueImplicitEstimate() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_CUBRID2CUBRIDConfig();
		MigrationProgressUIController controller = new MigrationProgressUIController();
		controller.setConfig(config);
		controller.setProgressMonitorDialogRunner(new ProgressMonitorDialogRunnerMock());
		controller.setReportEditorPartId("test");

		config.setImplicitEstimate(true);
		//To be tested
		controller.updateTableRowCount();
		String[][] tvInput = controller.getProgressTableInput();
		Assert.assertNotNull(tvInput);
		Assert.assertTrue(tvInput.length > 0);

		Table codeTable = config.getSrcTableSchema(null, "code");
		long tableRowCount = 10;
		String[] item = controller.updateTableExpData(codeTable.getName(), 0);
		Assert.assertTrue(item.length == 0);
		item = controller.updateTableExpData(codeTable.getName(), tableRowCount);
		Assert.assertEquals(codeTable.getName(), item[0]);
		Assert.assertEquals(MigrationProgressUIController.NA_STRING, item[1]);
		Assert.assertEquals(String.valueOf(tableRowCount), item[2]);
		Assert.assertEquals(MigrationProgressUIController.NA_STRING, item[3]);
		Assert.assertEquals(MigrationProgressUIController.NA_STRING, item[4]);

		item = controller.updateTableImpData(codeTable.getName(), tableRowCount);
		Assert.assertEquals(codeTable.getName(), item[0]);
		Assert.assertEquals(MigrationProgressUIController.NA_STRING, item[1]);
		Assert.assertEquals(String.valueOf(tableRowCount), item[2]);
		Assert.assertEquals(String.valueOf(tableRowCount), item[3]);
		Assert.assertEquals(MigrationProgressUIController.NA_STRING, item[4]);
	}

	@Test
	public void test_MigrationIsRunning() throws Exception {
		MigrationProgressUIController controller = new MigrationProgressUIController();
		controller.setProgressMonitorDialogRunner(new ProgressMonitorDialogRunnerMock());
		controller.setReportEditorPartId("test");
		Assert.assertFalse(controller.isMigrationRunning());
	}

	@Test
	public void test_EventJudgement() throws Exception {
		MigrationProgressUIController controller = new MigrationProgressUIController();
		MigrationEvent event = new MigrationErrorEvent(null);

		event = new CreateObjectEvent(null); //Create an CreateObjectEvent with success flag
		Assert.assertFalse(controller.ifEventHasError(event));
		Assert.assertFalse(controller.ifShouldUpdateExportStatus(event));
		Assert.assertFalse(controller.ifShouldUpdateImportStatus(event));

		event = new CreateObjectEvent(null, null); //Create an CreateObjectEvent with failed flag
		Assert.assertTrue(controller.ifEventHasError(event));
		Assert.assertFalse(controller.ifShouldUpdateExportStatus(event));
		Assert.assertFalse(controller.ifShouldUpdateImportStatus(event));

		event = new MigrationErrorEvent(null);
		Assert.assertTrue(controller.ifEventHasError(event));
		Assert.assertFalse(controller.ifShouldUpdateExportStatus(event));
		Assert.assertFalse(controller.ifShouldUpdateImportStatus(event));

		event = new ImportRecordsEvent(new SourceTableConfig(), 10);
		Assert.assertFalse(controller.ifEventHasError(event));
		Assert.assertFalse(controller.ifShouldUpdateExportStatus(event));
		Assert.assertTrue(controller.ifShouldUpdateImportStatus(event));

		event = new ImportRecordsEvent(new SourceTableConfig(), 10, new RuntimeException("test"),
				"");
		Assert.assertTrue(controller.ifEventHasError(event));
		Assert.assertFalse(controller.ifShouldUpdateExportStatus(event));
		Assert.assertFalse(controller.ifShouldUpdateImportStatus(event));

		event = new ExportRecordsEvent(new SourceTableConfig(), 1);
		Assert.assertFalse(controller.ifEventHasError(event));
		Assert.assertTrue(controller.ifShouldUpdateExportStatus(event));
		Assert.assertFalse(controller.ifShouldUpdateImportStatus(event));

		event = new ExportCSVEvent(new SourceCSVConfig(), 1);
		Assert.assertFalse(controller.ifEventHasError(event));
		Assert.assertTrue(controller.ifShouldUpdateExportStatus(event));
		Assert.assertFalse(controller.ifShouldUpdateImportStatus(event));

		event = new ExportSQLEvent("", 1);
		Assert.assertFalse(controller.ifEventHasError(event));
		Assert.assertTrue(controller.ifShouldUpdateExportStatus(event));
		Assert.assertFalse(controller.ifShouldUpdateImportStatus(event));

		event = new ImportCSVEvent(new SourceCSVConfig(), 1, 1);
		Assert.assertFalse(controller.ifEventHasError(event));
		Assert.assertFalse(controller.ifShouldUpdateExportStatus(event));
		Assert.assertTrue(controller.ifShouldUpdateImportStatus(event));

		event = new ImportCSVEvent(new SourceCSVConfig(), 1, 1, new RuntimeException(), "");
		Assert.assertTrue(controller.ifEventHasError(event));
		Assert.assertFalse(controller.ifShouldUpdateExportStatus(event));
		Assert.assertFalse(controller.ifShouldUpdateImportStatus(event));

		event = new ImportSQLsEvent("", 1, 1);
		Assert.assertFalse(controller.ifEventHasError(event));
		Assert.assertFalse(controller.ifShouldUpdateExportStatus(event));
		Assert.assertTrue(controller.ifShouldUpdateImportStatus(event));

		event = new ImportSQLsEvent("", 1, 1, new RuntimeException(), "");
		Assert.assertTrue(controller.ifEventHasError(event));
		Assert.assertFalse(controller.ifShouldUpdateExportStatus(event));
		Assert.assertFalse(controller.ifShouldUpdateImportStatus(event));
	}
}
