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
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.event.ImportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportSQLsEvent;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.ui.BaseUITestCase;

/**
 * @author Kevin Cao
 * 
 */
public class FileMigrationProgressUIControllerTest extends
		BaseUITestCase {

	@Test
	public void testSQL() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_LF_SQL2CUBRIDConfig();
		FileMigrationProgressUIController controller = new FileMigrationProgressUIController();
		controller.setConfig(config);
		controller.setProgressMonitorDialogRunner(new ProgressMonitorDialogRunnerMock());
		controller.setReportEditorPartId("test");

		Assert.assertEquals(100, controller.getTotalProgress());
		//To be tested
		controller.calculateTotalFileSize();
		String[][] tvInput = controller.getProgressTableInput();
		Assert.assertNotNull(tvInput);
		Assert.assertTrue(tvInput.length > 0);

		String sqlFile = config.getSqlFiles().get(0);

		int progressBarProgressValue = controller.getProgressBarProgressValue(new ImportSQLsEvent(
				sqlFile, 10, 10000));
		while (progressBarProgressValue <= 0) {
			progressBarProgressValue = controller.getProgressBarProgressValue(new ImportSQLsEvent(
					sqlFile, 10, 10000));
		}
		System.out.println(progressBarProgressValue);
		Assert.assertTrue(progressBarProgressValue > 0);

		String[] item = controller.updateTableExpData(sqlFile, 100);
		Assert.assertEquals(sqlFile, item[0]);
		Assert.assertEquals("100", item[1]);
		Assert.assertEquals("0", item[2]);

		item = controller.updateTableImpData(sqlFile, 100);
		Assert.assertEquals(3, item.length);
		Assert.assertEquals(sqlFile, item[0]);
		Assert.assertEquals("100", item[1]);
		Assert.assertEquals("100", item[2]);
	}

	@Test
	public void testCSV() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_LF_CSV2CUBRIDConfig();
		FileMigrationProgressUIController controller = new FileMigrationProgressUIController();
		controller.setConfig(config);
		controller.setProgressMonitorDialogRunner(new ProgressMonitorDialogRunnerMock());
		controller.setReportEditorPartId("test");

		//To be tested
		controller.calculateTotalFileSize();
		String[][] tvInput = controller.getProgressTableInput();
		Assert.assertNotNull(tvInput);
		Assert.assertTrue(tvInput.length > 0);

		SourceCSVConfig scc = config.getCSVConfigs().get(0);
		int progressBarProgressValue = controller.getProgressBarProgressValue(new ImportCSVEvent(
				scc, 10, 10000));
		while (progressBarProgressValue <= 0) {
			progressBarProgressValue = controller.getProgressBarProgressValue(new ImportCSVEvent(
					scc, 10, 10000));
		}
		Assert.assertTrue(progressBarProgressValue > 0);

		String[] item = controller.updateTableExpData(scc.getName(), 100);
		Assert.assertEquals(scc.getName(), item[0]);
		Assert.assertEquals("100", item[1]);
		Assert.assertEquals("0", item[2]);

		item = controller.updateTableImpData(scc.getName(), 100);
		Assert.assertEquals(3, item.length);
		Assert.assertEquals(scc.getName(), item[0]);
		Assert.assertEquals("100", item[1]);
		Assert.assertEquals("100", item[2]);

		//		controller.addMessage2Text(txtProgress, eventDate, msg, isError);
		//		controller.createMigrationReporter(startMode);
		//		controller.openMigrationReport(oldEditorPart);
		//		controller.setProgressMonitorDialogRunner(progressMonitorDialogRunner);
		//		controller.startMigration(monitor, startMode);
		//		controller.updateTableExpData(tableName, exp);
		//		controller.updateTableImpData(tableName, imp);

		//		controller.stopMigrationNow();
		//		controller.canBeClosed();
	}
}
