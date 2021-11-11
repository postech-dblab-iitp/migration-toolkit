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
package com.cubrid.cubridmigration.ui.history.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.StructuredSelection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cubrid.common.ui.swt.EditorPartProviderMock;
import com.cubrid.common.ui.swt.ProgressMonitorDialogRunnerMock;
import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;
import com.cubrid.cubridmigration.ui.BaseUITestCase;
import com.cubrid.cubridmigration.ui.history.MigrationReporter;

/**
 * @author Kevin Cao
 * 
 */
public class MigrationHistoryUIControllerTest extends
		BaseUITestCase {

	private MigrationHistoryUIController controller = new MigrationHistoryUIController() {

		/**
		 * Mock for test case running
		 */
		protected String getReportEditorPartID(final MigrationBriefReport mbr) {
			return "";
		}
	};

	{
		controller.setEditorPartProvider(new EditorPartProviderMock());
		controller.setProgressMonitorDialogRunner(new ProgressMonitorDialogRunnerMock());
	}

	@Before
	public void before() {
		try {
			CUBRIDIOUtils.copyFolder(new File(testReportFilesDir),
					new File(PathUtils.getReportDir()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@After
	public void after() {
		List<MigrationBriefReport> allLocalHistory = controller.getAllLocalHistory();
		IStructuredSelection itemSelection = new StructuredSelection(allLocalHistory);
		controller.deleteSelectedHistory(itemSelection);
		PathUtils.clearTempDir();
	}

	@Test
	public void test_getAllLocalHistory() {
		List<MigrationBriefReport> allLocalHistory = controller.getAllLocalHistory();
		IStructuredSelection itemSelection = new StructuredSelection(allLocalHistory.get(0));
		assertFalse(itemSelection.isEmpty());
	}

	@Test
	public void test_deleteSelectedHistory_single() {
		List<MigrationBriefReport> allLocalHistory = controller.getAllLocalHistory();
		int size = allLocalHistory.size();
		IStructuredSelection itemSelection = new StructuredSelection(allLocalHistory.get(0));
		controller.deleteSelectedHistory(itemSelection);
		allLocalHistory = controller.getAllLocalHistory();
		assertEquals(size - 1, allLocalHistory.size());
	}

	@Test
	public void test_deleteSelectedHistory_multi() {
		List<MigrationBriefReport> allLocalHistory = controller.getAllLocalHistory();
		IStructuredSelection itemSelection = new StructuredSelection(allLocalHistory);
		controller.deleteSelectedHistory(itemSelection);
		allLocalHistory = controller.getAllLocalHistory();
		assertTrue(allLocalHistory.isEmpty());
	}

	@Test
	public void test_importHistory() {
		List<MigrationBriefReport> allLocalHistory = controller.getAllLocalHistory();
		IStructuredSelection itemSelection = new StructuredSelection(allLocalHistory);
		controller.deleteSelectedHistory(itemSelection);

		MigrationBriefReport mbr = (MigrationBriefReport) itemSelection.getFirstElement();
		controller.importHistory(testReportFilesDir + new File(mbr.getHistoryFile()).getName());
		allLocalHistory = controller.getAllLocalHistory();
		assertFalse(allLocalHistory.isEmpty());

	}

	@Test
	public void test_loadReporterInProgress() {
		List<MigrationBriefReport> allLocalHistory = controller.getAllLocalHistory();
		MigrationBriefReport mbr = allLocalHistory.get(0);
		final MigrationReporter reporter = controller.getMigrationReporterByBrief(mbr);
		controller.loadReporterInProgress(reporter);
		assertNotNull(reporter.getReport());
		//controller.showMigrationReport(itemSelection);
	}

	@Test
	public void test_showMigrationReport() {
		List<MigrationBriefReport> allLocalHistory = controller.getAllLocalHistory();
		IStructuredSelection itemSelection = new StructuredSelection(allLocalHistory);
		controller.showMigrationReport(itemSelection);
		itemSelection = new StructuredSelection();
		controller.showMigrationReport(itemSelection);
	}
}
