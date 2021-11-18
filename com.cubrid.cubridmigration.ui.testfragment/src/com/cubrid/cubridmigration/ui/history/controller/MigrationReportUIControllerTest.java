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

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.ui.BaseUITestCase;
import com.cubrid.cubridmigration.ui.common.TextAppender;
import com.cubrid.cubridmigration.ui.history.MigrationReporter;

/**
 * @author Kevin Cao
 * 
 */
public class MigrationReportUIControllerTest extends
		BaseUITestCase {
	private static final String REPORT_FILE_NAME = "1405584596196.mh";
	String reportFileFullName;

	@Before
	public void startup() {
		try {
			reportFileFullName = PathUtils.getReportDir() + REPORT_FILE_NAME;
			CUBRIDIOUtils.copyFile(new File(testReportFilesDir + REPORT_FILE_NAME), new File(
					reportFileFullName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@After
	public void end() {
		PathUtils.deleteFile(new File(reportFileFullName));
		PathUtils.clearTempDir();
	}

	@Test
	public void test1() throws Exception {
		MigrationReportUIController controller = new MigrationReportUIController();
		String dir = PathUtils.getBaseTempDir();
		MigrationReporter reporter = new MigrationReporter(new File(reportFileFullName));
		reporter.loadMigrationHistory();
		Assert.assertNotNull(reporter.getReport());

		controller.saveReportToDirectory(reporter, dir);
		final StringBuffer sb = new StringBuffer();
		TextAppender textAppender = new TextAppender() {

			public void append(String text) {
				sb.append(text);
			}

		};
		controller.loadLogText(reporter, textAppender);
		Assert.assertTrue(sb.length() > 0);
		sb.delete(0, sb.length() - 1);
		controller.loadNonSupportedObjectText(reporter, textAppender);
		Assert.assertTrue(sb.toString().trim().length() == 0);
		Assert.assertFalse(controller.isFileOutputMigration(reporter.getReport()));
	}
}
