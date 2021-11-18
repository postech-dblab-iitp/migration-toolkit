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
package com.cubrid.cubridmigration.ui.history.tableviewer;

import org.eclipse.swt.SWT;
import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;

/**
 * 
 * @author Kevin Cao
 * 
 */
public class MigrationHistoryBriefComparatorTest {

	@Test
	public void test_compare() {
		MigrationHistoryBriefComparator comparator = new MigrationHistoryBriefComparator();
		MigrationBriefReport mbf1 = new MigrationBriefReport();
		mbf1.setScriptName("test1");
		mbf1.setStartTime(System.currentTimeMillis() - 1000);
		MigrationBriefReport mbf2 = new MigrationBriefReport();
		mbf2.setScriptName("test2");
		mbf2.setStartTime(System.currentTimeMillis() - 500);

		comparator.setColumnIndex(0);
		comparator.setSortMode(SWT.UP);
		Assert.assertTrue(comparator.compare(mbf1, mbf2) < 0);
		Assert.assertTrue(comparator.compare(mbf2, mbf1) > 0);

		comparator.setColumnIndex(0);
		comparator.setSortMode(SWT.DOWN);
		Assert.assertTrue(comparator.compare(mbf1, mbf2) > 0);
		Assert.assertTrue(comparator.compare(mbf2, mbf1) < 0);

		comparator.setColumnIndex(1);
		comparator.setSortMode(SWT.UP);
		Assert.assertTrue(comparator.compare(mbf1, mbf2) < 0);
		Assert.assertTrue(comparator.compare(mbf2, mbf1) > 0);

		comparator.setColumnIndex(1);
		comparator.setSortMode(SWT.DOWN);
		Assert.assertTrue(comparator.compare(mbf1, mbf2) > 0);
		Assert.assertTrue(comparator.compare(mbf2, mbf1) < 0);

		mbf1.setStartTime(mbf2.getStartTime());
		Assert.assertTrue(comparator.compare(mbf1, mbf2) == 0);

		comparator.setColumnIndex(2);
		comparator.setSortMode(SWT.UP);
		Assert.assertTrue(comparator.compare(mbf1, mbf2) == 0);
	}
}
