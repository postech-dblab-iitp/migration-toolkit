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
package com.cubrid.cubridmigration.core.engine.config;

import junit.framework.Assert;

import org.junit.Test;

public class SourceEntryTableConfigTest {

	@Test
	public void testSourceEntryTableConfig() {
		SourceEntryTableConfig setc = new SourceEntryTableConfig();
		setc.setCreateNewTable(false);
		setc.setMigrateData(false);
		setc.addColumnConfig("f1", "f1", false);
		setc.addColumnConfig("f2", "f2", false);
		setc.addColumnConfig("f3", "f3", false);

		setc.addFKConfig("fk1", "fk1", false);
		setc.addFKConfig("fk2", "fk2", false);

		setc.addIndexConfig("idx1", "idx1", false);
		setc.addIndexConfig("idx2", "idx2", false);
		setc.setCreateNewTable(true);
		Assert.assertTrue(setc.getColumnConfig("f1").isCreate());
		Assert.assertTrue(setc.getFKConfig("fk1").isCreate());
		Assert.assertTrue(setc.getIndexConfig("idx2").isCreate());
		setc.setCreateNewTable(false);
		Assert.assertTrue(setc.getColumnConfig("f1").isCreate());
		Assert.assertTrue(setc.getFKConfig("fk1").isCreate());
		Assert.assertTrue(setc.getIndexConfig("idx2").isCreate());

		setc = new SourceEntryTableConfig();
		setc.setCreateNewTable(false);
		setc.setMigrateData(false);
		setc.addColumnConfig("f1", "f1", false);
		setc.addColumnConfig("f2", "f2", false);
		setc.addColumnConfig("f3", "f3", false);

		setc.addFKConfig("fk1", "fk1", false);
		setc.addFKConfig("fk2", "fk2", false);
		setc.addFKConfig("fk2", "fk2", false);

		setc.addIndexConfig("idx1", "idx1", false);
		setc.addIndexConfig("idx2", "idx2", false);
		setc.addIndexConfig("idx2", "idx2", false);
		setc.setMigrateData(true);
		Assert.assertTrue(setc.getColumnConfig("f1").isCreate());
		Assert.assertFalse(setc.getFKConfig("fk1").isCreate());
		Assert.assertFalse(setc.getIndexConfig("idx2").isCreate());
		setc.setMigrateData(false);
		Assert.assertTrue(setc.getColumnConfig("f1").isCreate());
		Assert.assertFalse(setc.getFKConfig("fk1").isCreate());
		Assert.assertFalse(setc.getIndexConfig("idx2").isCreate());

		setc.setCondition("");
		Assert.assertNotNull(setc.getCondition());
		setc.setCondition(null);
		Assert.assertNotNull(setc.getCondition());
	}
}
