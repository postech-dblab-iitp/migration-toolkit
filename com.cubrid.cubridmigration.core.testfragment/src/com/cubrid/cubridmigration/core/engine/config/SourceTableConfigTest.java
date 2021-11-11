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

public class SourceTableConfigTest {

	@Test
	public void testSourceTableConfig() {
		SourceTableConfig stc = new SourceTableConfig();
		stc.addColumnConfig("f1", "f1", false);
		stc.addColumnConfig("f2", "f2", false);
		stc.addColumnConfig("f3", "f3", false);
		stc.setCreateNewTable(true);

		stc.removeColumnConfig("f1");
		Assert.assertEquals(2, stc.getColumnConfigList().size());
		stc.removeColumnConfig("f");
		Assert.assertEquals(2, stc.getColumnConfigList().size());
		stc.removeColumnConfig(null);
		Assert.assertEquals(2, stc.getColumnConfigList().size());
		stc.clearColumnList();
		Assert.assertEquals(0, stc.getColumnConfigList().size());

		stc.setCreateNewTable(false);
		stc.addColumnConfig("f1", "f1", false);
		stc.addColumnConfig("f2", "f2", false);
		stc.addColumnConfig("f3", "f3", false);
		stc.setMigrateData(true);
		stc.clearColumnList();
		stc.addColumnConfig("f1", "f1", true);
		stc.addColumnConfig("f2", "f2", true);
		stc.addColumnConfig("f3", "f3", true);
		stc.addColumnConfig("f3", "f4", false);
		stc.setMigrateData(false);
		SourceColumnConfig scc = stc.getColumnConfig("f3");
		Assert.assertNotNull(scc);
	}
}
