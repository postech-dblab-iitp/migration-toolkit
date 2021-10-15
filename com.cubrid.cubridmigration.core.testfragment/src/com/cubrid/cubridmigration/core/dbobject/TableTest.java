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
package com.cubrid.cubridmigration.core.dbobject;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * DbUtilTest
 * 
 * @author JessieHuang
 * @version 1.0 - 2009-9-18
 */
public class TableTest {

	//	@Test
	//	public void testEqualsObject() {
	//		Table table1 = new Table();
	//		Assert.assertTrue(table1.equals(table1));
	//		Assert.assertFalse(table1.equals(12));
	//		Assert.assertFalse(table1.equals(null));
	//		Assert.assertTrue(table1.hashCode() == table1.hashCode());
	//
	//		Table table2 = new Table();
	//		//Assert.assertTrue(table1.equals(table2));
	//		Assert.assertTrue(table1.hashCode() == table2.hashCode());
	//
	//		table1.setName("testTable");
	//		Assert.assertFalse(table2.equals(table1));
	//		table2.setName("testTable");
	//		Assert.assertTrue(table1.equals(table2));
	//		Assert.assertTrue(table1.hashCode() == table2.hashCode());
	//
	//		table2.setName("testTable2");
	//		Assert.assertFalse(table1.equals(table2));
	//		Assert.assertTrue(table1.hashCode() != table2.hashCode());
	//	}

	@Test
	public void testColumn() {
		Table tbl = new Table();
		Column col = new Column();
		col.setName("f1");
		tbl.addColumn(col);
		Assert.assertFalse(tbl.getColumns().isEmpty());

		tbl.removeColumn(col);
		tbl.removeColumn(null);
		Assert.assertTrue(tbl.getColumns().isEmpty());

		FK fk = new FK(tbl);
		fk.setName("fk");

		tbl.removeFK("fkno");

		tbl.addFK(fk);
		tbl.addFK(fk);
		Assert.assertNotNull(tbl.getFKByName("fk"));

		tbl.removeFK("fkno");
		tbl.removeFK("fk");
		Assert.assertNull(tbl.getFKByName("fk"));
	}
}
