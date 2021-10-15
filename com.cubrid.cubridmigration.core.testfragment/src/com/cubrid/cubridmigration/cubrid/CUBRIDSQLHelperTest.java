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
package com.cubrid.cubridmigration.cubrid;

import java.math.BigInteger;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfoTest;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.cubrid.CUBRIDSQLHelper;

public class CUBRIDSQLHelperTest {
	CUBRIDSQLHelper ddl = CUBRIDSQLHelper.getInstance(null);

	@Test
	public void testCUBRIDDDLUtil() {
		Table table = new Table();
		table.setName("test");
		Column col1 = new Column();
		col1.setName("f1");
		col1.setDataType("int");
		col1.setShownDataType("int");
		col1.setAutoIncIncrVal(1);
		col1.setAutoIncrement(true);
		col1.setAutoIncSeedVal(1);
		table.addColumn(col1);

		Column col2 = new Column();
		col2.setName("f2");
		col2.setDataType("int");
		col2.setShownDataType("int");
		col2.setAutoIncIncrVal(1);
		col2.setAutoIncrement(true);
		col2.setAutoIncSeedVal(1);
		table.addColumn(col2);

		Column col3 = new Column();
		col3.setName("f3");
		col3.setDataType("int");
		col3.setShownDataType("int");
		col3.setAutoIncIncrVal(1);
		col3.setAutoIncrement(true);
		col3.setAutoIncSeedVal(1);
		table.addColumn(col3);

		Column col4 = new Column();
		col4.setName("f4");
		col4.setDataType("int");
		col4.setShownDataType("int");
		col4.setAutoIncIncrVal(1);
		col4.setAutoIncrement(true);
		col4.setAutoIncSeedVal(1);
		table.addColumn(col4);

		PK pk = new PK(table);
		pk.setName("pk");
		pk.addColumn(col1.getName());
		table.setPk(pk);

		FK fk = new FK(table);
		fk.setReferencedTableName("t2");
		fk.setName("fk");
		fk.addRefColumnName("f3", "f3");
		table.addFK(fk);

		Index idx = new Index(table);
		idx.setName("idx");
		idx.addColumn("idx", true);
		table.addIndex(idx);

		//ddl.getTablePKAndIndexDDL(table);
		//ddl.getTableFKDDL(table);

		idx = new Index(table);
		idx.setName("idx2");
		idx.addColumn("idx2", true);
		ddl.getIndexDDL(table.getName(), idx, "");

		idx = new Index(table);
		idx.setName("idx3");
		idx.addColumn("idx3", false);
		ddl.getIndexDDL(table.getName(), idx, "");

		idx = new Index(table);
		idx.setName("idx4");
		idx.addColumn("idx4", true);
		ddl.getIndexDDL(table.getName(), idx, "");

		idx = new Index(table);
		idx.setName("idx5");
		idx.addColumn("idx5", true);
		ddl.getIndexDDL(table.getName(), idx, "");

		idx = new Index(table);
		idx.setName("idx6");
		idx.addColumn("idx6", true);
		ddl.getIndexDDL(table.getName(), idx, "");

		idx = new Index(table);
		idx.setName("idx7");
		idx.addColumn("idx7", true);
		ddl.getIndexDDL(table.getName(), idx, "");

		final PartitionInfo pti = PartitionInfoTest.newPartitionInfo();
		table.setPartitionInfo(pti);
		System.out.println(ddl.getTablePartitonDDL(table));

		pti.setPartitionFunc("abs");
		pti.setPartitionMethod(PartitionInfo.PARTITION_METHOD_LIST);
		System.out.println(ddl.getTablePartitonDDL(table));
		pti.setPartitionFunc("ABS");
		pti.setPartitionMethod(PartitionInfo.PARTITION_METHOD_RANGE);
		System.out.println(ddl.getTablePartitonDDL(table));

		Sequence sequence = new Sequence();
		sequence.setName("test_sequence");
		sequence.setCurrentValue(new BigInteger("0"));
		sequence.setIncrementBy(new BigInteger("1"));
		sequence.setNoMinValue(true);
		sequence.setNoMaxValue(true);
		sequence.setCycleFlag(true);
		sequence.setCacheSize(0);
		Assert.assertEquals(
				"CREATE SERIAL \"test_sequence\" START WITH 0 INCREMENT BY 1 NOMINVALUE  NOMAXVALUE  CYCLE NOCACHE",
				ddl.getSequenceDDL(sequence));

		sequence.setNoMinValue(false);
		sequence.setNoMaxValue(false);
		sequence.setCycleFlag(false);
		sequence.setCacheSize(100);
		sequence.setMinValue(new BigInteger("0"));
		sequence.setMaxValue(new BigInteger("1000"));
		Assert.assertEquals(
				"CREATE SERIAL \"test_sequence\" START WITH 0 INCREMENT BY 1 MINVALUE 0 MAXVALUE  1000 NOCYCLE CACHE 100",
				ddl.getSequenceDDL(sequence));

	}
}
