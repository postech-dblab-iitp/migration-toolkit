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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class PartitionInfoTest {

	private static final String HASH = PartitionInfo.PARTITION_METHOD_HASH;

	@Test
	public void testPartitionInfo() throws CloneNotSupportedException {
		PartitionInfo cloneObj = newPartitionInfo();

		Assert.assertEquals(1, cloneObj.getPartitionColumnCount());
		Assert.assertFalse(cloneObj.getPartitionColumns().isEmpty());
		Assert.assertEquals(4, cloneObj.getPartitionCount());
		Assert.assertNotNull(cloneObj.getDDL());
		Assert.assertNotNull(cloneObj.getPartitionExp());
		Assert.assertNotNull(cloneObj.getPartitionFunc());
		Assert.assertNotNull(cloneObj.getPartitionMethod());
		Assert.assertNotNull(cloneObj.getPartitions());
		Assert.assertNotNull(cloneObj.getPartitionTableByName("part_0"));
		Assert.assertNull(cloneObj.getPartitionTableByName("nopart"));
		Assert.assertNotNull(cloneObj.getPartitionTableByPosition(0));
		Assert.assertNull(cloneObj.getPartitionTableByPosition(100));
		Assert.assertNotNull(cloneObj.getSubPartitionColumnCount());
		Assert.assertNotNull(cloneObj.getSubPartitionColumns());
		Assert.assertNotNull(cloneObj.getSubPartitionCount());
		Assert.assertNotNull(cloneObj.getSubPartitionExp());
		Assert.assertNotNull(cloneObj.getSubPartitionFunc());
		Assert.assertNotNull(cloneObj.getSubPartitionMethod());
		Assert.assertNotNull(cloneObj.getSubPartitionNameByIdx(0));
		Assert.assertNull(cloneObj.getSubPartitionNameByIdx(100));
		Assert.assertNotNull(cloneObj.getSubPartitions());

	}

	/**
	 * 
	 * @return PartitionInfo
	 */
	public static PartitionInfo newPartitionInfo() {
		PartitionInfo pt = new PartitionInfo();
		pt.setBoundaryValueOnRight(true);
		pt.setPartitionColumnCount(1);
		pt.setPartitionCount(4);
		pt.setDDL("partition by hash (f1) partitions(4) ");
		pt.setPartitionExp("f1");
		pt.setPartitionFunc("");
		pt.setPartitionMethod(HASH);
		List<Column> partionColumns = new ArrayList<Column>();
		Column col = new Column();
		col.setName("f1");
		col.setDataType("varchar(100)");
		partionColumns.add(col);
		pt.setPartitionColumns(partionColumns);
		List<PartitionTable> partitions = new ArrayList<PartitionTable>();
		for (int i = 0; i < 4; i++) {
			PartitionTable ptable = new PartitionTable();
			ptable.setPartitionDesc("1" + i + "0000");
			ptable.setPartitionIdx(i);
			ptable.setPartitionName("part_" + i);
			partitions.add(ptable);
		}
		pt.setPartitions(partitions);

		partionColumns = new ArrayList<Column>();
		col = new Column();
		col.setName("f2");
		col.setDataType("varchar(100)");
		pt.setSubPartitionColumns(partionColumns);
		pt.setSubPartitionColumnCount(1);
		pt.setSubPartitionCount(4);
		pt.setSubPartitionExp("f2");
		pt.setSubPartitionFunc("");
		pt.setSubPartitionMethod(HASH);
		for (int i = 0; i < 4; i++) {
			PartitionTable ptable = new PartitionTable();
			ptable.setPartitionDesc("1" + i + "0000");
			ptable.setPartitionIdx(i);
			ptable.setPartitionName("sub_part_" + i);
			pt.addSubPartition(ptable);
		}
		PartitionInfo cloneObj = (PartitionInfo) pt.clone();
		return cloneObj;
	}

}
