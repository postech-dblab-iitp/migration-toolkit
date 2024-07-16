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
package com.cubrid.cubridmigration.core.trans;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;

public class DBImportHelperTest {

	@Test
	public void testNewTargetColumn() throws Throwable {
		MigrationConfiguration config = TemplateParserTest.getMySQLConfig();
		Column result = MigrationTransFactory.getTransformHelper(
				DatabaseType.MYSQL, DatabaseType.CUBRID).newTargetColumn(
				config.getSrcColumnSchema(null, "code", "f_name"),
				config.getTargetTableSchema("code"), config);

		Assert.assertNotNull(result);
		Assert.assertEquals("f_name", result.getName());
	}

	@Test
	public void testCheckScale() {
		MapObject targetMappingItem = new MapObject();
		Column sourceColumn = new Column();
		Column targetColumn = new Column();
		//1
		final DBTransformHelper transformHelper = MigrationTransFactory.getTransformHelper(
				DatabaseType.CUBRID, DatabaseType.CUBRID);
		targetMappingItem.setScale("n");
		sourceColumn.setPrecision(38);
		sourceColumn.setScale(10);
		targetColumn.setPrecision(38);
		targetColumn.setScale(9);
		Assert.assertNotNull(transformHelper.checkScale(targetMappingItem,
				sourceColumn, targetColumn));

		sourceColumn.setPrecision(38);
		sourceColumn.setScale(9);
		targetColumn.setPrecision(38);
		targetColumn.setScale(10);
		Assert.assertNull(transformHelper.checkScale(targetMappingItem,
				sourceColumn, targetColumn));
		//2
		targetMappingItem.setScale("s");
		sourceColumn.setPrecision(38);
		sourceColumn.setScale(10);
		targetColumn.setPrecision(38);
		targetColumn.setScale(9);
		Assert.assertNotNull(transformHelper.checkScale(targetMappingItem,
				sourceColumn, targetColumn));

		sourceColumn.setPrecision(38);
		sourceColumn.setScale(9);
		targetColumn.setPrecision(38);
		targetColumn.setScale(10);
		Assert.assertNotNull(transformHelper.checkScale(targetMappingItem,
				sourceColumn, targetColumn));

		sourceColumn.setPrecision(37);
		sourceColumn.setScale(9);
		targetColumn.setPrecision(38);
		targetColumn.setScale(10);
		Assert.assertNull(transformHelper.checkScale(targetMappingItem,
				sourceColumn, targetColumn));
		//3
		targetMappingItem.setScale("11");
		sourceColumn.setPrecision(37);
		sourceColumn.setScale(9);
		targetColumn.setPrecision(38);
		targetColumn.setScale(10);
		Assert.assertNotNull(transformHelper.checkScale(targetMappingItem,
				sourceColumn, targetColumn));

		sourceColumn.setPrecision(37);
		sourceColumn.setScale(9);
		targetColumn.setPrecision(38);
		targetColumn.setScale(12);
		Assert.assertNull(transformHelper.checkScale(targetMappingItem,
				sourceColumn, targetColumn));
		//4
		targetMappingItem.setScale(null);

		sourceColumn.setPrecision(37);
		sourceColumn.setScale(9);
		targetColumn.setPrecision(38);
		targetColumn.setScale(12);
		Assert.assertNull(transformHelper.checkScale(targetMappingItem,
				sourceColumn, targetColumn));

	}

	@Test
	public void testNewTargetTable() throws Exception {
		MigrationConfiguration config = TemplateParserTest.getMySQLConfig();

		Table st = config.getSrcTableSchema(null, "test_string");
		PartitionInfo pi = new PartitionInfo();
		pi.setDDL("partition by hash f1 by 4");
		st.setPartitionInfo(pi);
		Table tt = MigrationTransFactory.getTransformHelper(DatabaseType.MYSQL,
				DatabaseType.CUBRID).createRDBTable(
				new SourceEntryTableConfig(), st, config);
		Assert.assertNotNull(tt.getPartitionInfo());
	}
}
