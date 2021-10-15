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
package com.cubrid.cubridmigration.mssql.trans;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.BaseTestCaseWithJDBC;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.trans.MigrationTransFactory;

public class MSSQL2CUBRIDTranformHelperTest extends
		BaseTestCaseWithJDBC {

	@Test
	public void test1() {
		MSSQL2CUBRIDTranformHelper transformHelper = (MSSQL2CUBRIDTranformHelper) MigrationTransFactory.getTransformHelper(
				DatabaseType.MSSQL, DatabaseType.CUBRID);
		Column srcColumn = new Column();
		DataTypeInstance dti = new DataTypeInstance();
		dti.setName("numeric");
		dti.setPrecision(18);
		dti.setScale(0);
		srcColumn.setDataTypeInstance(dti);

		Column cubridColumn = new Column();
		DataTypeInstance dti2 = new DataTypeInstance();
		dti2.setName("numeric");
		dti2.setPrecision(18);
		dti2.setScale(0);
		cubridColumn.setDataTypeInstance(dti2);

		srcColumn.setDefaultValue(null);
		transformHelper.adjustDefaultValue(srcColumn, cubridColumn);
		Assert.assertNull(cubridColumn.getDefaultValue());

		srcColumn.setDefaultValue("NULL");
		transformHelper.adjustDefaultValue(srcColumn, cubridColumn);
		Assert.assertNull(cubridColumn.getDefaultValue());

		srcColumn.setDefaultValue("(NULL)");
		transformHelper.adjustDefaultValue(srcColumn, cubridColumn);
		Assert.assertNull(cubridColumn.getDefaultValue());

		srcColumn.setDefaultValue(" (NULL) ");
		transformHelper.adjustDefaultValue(srcColumn, cubridColumn);
		Assert.assertNull(cubridColumn.getDefaultValue());

		srcColumn.setDefaultValue("(1)");
		transformHelper.adjustDefaultValue(srcColumn, cubridColumn);
		Assert.assertEquals("1", cubridColumn.getDefaultValue());

		srcColumn.setDefaultValue("((1))");
		transformHelper.adjustDefaultValue(srcColumn, cubridColumn);
		Assert.assertEquals("1", cubridColumn.getDefaultValue());

		srcColumn.setDefaultValue("((N))");
		transformHelper.adjustDefaultValue(srcColumn, cubridColumn);
		Assert.assertNull(cubridColumn.getDefaultValue());

		srcColumn.setDefaultValue("'1'");
		transformHelper.adjustDefaultValue(srcColumn, cubridColumn);
		Assert.assertEquals("1", cubridColumn.getDefaultValue());
	}
}
