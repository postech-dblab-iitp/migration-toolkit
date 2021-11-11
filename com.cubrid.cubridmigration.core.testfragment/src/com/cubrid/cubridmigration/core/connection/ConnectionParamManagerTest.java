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
package com.cubrid.cubridmigration.core.connection;

import java.io.File;
import java.net.URL;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;

/**
 * 
 * ConnectionParamManager Test cases.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-6-11 created by Kevin Cao
 */
public class ConnectionParamManagerTest {

	@Test
	public void testConnectionParamManager() throws Exception {
		MigrationConfiguration config = TemplateParserTest.get_OL_MySQL2CUBRIDConfig();

		URL url = ClassLoader.getSystemResource("com/cubrid/cubridmigration/jdbcconnection.xml");
		String driverPath = PathUtils.getURLFilePath(url);
		final CMTConParamManager cpm = CMTConParamManager.getInstance();
		final File file = new File(driverPath);
		cpm.loadFromFile(file);
		cpm.setDefaultFile(file);

		//Test add null
		cpm.addConnection(null, false);

		final ConnParameters sourceConParams = config.getSourceConParams();
		sourceConParams.setName("mysqltest");

		final ConnParameters sourceConParams2 = sourceConParams.clone();
		//Add duplicated name connection
		sourceConParams2.setName("mysqltest");
		cpm.addConnection(sourceConParams2, false);
		Assert.assertNotNull(cpm.getConnection("mysqltest"));

		//Add connection
		sourceConParams2.setName("mysqltest2");
		sourceConParams2.setConUser("test");
		cpm.addConnection(sourceConParams2, false);
		Assert.assertNotNull(cpm.getConnection("mysqltest2"));
		Assert.assertEquals(5, cpm.getConnections().size());
		Assert.assertTrue(cpm.isConnectionExists(sourceConParams2));
		Assert.assertTrue(cpm.isNameUsed("mysqltest2"));

		//Remove connection
		cpm.removeConnection("mysqltest2", false);
		Assert.assertNull(cpm.getConnection("mysqltest2"));
		Assert.assertEquals(4, cpm.getConnections().size());
		Assert.assertFalse(cpm.isConnectionExists(sourceConParams2));
		Assert.assertFalse(cpm.isNameUsed("mysqltest2"));

		//Update connection: old connection not found 
		cpm.updateConnection("mysqltest2", sourceConParams, false);
		//Update connection: new connection null
		cpm.updateConnection("mysqltest", null, false);
		//Update connection: is same DB
		cpm.updateConnection("mysqltest", sourceConParams2, false);
		Assert.assertNotNull(cpm.getConnection("mysqltest2"));
		Assert.assertNull(cpm.getConnection("mysqltest"));
		//Restore
		cpm.updateConnection("mysqltest2", sourceConParams, false);
		Assert.assertNotNull(cpm.getConnection("mysqltest"));
		Assert.assertNull(cpm.getConnection("mysqltest2"));

		//Test catalogs
		Catalog catalog = TemplateParserTest.getMySQLCatalog(config);
		Assert.assertNull(cpm.getCatalog("mysqltest"));
		cpm.updateCatalog("mysqltest", catalog);
		Assert.assertNotNull(cpm.getCatalog("mysqltest"));
		cpm.updateCatalog("mysqltest", null);
		Assert.assertNull(cpm.getCatalog("mysqltest"));
	}
}
