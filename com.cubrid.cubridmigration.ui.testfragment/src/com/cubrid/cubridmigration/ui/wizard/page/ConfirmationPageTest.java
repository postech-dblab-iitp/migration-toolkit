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
package com.cubrid.cubridmigration.ui.wizard.page;

import java.util.List;

import org.eclipse.swt.custom.StyleRange;
import org.junit.Test;

import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.TemplateParserTest;

public class ConfirmationPageTest {

	@Test
	public void test_getConfigSummary() throws Exception {
		List<StyleRange> styleRanges = null;
		MigrationConfiguration migration = TemplateParserTest.get_OL_CUBRID2CUBRIDConfig();
		String result = ConfirmationPage.getConfigSummary(migration, styleRanges);
		//Assert.assertEquals(1021,result.length());
		System.out.println(result);

		migration = TemplateParserTest.get_OL_CUBRID2CSVConfig();
		result = ConfirmationPage.getConfigSummary(migration, styleRanges);
		//Assert.assertEquals(1249,result.length());
		System.out.println(result);

		migration = TemplateParserTest.get_OL_CUBRID2DumpConfig();
		result = ConfirmationPage.getConfigSummary(migration, styleRanges);
		//Assert.assertEquals(1047,result.length());
		System.out.println(result);

		migration = TemplateParserTest.get_OL_CUBRID2SQLConfig();
		result = ConfirmationPage.getConfigSummary(migration, styleRanges);
		//Assert.assertEquals(1328,result.length());
		System.out.println(result);

		migration = TemplateParserTest.get_OL_CUBRID2XLSConfig();
		result = ConfirmationPage.getConfigSummary(migration, styleRanges);
		//Assert.assertEquals(1249,result.length());
		System.out.println(result);

		migration = TemplateParserTest.get_OF_MySQLXML2CUBRIDConfig();
		result = ConfirmationPage.getConfigSummary(migration, styleRanges);
		//Assert.assertEquals(1034,result.length());
		System.out.println(result);
	}
}
