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

import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.Test;

public class CSVSettingsTest {

	@Test
	public void testCSVSettings() {
		CSVSettings settings = new CSVSettings();
		Assert.assertFalse(settings.equals(null));

		CSVSettings settings2 = new CSVSettings();
		Assert.assertTrue(settings.equals(settings2));

		final ArrayList<String> ns = new ArrayList<String>();
		ns.add("NULL");
		settings.setNullStrings(ns);
		Assert.assertFalse(settings.equals(settings2));

		ns.clear();
		ns.add("\\N");
		ns.add("NULL");
		ns.add("(NULL)");
		settings.setNullStrings(ns);
		Assert.assertTrue(settings.equals(settings2));
		settings.setSeparateChar(';');
		Assert.assertFalse(settings.equals(settings2));

		settings.setSeparateChar(',');
		Assert.assertTrue(settings.equals(settings2));
		settings.setEscapeChar('|');
		Assert.assertFalse(settings.equals(settings2));

		settings.setEscapeChar('\0');
		Assert.assertTrue(settings.equals(settings2));
		settings.setQuoteChar('\'');
		Assert.assertFalse(settings.equals(settings2));

		settings.setQuoteChar('\"');
		Assert.assertTrue(settings.equals(settings2));
		settings.setCharset("utf8");
		Assert.assertFalse(settings.equals(settings2));
	}
}
