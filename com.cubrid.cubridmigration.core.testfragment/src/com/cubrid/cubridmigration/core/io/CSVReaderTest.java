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
package com.cubrid.cubridmigration.core.io;

import java.io.StringReader;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import au.com.bytecode.opencsv.CSVReader;

public class CSVReaderTest {

	@Test
	public void testCSVReader() throws Exception {
		StringReader sr = new StringReader(
				"aaa,\"bbb\",ccc\r\nddd,\"eee\",fff\r\nggg,hhh,iii");
		CSVReader reader = new CSVReader(sr, ',', '"', 1);
		List<String[]> result = reader.readAll();
		Assert.assertEquals(2, result.size());
		Assert.assertEquals("ddd", result.get(0)[0]);
		Assert.assertEquals("eee", result.get(0)[1]);

	}

	@Test
	public void testCSVReader2() throws Exception {
		StringReader sr = new StringReader("aaa");
		CSVReader reader = new CSVReader(sr, ',', '"', 0);
		List<String[]> result = reader.readAll();
		Assert.assertEquals(1, result.size());
	}

	@Test
	public void testCSVReader3() throws Exception {
		StringReader sr = new StringReader("aaa\r\nbbb");
		CSVReader reader = new CSVReader(sr, ',', '"', 0);
		List<String[]> result = reader.readAll();
		Assert.assertEquals(2, result.size());
	}
}
