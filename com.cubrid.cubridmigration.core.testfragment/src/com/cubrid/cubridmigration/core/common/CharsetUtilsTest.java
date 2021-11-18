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
package com.cubrid.cubridmigration.core.common;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.core.common.json.ReadFile;

public class CharsetUtilsTest {
	@Test
	public void testCharset() {
		Charset charset = Charset.forName("iso8859-1");
		System.out.println(charset.name() + ":" + charset.aliases().toString());

		Assert.assertEquals(2, CharsetUtils.getCharsetByte("1111"));
		CharsetUtils.getCharsetByte("utf-8");
		Assert.assertEquals(3, CharsetUtils.getCharsetByte("  "));
		Assert.assertEquals(3, CharsetUtils.getCharsetByte(null));
	}

	@Test
	public void testGetCharsetByte() throws IOException {
		String path = System.getProperty("user.dir")
				+ "/src/com/cubrid/cubridmigration/core/dbmetadata/";
		String content = ReadFile.readFile(path + "oracle charset.txt");
		String[] charsets = content.split("\n");

		for (String oracleCharset : charsets) {
			int charsetByte = CharsetUtils.getOracleCharsetByte(oracleCharset);

			System.out.println(oracleCharset + ":" + charsetByte);

		}

	}

	@Test
	public void testGetCharsets() {
		Map<String, Charset> map = Charset.availableCharsets();
		Iterator<String> it = map.keySet().iterator();

		while (it.hasNext()) {
			// Get charset name 
			String charsetName = (String) it.next();
			// Get charset 
			Charset charset = Charset.forName(charsetName);

			if (charset.isRegistered()) {
				System.out.println(charset);
			}
		}

		System.out.println("end");
	}

	@Test
	public void testTrunOracleCs2Normal() throws Exception {
		Assert.assertEquals("UTF8",
				CharsetUtils.turnOracleCharset2Normal("XXXUTF8"));
		Assert.assertEquals("GBK",
				CharsetUtils.turnOracleCharset2Normal("XXXGBK"));
		Assert.assertEquals("ASCII",
				CharsetUtils.turnOracleCharset2Normal("XXXASCII"));
		Assert.assertEquals("ISO8859-1",
				CharsetUtils.turnOracleCharset2Normal("XXXISO8859P1"));
		Assert.assertEquals("UTF16",
				CharsetUtils.turnOracleCharset2Normal("XXXUTF16"));
		Assert.assertEquals("GB2312",
				CharsetUtils.turnOracleCharset2Normal("XXXGB2312"));

		new String("aaaaa".getBytes("utf16"), "UTF16");
	}
	//	@Test
	//	public void testcharSizeChange() {
	//		Assert.assertTrue(CharsetUtils.charSizeChange("utf-8", "utf-8"));
	//		Assert.assertFalse(CharsetUtils.charSizeChange("utf-8", "gbk"));
	//	}
}
