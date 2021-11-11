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
package com.cubrid.cubridmigration.core.common.xml;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;

/**
 * CUBRIDUtil
 * 
 * @author JessieHuang
 * @version 1.0 - 2010-01-14
 */
public class XMLMementoTest {
	XMLMemento xml;

	/**
	 * init
	 * 
	 * @throws IOException e
	 */
	@Before
	public final void init() throws IOException {
		StringBuffer buf = new StringBuffer();
		buf.append("<MySQL2CUBRID>");
		buf.append("<DataTypeMapping>");
		buf.append("<SourceDataType>");
		buf.append("<type>bit</type>");
		buf.append("<precision>1</precision>");
		buf.append("<scale></scale>");
		buf.append("</SourceDataType>");
		buf.append("<TargetDataType>");
		buf.append("<type>character</type>");
		buf.append("<precision>1</precision>");
		buf.append("<scale></scale>");
		buf.append("</TargetDataType>");
		buf.append("</DataTypeMapping>");
		buf.append("<DataTypeMapping>");
		buf.append("<SourceDataType>");
		buf.append("<type>bit</type>");
		buf.append("<precision>n</precision>");
		buf.append("</SourceDataType>");
		buf.append("<TargetDataType>");
		buf.append("<type>bit</type>");
		buf.append("<precision>n</precision>");
		buf.append("<scale></scale>");
		buf.append("</TargetDataType>");
		buf.append("</DataTypeMapping>");
		buf.append("</MySQL2CUBRID>");
		ByteArrayInputStream stream = new ByteArrayInputStream(
				buf.toString().getBytes());
		xml = (XMLMemento) XMLMemento.loadMemento(stream);
	}

	/**
	 * testCreateChild
	 */
	@Test
	public final void testCreateChild() {
		xml.createChild("abc");
		Assert.assertNotNull(xml.getChild("abc"));
	}

	/**
	 * testGetChild
	 */
	@Test
	public final void testGetChild() {
		IXMLMemento child = xml.getChild("DataTypeMapping");
		Assert.assertNotNull(child);
	}

	/**
	 * testGetChildren
	 */
	@Test
	public final void testGetChildren() {
		IXMLMemento[] children = xml.getChildren("DataTypeMapping");
		Assert.assertTrue(children.length > 0);
	}

	/**
	 * testGetFloat
	 */
	@Test
	public final void testGetFloat() {
		xml.putFloat("num", 1.23f);
		float res = xml.getFloat("num");
		Assert.assertTrue(1.23f == res);
		Assert.assertNull(xml.getFloat("noexistsattribute"));
	}

	/**
	 * testGetBoolean
	 */
	@Test
	public final void testGetBoolean() {
		xml.putBoolean("bool", Boolean.FALSE);
		xml.putBoolean("bool", Boolean.TRUE);
		boolean res = xml.getBoolean("bool");
		Assert.assertTrue(res);
		Assert.assertFalse(xml.getBoolean("noexistsattribute"));
	}

	/**
	 * testGetInteger
	 */
	@Test
	public final void testGetInteger() {
		xml.putInteger("int", 1);
		int res = xml.getInteger("int");
		Assert.assertTrue(res == 1);
		Assert.assertNull(xml.getInteger("noexistsattribute"));
	}

	/**
	 * testGetString
	 */
	@Test
	public final void testGetString() {
		xml.putString("str", "abc");
		String res = xml.getString("str");
		Assert.assertEquals("abc", res);
		Assert.assertNull(xml.getString("noexistsattribute"));
	}

	/**
	 * testGetTextData
	 */
	@Test
	public final void testGetTextData() {
		xml.putTextData("aaaaaaaaaa");
		Assert.assertNotNull(xml.getTextData());
	}

	/**
	 * testGetAttributeNames
	 */
	@Test
	public final void testGetAttributeNames() {
		xml.putString("str", "abc");
		Assert.assertTrue(xml.getAttributeNames().size() > 0);
	}

	/**
	 * testGetContents
	 * 
	 * @throws IOException e
	 */
	@Test
	public final void testGetContents() throws IOException {
		byte[] bs = xml.getContents();
		Assert.assertNotNull(bs);
	}

	/**
	 * testGetInputStream
	 * 
	 * @throws IOException e
	 */
	@Test
	public final void testGetInputStream() throws IOException {
		InputStream input = xml.getInputStream();
		Assert.assertNotNull(input);
	}

	/**
	 * testSaveToString
	 * 
	 * @throws IOException e
	 */
	@Test
	public final void testSaveToString() throws IOException {
		String str = xml.saveToString();
		Assert.assertNotNull(str);
	}

	/**
	 * testSaveToFile
	 * 
	 * @throws IOException e
	 */
	@Test
	public final void testSaveToFile() throws IOException {
		URL url = ClassLoader.getSystemResource("./");
		String path = CUBRIDIOUtils.IS_OS_WINDOWS ? url.getPath().substring(1)
				: url.getPath();
		String fileName = path + "testxml.xml";
		xml.saveToFile(fileName);

		XMLMemento newXml = (XMLMemento) XMLMemento.loadMemento(fileName);
		Assert.assertNotNull(newXml);

		File file = new File(fileName);
		boolean flag = file.delete();
		Assert.assertTrue(flag);
	}

	/**
	 * testCreateWriteRoot
	 * 
	 * @throws ParserConfigurationException e
	 */
	@Test
	public final void testCreateWriteRoot() throws ParserConfigurationException {
		XMLMemento newXml = XMLMemento.createWriteRoot("test");
		Assert.assertNotNull(newXml.getChildren("test"));
	}
}
