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

import java.io.File;

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.BaseTestCaseWithPath;

public class PathUtilsTest extends
		BaseTestCaseWithPath {

	@Test
	public void testDirs() {
		System.out.println(PathUtils.getWorkspace());
		System.out.println(PathUtils.getCMTWorkspace());
		System.out.println(PathUtils.getSchemaCacheDir());
		System.out.println(PathUtils.getLogDir());
		System.out.println(PathUtils.getDefaultBaseTempDir());
		System.out.println(PathUtils.getScriptDir());
		System.out.println(PathUtils.getGSSLoginFile());

		System.out.println(PathUtils.getUserHomeDir());
		System.out.println(PathUtils.getDefaultKrbConfigFile());
		System.out.println(PathUtils.getDefaultTicketFile());

		Assert.assertEquals("test", PathUtils.extracFileExt("test.t.test"));
		Assert.assertEquals("10,000 KB", PathUtils.getFileKBSize(10000000));
		Assert.assertEquals("10,001 KB", PathUtils.getFileKBSize(10000100));
		Assert.assertEquals("a_b_c", PathUtils.transStr2FileName("a:b c"));
	}

	@Test
	public void testCheckPathExist() {
		PathUtils.checkPathExist(new File("."));
		PathUtils.checkPathExist(new File("./check"));
		new File("./check").delete();
	}

	@Test
	public void testGetMonitorHistoryDir() {
		Assert.assertNotNull(PathUtils.getMonitorHistoryDir());
	}

	@Test
	public void testMergePath() {
		System.out.println(PathUtils.mergePath("/home/cmt/", "lob/"));
		System.out.println(PathUtils.mergePath("/home/cmt/", "/lob/"));
		System.out.println(PathUtils.mergePath("/home/cmt", "lob/"));
		System.out.println(PathUtils.mergePath("/home/cmt", null));
		System.out.println(PathUtils.mergePath(null, "lob/"));
		System.out.println(PathUtils.mergePath(null, null));
		System.out.println(PathUtils.mergePath(null, "/lob/"));
		System.out.println(PathUtils.mergePath("/home/cmt/", null));
	}

	//	@Test
	//	public void testSetInstallLocation() {
	//		URL installLocation = ClassLoader.getSystemResource("");
	//		PathUtils.setInstallLocation(installLocation);
	//		Assert.assertNotNull(PathUtils.getInstallPath());
	//	}
	//
	//	@Test
	//	public void testSetJdbcLibDir() {
	//		PathUtils.setJDBCLibDir(TestUtil2.getJdbcPath());
	//		System.out.println(PathUtils.getJDBCLibDir());
	//	}

	@Test
	public void testSetTempDir() throws Exception {
		String dir = new File(System.getProperty("user.dir"), "tmp").getCanonicalPath();
		PathUtils.setBaseTempDir(dir);
		System.out.println(PathUtils.getBaseTempDir());
		PathUtils.setBaseTempDir(null);
		System.out.println(PathUtils.getBaseTempDir());
	}

	@Test
	public void testGetFileNameWithoutExtendName() {
		Assert.assertEquals("/home/xxx/file",
				PathUtils.getFileNameWithoutExtendName("/home/xxx/file.xml"));
		Assert.assertEquals("c:\\home\\xxx\\file",
				PathUtils.getFileNameWithoutExtendName("c:\\home\\xxx\\file.xml"));
		Assert.assertEquals("file", PathUtils.getFileNameWithoutExtendName("file.xml"));
		Assert.assertEquals("", PathUtils.getFileNameWithoutExtendName(".xml"));
		Assert.assertEquals("/", PathUtils.getFileNameWithoutExtendName("/.xml"));
		Assert.assertEquals("file", PathUtils.getFileNameWithoutExtendName("file"));
	}
}
