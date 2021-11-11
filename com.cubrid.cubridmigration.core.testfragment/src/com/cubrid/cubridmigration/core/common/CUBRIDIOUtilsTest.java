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
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class CUBRIDIOUtilsTest {

	static {
		PathUtils.initPaths();
	}

	//	@Test
	//	public void testProcessLineFileLineHandler() throws IOException {
	//		TestUtil2.setTestServerConfFilePath();
	//		File file = new File(PathUtil.getServerConfFile());
	//		IOUtil.processLine(file, new LineHandler() {
	//			public boolean handle(String line) {
	//				return false;
	//			}
	//		});
	//	}
	@Test
	public void testWriteToFile() throws Exception {
		Reader reader = new StringReader("testtesttest");
		final String pathname = "./temp/test.txt";
		final File file = new File(pathname);
		if (!file.exists()) {
			PathUtils.createFile(file);
		}
		CUBRIDIOUtils.writeToFile(reader, pathname, CUBRIDIOUtils.DEFAULT_CHARSET);
		Assert.assertTrue(file.delete());
		//		CUBRIDIOUtils.saveTable2Excel(tabs, columns, data, file);
	}

	@Test
	public void testSaveTable2Excel() throws Exception {
		final String pathname = "./temp/test.xls";
		String[] tabs = new String[] { "tab1", "tab2" };
		List<String[]> columns = new ArrayList<String[]>();
		columns.add(new String[] { "c11", "c12", "c13" });
		columns.add(new String[] { "c21", "c22" });
		List<List<String[]>> data = new ArrayList<List<String[]>>();
		List<String[]> d1 = new ArrayList<String[]>();
		d1.add(new String[] { "v11", "v12", "v13" });
		data.add(d1);
		List<String[]> d2 = new ArrayList<String[]>();
		d2.add(new String[] { "v21", "v22" });
		data.add(d2);
		CUBRIDIOUtils.saveTable2Excel(tabs, columns, data, pathname);
		Assert.assertTrue(new File(pathname).delete());
	}

	@Test
	public void testReadFile() throws Exception {
		URL url = CUBRIDIOUtilsTest.class.getResource("/com/cubrid/cubridmigration/jdbc.properties");
		String result = CUBRIDIOUtils.readFile(new FileReader(new File(url.getFile())));
		Assert.assertNotNull(result);
	}

	@Test
	public void testWriteLines() throws Exception {
		File file = new File("test.test");
		file.createNewFile();
		CUBRIDIOUtils.writeLines(file, new String[] { "", "" });

		file.createNewFile();
		CUBRIDIOUtils.writeLines(file, new String[] { "test", "test2" }, "utf-8");
		file.delete();
	}

	@Test
	public void testZip() throws Exception {
		File[] files = new File[] { new File("./temp/zip/test1.test"),
				new File("./temp/zip/test2.test"), new File("./temp/zip/1/test3.test"),
				new File("./temp/zip/1/test4.test"), new File("./temp/zip/2/test5.test"),
				new File("./temp/zip/2/test6.test") };
		for (File fl : files) {
			PathUtils.createFile(fl);
		}
		final String zipFilename = "./temp/test.test.zip";
		CUBRIDIOUtils.zip(new File(zipFilename).getCanonicalPath(),
				new String[] { files[0].getParentFile().getCanonicalPath() }, true);

		String[] deleteFiles = new String[] { ("./temp/zip/2/test5.test"),
				("./temp/zip/2/test6.test"), ("./temp/zip/2/"), ("./temp/zip/1/test3.test"),
				("./temp/zip/1/test4.test"), ("./temp/zip/1/"), ("./temp/zip/test2.test"),
				("./temp/zip/test1.test"), ("./temp/zip/") };
		for (String df : deleteFiles) {
			new File(df).delete();
		}

		testUnZip();
	}

	public void testUnZip() throws Exception {
		final String pathname = "./temp/test.test.zip";
		final File zipFilename = new File(pathname);
		Assert.assertTrue(CUBRIDIOUtils.getFileLength(pathname) > 0);
		final File outFile = new File(PathUtils.getReportDir() + "/test.test.zip");
		PathUtils.createFile(outFile);
		CUBRIDIOUtils.copyFile(zipFilename, outFile);
		CUBRIDIOUtils.unzip(zipFilename.getCanonicalPath(), PathUtils.getReportDir());

		String[] deleteFiles = new String[] { (PathUtils.getReportDir() + "/2/test5.test"),
				(PathUtils.getReportDir() + "/2/test6.test"), (PathUtils.getReportDir() + "/2/"),
				(PathUtils.getReportDir() + "/1/test3.test"),
				(PathUtils.getReportDir() + "/1/test4.test"), (PathUtils.getReportDir() + "/1/"),
				(PathUtils.getReportDir() + "/test2.test"),
				(PathUtils.getReportDir() + "/test1.test") };
		for (String df : deleteFiles) {
			new File(df).delete();
		}
		CUBRIDIOUtils.extractFromZip(pathname, "/2/test5.test", PathUtils.getReportDir());
		CUBRIDIOUtils.clearFileOrDir(PathUtils.getReportDir());
		Assert.assertTrue(zipFilename.delete());

	}

	@Test
	public void testIsLocal() throws Exception {
		Assert.assertTrue(CUBRIDIOUtils.isLocal("localhost"));
		Assert.assertTrue(CUBRIDIOUtils.isLocal("127.0.0.1"));
		Assert.assertTrue(CUBRIDIOUtils.isLocal(InetAddress.getLocalHost().getHostAddress()));
		Assert.assertFalse(CUBRIDIOUtils.isLocal("192.168.1.199"));
		Assert.assertFalse(CUBRIDIOUtils.isLocal("255.34.64.199"));

	}
	//	@Test
	//	public final void testMergeFile() throws IOException, URISyntaxException {
	//		URL url = ClassLoader.getSystemResource("./");
	//		File path = new File(url.toURI());
	//
	//		File file = new File(path, "1.txt");
	//
	//		File file1 = new File(path, "2.txt");
	//		IOUtils.writeLines(file1, new String[]{"1" }, null);
	//
	//		File file2 = new File(path, "3.txt");
	//		IOUtils.writeLines(file2, new String[]{"2" }, null);
	//
	//		List<File> list = new ArrayList<File>();
	//		list.add(file1);
	//		list.add(file2);
	//		IOUtils.mergeFileToSummary(file, file1);
	//		IOUtils.mergeFileToSummary(file, file2);
	//	}
}
