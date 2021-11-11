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

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.hadoop.HadoopStreamFactory;

/**
 * 
 * IOUtil
 * 
 * @author lcl
 * @version 1.0 - 2009-9-18
 */
public final class CUBRIDIOUtils {
	private final static Logger LOG = LogUtil.getLogger(CUBRIDIOUtils.class);

	private static final int BUFFER_SIZE = 4096;
	private static final String OS_NAME = getSystemProperty("os.name");
	public static final boolean IS_OS_WINDOWS = getOSMatches("Windows");
	public static final String DEFAULT_CHARSET = System.getProperty("file.encoding");
	public static final int DEFAULT_MEMORY_CACHE_SIZE = 1024 * 1024;

	/**
	 * Clear directory
	 * 
	 * @param file to be cleared
	 */
	public static void clearFileOrDir(File file) {
		if (!file.exists()) {
			return;
		}
		if (file.isFile()) {
			file.delete();
			return;
		}
		File[] children = file.listFiles();
		if (children == null || children.length == 0) {
			file.delete();
			return;
		}
		for (File child : children) {
			clearFileOrDir(child);
		}
		file.delete();
	}

	/**
	 * Clear directory
	 * 
	 * @param dir to be cleared
	 */
	public static void clearFileOrDir(String dir) {
		File file = new File(dir);
		clearFileOrDir(file);
	}

	/**
	 * Copy file
	 * 
	 * @param f1 File
	 * @param f2 File
	 * @throws IOException ex
	 */
	public static void copyFile(File f1, File f2) throws IOException {
		if (f1 == null || f2 == null || !f1.exists()) {
			return;
		}
		int length = 2097152;
		FileInputStream in = new FileInputStream(f1);
		FileOutputStream out = new FileOutputStream(f2);
		FileChannel inC = in.getChannel();
		FileChannel outC = out.getChannel();
		ByteBuffer b = null;
		while (true) {
			if (inC.position() == inC.size()) {
				inC.close();
				outC.close();
				return;
			}
			if ((inC.size() - inC.position()) < length) {
				length = (int) (inC.size() - inC.position());
			} else {
				length = 2097152;
			}

			b = ByteBuffer.allocateDirect(length);
			inC.read(b);
			b.flip();
			outC.write(b);
			outC.force(false);
		}
	}

	/**
	 * Copy fold and children fold
	 * 
	 * @param src File
	 * @param dest File
	 * @throws IOException ex
	 */
	public static void copyFolder(File src, File dest) throws IOException {
		if (src == null || !src.exists() || dest == null) {
			return;
		}
		if (src.isDirectory()) {
			if (!dest.exists()) {
				dest.mkdirs();
			}
			String files[] = src.list();
			if (files == null) {
				return;
			}
			for (String file : files) {
				File srcFile = new File(src, file);
				File destFile = new File(dest, file);
				copyFolder(srcFile, destFile);
			}
		} else {
			copyFile(src, dest);
		}
	}

	/**
	 * UnZip a file to target directory.
	 * 
	 * @param zipFilename the target zip file
	 * @param toBeExtracted the file name in the zip file or the extend name in
	 *        the zip file.
	 * @param outputDir the output directory.
	 * @return result file full name
	 * @throws IOException if errors
	 */
	public static String extractFromZip(String zipFilename, String toBeExtracted, String outputDir) throws IOException {
		ZipFile zipFile = new ZipFile(zipFilename);
		File otDir = new File(outputDir);
		try {
			if (!otDir.exists()) {
				otDir.mkdirs();
			}
			Enumeration<? extends ZipEntry> en = zipFile.entries();
			while (en.hasMoreElements()) {
				ZipEntry zipEntry = (ZipEntry) en.nextElement();
				if (zipEntry.isDirectory()) {
					continue;
				}
				final String zipName = zipEntry.getName();
				if (!zipName.equals(toBeExtracted) && !zipName.endsWith(toBeExtracted)) {
					continue;
				}
				// unzip file   
				//replace the file by default
				File file = new File(otDir.getAbsoluteFile() + File.separator + zipName);
				//If the file can't be delete because of the file is in using, return the file directly.
				if (file.exists() && !file.delete()) {
					return file.getAbsolutePath();
				}
				PathUtils.createFile(file);
				InputStream in = zipFile.getInputStream(zipEntry);
				FileOutputStream out = new FileOutputStream(file);
				try {
					int c;
					byte[] by = new byte[1024];
					while ((c = in.read(by)) != -1) {
						out.write(by, 0, c);
					}
				} finally {
					out.close();
					in.close();
				}
				return file.getAbsolutePath();
			}
		} finally {
			zipFile.close();
		}
		return null;
	}

	/**
	 * 
	 * Create file input stream: local file or HDFS file
	 * 
	 * @param fileName local file or hdfs://..... file
	 * @return input stream
	 * @throws IOException ex
	 */
	public static InputStream getFileInputStream(String fileName) throws IOException {
		if (StringUtils.isBlank(fileName)) {
			throw new IllegalArgumentException(
					"Can't create input stream: file name can't be empty.");
		}
		if (fileName.startsWith("hdfs://")) {
			return HadoopStreamFactory.getFileInputStream(fileName);
		}
		return new BufferedInputStream(new FileInputStream(fileName));
	}

	/**
	 * 
	 * Retrieves file lenght: local file or HDFS file
	 * 
	 * @param fileName local file or hdfs://..... file
	 * @return file length
	 */
	public static long getFileLength(String fileName) {
		if (StringUtils.isBlank(fileName)) {
			throw new IllegalArgumentException(
					"Can't create input stream: file name can't be empty.");
		}
		if (fileName.startsWith("hdfs://")) {
			FileStatus fileStatus;
			try {
				fileStatus = HadoopStreamFactory.getFileStatus(fileName);
				return fileStatus == null ? 0 : fileStatus.getLen();
			} catch (IOException e) {
				LOG.error("Get file length error.", e);
				return 0;
			}
		}
		return new File(fileName).length();
	}

	/**
	 * match os name
	 * 
	 * @param osNamePrefix String
	 * @return boolean
	 */
	private static boolean getOSMatches(String osNamePrefix) {
		if (OS_NAME == null) {
			return false;
		}

		return OS_NAME.startsWith(osNamePrefix);
	}

	/**
	 * get SystemProperty
	 * 
	 * @param property String
	 * @return String
	 */
	private static String getSystemProperty(String property) {
		try {
			return System.getProperty(property);
		} catch (SecurityException ex) {
			LOG.error(new StringBuffer().append(
					"Caught a SecurityException reading the system property '").append(property).append(
					"'; the SystemUtils property value will default to null.").toString());
			return null;
		}
	}

	/**
	 * Retrieves that if the ip address is local host
	 * 
	 * @param ip the IP address
	 * @return true if local
	 */
	public static boolean isLocal(String ip) {
		if ("localhost".equalsIgnoreCase(ip) || "127.0.0.1".equals(ip)) {
			return true;
		}
		try {
			InetAddress[] mArLocalIP = InetAddress.getAllByName(InetAddress.getLocalHost().getHostName());
			for (InetAddress address : mArLocalIP) {
				if (address.getHostAddress().equals(ip)) {
					return true;
				}
			}
		} catch (Exception e) {
			LOG.error("", e);
		}
		return false;
	}

	/**
	 * Use XMLdecoder to load a java object from a XML file
	 * 
	 * @param xmlFile to be load
	 * @return the object
	 */
	public static Object loadObjectFromXML(String xmlFile) {
		try {
			XMLDecoder xe = new XMLDecoder(new FileInputStream(xmlFile));
			try {
				return xe.readObject();
			} finally {
				xe.close();
			}
		} catch (FileNotFoundException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Merge the content of source file to the target file.
	 * 
	 * @param sourceFile is the file to be read.
	 * @param targetFile is the file to be written.
	 * @throws IOException when IO errors
	 */
	public static void mergeFile(String sourceFile, String targetFile) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]sourceFile=" + sourceFile + ", targetFile=" + targetFile);
		}
		File sFile = new File(sourceFile);
		File file = new File(targetFile);
		// If target file is source file , don't merge.
		if (sFile.getCanonicalPath().equals(file.getCanonicalPath())) {
			return;
		}
		if (!file.exists()) {
			PathUtils.createFile(file);
		}
		OutputStream os = new BufferedOutputStream(new FileOutputStream(file, true));
		InputStream is = new BufferedInputStream(new FileInputStream(sFile));
		try {
			byte[] cache = new byte[CUBRIDIOUtils.DEFAULT_MEMORY_CACHE_SIZE];
			int count = is.read(cache);
			while (count >= 0) {
				os.write(cache, 0, count);
				count = is.read(cache);
			}
		} finally {
			os.close();
			is.close();
		}
	}

	/**
	 * Read data from an excel file (XLS/XLSX), Only support String type cells,
	 * other data type cell will be set to ""
	 * 
	 * @param fileName to read
	 * @return data
	 */
	public static List<String[]> readDataFromExcel(String fileName) {
		try {
			FileInputStream fs = null;
			try {
				fs = new FileInputStream(fileName);
				Workbook wb = WorkbookFactory.create(fs);

				Sheet sheet = wb.getSheetAt(0);
				List<String[]> result = new ArrayList<String[]>(sheet.getLastRowNum() + 1);
				for (int i = 0; i <= sheet.getLastRowNum(); i++) {
					Row row = sheet.getRow(i);
					String[] data = new String[row.getLastCellNum()];
					for (int j = 0; j < row.getLastCellNum(); j++) {
						Cell cell = row.getCell(j);
						data[j] = (cell.getCellType() != 1 || cell.getStringCellValue() == null) ? ""
								: cell.getStringCellValue();
					}
					result.add(data);
				}
				return result;
			} finally {
				if (fs != null) {
					fs.close();
				}
			}
		} catch (InvalidFormatException ex) {
			throw new RuntimeException(ex);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * get file content
	 * 
	 * @param file File
	 * @return String
	 * @throws IOException e
	 */
	public static String readFile(Reader file) throws IOException {
		BufferedReader in = new BufferedReader(file);
		StringBuffer buf = new StringBuffer();
		try {
			String line;

			while (null != (line = in.readLine())) {
				buf.append(line).append("\n");
			}

			return buf.toString();
		} finally {
			Closer.close(in);
		}
	}

	//	/**
	//	 * merger files
	//	 * 
	//	 * @param summaryFile File
	//	 * @param elemFile File
	//	 * @throws IOException e
	//	 */
	//	public static void mergeFileToSummary(File summaryFile, File elemFile) throws IOException {
	//		if (elemFile == null || summaryFile == null) {
	//			return;
	//		}
	//
	//		FileOutputStream outStream = null;
	//		FileInputStream inStream = null;
	//		try {
	//			outStream = new FileOutputStream(summaryFile, true);
	//			inStream = new FileInputStream(elemFile);
	//
	//			byte buf[] = new byte[4096];
	//			int length;
	//
	//			while ((length = inStream.read(buf)) > 0) {
	//				outStream.write(buf, 0, length);
	//				outStream.flush();
	//			}
	//		} finally {
	//			Closer.close(inStream);
	//			Closer.close(outStream);
	//		}
	//	}

	/**
	 * save Data to Excel
	 * 
	 * @param fileName the excel file name
	 * @param tabHeader the file header
	 * @param data List<String[]>
	 * @return true/false
	 */
	public static boolean saveDataToExcel(String fileName, String[] tabHeader, List<String[]> data) {
		// create a new work book
		HSSFWorkbook workbook = new HSSFWorkbook();

		// create a default sheet
		HSSFSheet sheet = workbook.createSheet();

		HSSFCellStyle cellStyle = workbook.createCellStyle();
		cellStyle.setAlignment(HSSFCellStyle.ALIGN_LEFT);

		// set header 
		HSSFRow row = sheet.createRow(0);

		for (int i = 0; i < tabHeader.length; i++) {
			HSSFCell cell = row.createCell(i);
			cell.setCellValue(tabHeader[i]);
			cell.setCellStyle(cellStyle);
			sheet.autoSizeColumn(i);
		}

		// set data
		for (int i = 1; i <= data.size(); i++) {
			row = sheet.createRow(i);

			String[] tmpdata = data.get(i - 1);

			for (int j = 0; j < tmpdata.length; j++) {
				HSSFCell cell = row.createCell(j);
				cell.setCellValue(tmpdata[j]);
				cell.setCellStyle(cellStyle);
			}
		}

		// save file
		FileOutputStream fileOut = null;
		try {
			fileOut = new FileOutputStream(fileName);
			workbook.write(fileOut);
			return true;
		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			Closer.close(fileOut);
		}

		return false;
	}

	/**
	 * Use XMLEncoder to save a java object to a XML file
	 * 
	 * @param xmlFile is the file
	 * @param obj to be saved
	 */
	public static void saveObject2XML(String xmlFile, Object obj) {
		try {
			XMLEncoder xe = new XMLEncoder(new FileOutputStream(xmlFile));
			try {
				xe.writeObject(obj);
				xe.flush();
			} finally {
				xe.close();
			}
		} catch (FileNotFoundException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Save table view data to excel file
	 * 
	 * @param tabs sheet information
	 * @param columns columns in every sheet
	 * @param data data in every sheet
	 * @param file to be saved.
	 */
	public static void saveTable2Excel(String[] tabs, List<String[]> columns,
			List<List<String[]>> data, String file) {
		if (tabs == null || tabs.length == 0) {
			return;
		}
		File tf = new File(file);
		if (tf.exists()) {
			PathUtils.deleteFile(tf);
		}
		try {
			PathUtils.createFile(tf);
			int index = 0;
			Workbook wb = new HSSFWorkbook();
			for (String tab : tabs) {
				Sheet sheet = wb.createSheet(tab);
				writeTvSheet(columns.get(index), data.get(index), sheet);
				index++;
			}
			FileOutputStream fileOut = new FileOutputStream(tf);
			try {
				wb.write(fileOut);
			} finally {
				fileOut.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * UnZip a file to target directory.
	 * 
	 * @param zipFilename the target zip file
	 * @param outputDir the output directory.
	 * @return List of files
	 * @throws IOException if errors
	 */
	public static List<File> unzip(String zipFilename, String outputDir) throws IOException {
		File outFile = new File(outputDir);
		if (!outFile.exists() && !outFile.mkdirs()) {
			throw new IOException();
		}
		ZipFile zipFile = new ZipFile(zipFilename);
		List<File> result = new ArrayList<File>();
		try {
			Enumeration<? extends ZipEntry> en = zipFile.entries();
			while (en.hasMoreElements()) {
				ZipEntry zipEntry = (ZipEntry) en.nextElement();
				if (zipEntry.isDirectory()) {
					// mkdir directory   
					String dirName = zipEntry.getName();
					dirName = dirName.substring(0, dirName.length() - 1);

					File dir = new File(outFile.getPath() + File.separator + dirName);
					if (!dir.exists() && !dir.mkdirs()) {
						LOG.error("Fail to create new directory.");
					}

				} else {
					// unzip file   
					File file = new File(outFile.getPath() + File.separator + zipEntry.getName());
					//replace the file by default
					if ((file.exists() && file.delete()) || (!file.exists())) {
						PathUtils.createFile(file);
					}

					InputStream in = zipFile.getInputStream(zipEntry);
					FileOutputStream out = new FileOutputStream(file);
					try {
						int c;
						byte[] by = new byte[1024];
						while ((c = in.read(by)) != -1) {
							out.write(by, 0, c);
						}
					} finally {
						out.close();
						in.close();
					}
					result.add(file);
				}
			}
		} finally {
			zipFile.close();
		}
		return result;
	}

	/**
	 * write Lines
	 * 
	 * @param file the file to be write.
	 * @param lines the content
	 * @throws IOException when IO errors.
	 */
	public static void writeLines(File file, String[] lines) throws IOException {
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(file);
			writeLines(fos, lines);
		} finally {
			Closer.close(fos);
		}
	}

	/**
	 * write Lines
	 * 
	 * @param file File
	 * @param lines String[]
	 * @param fileCharset String
	 * @throws IOException e
	 */
	public static void writeLines(File file, String[] lines, String fileCharset) throws IOException {
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(file);
			writeLines(fos, lines, fileCharset);
		} finally {
			Closer.close(fos);
		}
	}

	/**
	 * 
	 * write Lines
	 * 
	 * @param os the output stream to write.
	 * @param lines the content.
	 * @throws IOException when IO errors.
	 */
	public static void writeLines(OutputStream os, String[] lines) throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(os, DEFAULT_CHARSET);
		writeLines(writer, lines);
	}

	/**
	 * write Lines
	 * 
	 * @param os OutputStream
	 * @param lines String[]
	 * @param fileCharset String
	 * @throws IOException e
	 */
	public static void writeLines(OutputStream os, String[] lines, String fileCharset) throws IOException {
		OutputStreamWriter writer;

		if (fileCharset == null) {
			writer = new OutputStreamWriter(os, DEFAULT_CHARSET);
		} else {
			writer = new OutputStreamWriter(os, fileCharset);
		}

		writeLines(writer, lines);
	}

	/**
	 * writeLines
	 * 
	 * @param writer Writer
	 * @param lines String[]
	 * @throws IOException e
	 */
	public static void writeLines(Writer writer, String[] lines) throws IOException {
		BufferedWriter bw = new BufferedWriter(writer, BUFFER_SIZE);
		try {

			for (String line : lines) {
				bw.write(line);
				bw.newLine();
			}
		} finally {
			bw.flush();
		}
	}

	/**
	 * create File By InputStream
	 * 
	 * @param reader Reader
	 * @param fileName String
	 * @param fileCharset String
	 * @throws IOException e
	 */
	public static void writeToFile(Reader reader, String fileName, String fileCharset) throws IOException {

		Writer out = null;

		try {
			out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(fileName)),
					fileCharset));
			char buf[] = new char[4096];
			int len;

			while ((len = reader.read(buf)) > 0) {
				out.write(buf, 0, len);
				out.flush();
			}
		} finally {
			Closer.close(reader);
			Closer.close(out);
		}
	}

	/**
	 * create File By InputStream
	 * 
	 * @param fileName String
	 * @param inputStream InputStream
	 * @throws IOException e
	 */
	public static void writeToFile(String fileName, InputStream inputStream) throws IOException {

		BufferedOutputStream out = null;
		try {
			out = new BufferedOutputStream(new FileOutputStream(new File(fileName)));
			byte buf[] = new byte[4096];
			int len;

			while ((len = inputStream.read(buf)) > 0) {
				out.write(buf, 0, len);
				out.flush();
			}
		} finally {
			Closer.close(inputStream);
			Closer.close(out);
		}
	}

	/**
	 * Save data to excel sheet
	 * 
	 * @param columns of sheet
	 * @param data data
	 * @param sheet to be written
	 */
	private static void writeTvSheet(String[] columns, List<String[]> data, Sheet sheet) {
		int rdx = 0;
		Row row = sheet.createRow(rdx);

		for (int i = 0; i < columns.length; i++) {
			Cell cell = row.createCell(i);
			cell.setCellValue(columns[i]);
		}

		for (String[] ti : data) {
			rdx++;
			row = sheet.createRow(rdx);
			for (int i = 0; i < columns.length; i++) {
				Cell cell = row.createCell(i);
				cell.setCellValue(ti[i]);
			}
		}
	}

	/**
	 * Zip a file or a directory to a specified zip file
	 * 
	 * @param zipFileName the target zip file name.
	 * @param inputFiles the files to be Zipped.
	 * @param remove if true, the input files will be removed after zipped.
	 * @throws IOException if errors
	 */
	public static void zip(String zipFileName, String[] inputFiles, boolean remove) throws IOException {
		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFileName, true));
		try {
			for (String inFile : inputFiles) {
				final File inputFile = new File(inFile);
				zip(out, inputFile, "");
				if (remove) {
					PathUtils.deleteFile(inputFile);
				}
			}

		} finally {
			out.close();
		}
	}

	/**
	 * ZIP file to ZIP output stream
	 * 
	 * @param out ZIP output stream
	 * @param inputFile the file or path to be ZIP
	 * @param parentPath the parent path, don't end by '\' or '/'
	 * @throws IOException when error
	 */
	private static void zip(ZipOutputStream out, File inputFile, String parentPath) throws IOException {
		if (inputFile.isDirectory()) {
			File[] fl = inputFile.listFiles();
			if (fl == null) {
				return;
			}
			for (int i = 0; i < fl.length; i++) {
				if (fl[i].isDirectory()) {
					String pp = StringUtils.isBlank(parentPath) ? "" : parentPath + "/";
					out.putNextEntry(new ZipEntry(pp + fl[i].getName() + "/"));
					zip(out, fl[i], pp + fl[i].getName());
				} else {
					zip(out, fl[i], parentPath);
				}
			}
			return;
		}
		//If is file
		String pp = StringUtils.isBlank(parentPath) ? "" : parentPath + "/";
		out.putNextEntry(new ZipEntry(pp + inputFile.getName()));
		InputStream in = new BufferedInputStream(new FileInputStream(inputFile));
		try {
			byte[] buff = new byte[1024];
			int count = in.read(buff);
			while (count > 0) {
				out.write(buff, 0, count);
				count = in.read(buff);
			}
		} finally {
			in.close();
		}
	}

	private CUBRIDIOUtils() {
		//do nothing.
	}
}
