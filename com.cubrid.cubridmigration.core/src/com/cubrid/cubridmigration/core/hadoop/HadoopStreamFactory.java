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
package com.cubrid.cubridmigration.core.hadoop;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HadoopStreamFactory Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-8-30 created by Kevin Cao
 */
public class HadoopStreamFactory {

	/**
	 * Retrieves an input steam of parameter.
	 * 
	 * @param fileName local file name or file from hadoop
	 * @return InputStream
	 * @throws IOException ex
	 */
	public static InputStream getFileInputStream(String fileName) throws IOException {
		if (StringUtils.isBlank(fileName)) {
			throw new IllegalArgumentException(
					"Can't create input stream: file name can't be empty.");
		}
		if (fileName.startsWith("hdfs://")) {
			Configuration hdpCfg = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(fileName), hdpCfg); //FSDataImputStream
			FSDataInputStream in = fs.open(new Path(fileName));
			return in;
		}
		return new BufferedInputStream(new FileInputStream(fileName));
	}

	/**
	 * Retrieves the HDFS file status
	 * 
	 * @param fileName HDFS file
	 * @return file status or null
	 * @throws IOException ex
	 */
	public static FileStatus getFileStatus(String fileName) throws IOException {
		if (StringUtils.isBlank(fileName)) {
			throw new IllegalArgumentException(
					"Can't create input stream: file name can't be empty.");
		}
		if (fileName.startsWith("hdfs://")) {
			Configuration hdpCfg = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(fileName), hdpCfg); //FSDataImputStream
			return fs.getFileStatus(new Path(fileName));
		}
		return null;
	}

	/**
	 * Retrieves the HDFS file status
	 * 
	 * @param fileName HDFS file
	 * @return file status or null
	 * @throws IOException ex
	 */
	public static FileStatus[] getChidrenFileStatus(String fileName) throws IOException {
		if (StringUtils.isBlank(fileName)) {
			throw new IllegalArgumentException(
					"Can't create input stream: file name can't be empty.");
		}
		if (fileName.startsWith("hdfs://")) {
			Configuration hdpCfg = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(fileName), hdpCfg); //FSDataImputStream
			return fs.listStatus(new Path(fileName));
		}
		return new FileStatus[]{};
	}
}
