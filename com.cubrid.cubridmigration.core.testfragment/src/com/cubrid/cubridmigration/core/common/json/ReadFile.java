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
package com.cubrid.cubridmigration.core.common.json;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.cubrid.cubridmigration.core.common.Closer;

public class ReadFile {

	public static String getMySQLExportOptionJSon() {
		String path = getTestFilePath();
		String file = path + "exportOption.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}

	public static String getOraExportOptionJson() {
		String path = getTestFilePath();
		String file = path + "ora_exportOption.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}
	
	public static String getCubExportOptionJson() {
		String path = getTestFilePath();
		String file = path + "cub_exportOption.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}
	
	public static String getSQLServerExportOptionJson() {
		String path = getTestFilePath();
		String file = path + "mssql_exportOption.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}

	public static String getMySQLImportOptionJson() {
		String path = getTestFilePath();
		String file = path + "importOption.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}

	public static String getOraImportOptionJson() {
		String path = getTestFilePath();
		String file = path + "ora_importOption.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}
	public static String getCubImportOptionJson() {
		String path = getTestFilePath();
		String file = path + "cub_importOption.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}

	public static String getMySQLDBIDMappingJson() {
		String path = getTestFilePath();
		String file = path + "dbIDMapper.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}

	public static String getOraDBIDMappingJson() {
		String path = getTestFilePath();
		String file = path + "ora_dbIDMapper.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}
	
	public static String getCubDBIDMappingJson() {
		String path = getTestFilePath();
		String file = path + "cub_dbIDMapper.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}

	public static String getMysqlMonitorJson() {
		String path = getTestFilePath();
		String file = path + "monitor.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}

	public static String getOraMonitorJson() {
		String path = getTestFilePath();
		String file = path + "ora_monitor.txt";
		try {
			return readFile(file);
		} catch (IOException e) {
			return null;
		}
	}

	private static String getTestFilePath() {
		String path = System.getProperty("user.dir")
				+ "/src/com/cubrid/cubridmigration/core/common/json/";
		return path;
	}

	public static String readFile(String file) throws IOException {
		BufferedReader in = null;
		StringBuffer buf = new StringBuffer();
		try {
			in = new BufferedReader(new FileReader(new File(file)));

			String line;
			while (null != (line = in.readLine())) {
				buf.append(line).append("\n");
			}
			return buf.toString();
		} finally {
			Closer.close(in);
		}
	}
}
