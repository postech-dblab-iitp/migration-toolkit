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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * SQLParser Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-2-17 created by Kevin Cao
 */
public class SQLParser {

	/**
	 * Execute export operation, parsing SQL file and call importing SQLs
	 * 
	 * @param sqlFile SQL file
	 * @param encoding file char-set
	 * @param commitCount commit count
	 * @param callBack call back interface
	 * @throws IOException ex
	 */
	public static void executeSQLFile(String sqlFile, String encoding,
			int commitCount, ISQLParsingCallback callBack) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(new File(sqlFile)), encoding));
		executeSQLFile(reader, commitCount, callBack);
	}

	/**
	 * Execute export operation, parsing SQL file and call importing SQLs
	 * 
	 * @param reader BufferedReader
	 * @param commitCount commit count
	 * @param callBack call back interface
	 * @throws IOException ex
	 */
	public static void executeSQLFile(Reader reader, int commitCount,
			ISQLParsingCallback callBack) throws IOException {
		StringBuffer sql = new StringBuffer();
		try {
			boolean inQuote = false;
			boolean inComment = false;
			boolean inLineComment = false;
			long size = 0;
			char[] cbuf = new char[1];
			Character char2 = null;
			int count = reader.read(cbuf);
			List<String> sqlList = new ArrayList<String>(commitCount);
			while (count > 0) {
				size++;
				char ch = cbuf[0];
				switch (ch) {
				case '\'':
					if (inQuote) {
						inQuote = false;
					} else {
						inQuote = !inComment && !inLineComment && true;
					}
					break;
				case '/':
					count = reader.read(cbuf);
					if (count < 0) {
						break;
					}
					char2 = cbuf[0];
					if (cbuf[0] == '*') {
						inComment = !inQuote && !inLineComment && true;
					} else if (cbuf[0] == '\'') {
						if (inQuote) {
							inQuote = false;
						} else {
							inQuote = !inComment && !inLineComment && true;
						}
					}
					break;
				case '*':
					count = reader.read(cbuf);
					if (count < 0) {
						break;
					}
					char2 = cbuf[0];
					if (cbuf[0] == '/') {
						inComment = false;
					} else if (cbuf[0] == '\'') {
						if (inQuote) {
							inQuote = false;
						} else {
							inQuote = !inComment && !inLineComment && true;
						}
					}
					break;
				case '-':
					count = reader.read(cbuf);
					if (count < 0) {
						break;
					}
					char2 = cbuf[0];
					if (cbuf[0] == '-') {
						inLineComment = !inQuote && !inComment && true;
					} else if (cbuf[0] == '\'') {
						if (inQuote) {
							inQuote = false;
						} else {
							inQuote = !inComment && !inLineComment && true;
						}
					}
					break;
				case '\n':
					inLineComment = false;
					break;
				default:

				}
				//Break loop
				if (count < 0) {
					sql.append(ch);
					sqlList.add(sql.toString());
					sql = new StringBuffer();
					break;
				}
				sql.append(ch);
				if (char2 != null) {
					sql.append(char2);
					char2 = null;
				}
				//Ignore the comment chars.
				if (inComment || inLineComment) {
					count = reader.read(cbuf);
					continue;
				}
				if (ch == ';') {
					if (!inQuote) {
						//the ; in '' will be ignored
						sqlList.add(sql.toString());
						sql = new StringBuffer();
						if (callBack.isCommitNow(sqlList.size())) {
							callBack.executeSQLs(sqlList, size);
							size = 0;
							//New list, old list will be used by import task.
							sqlList = new ArrayList<String>(commitCount);
						}
					}

				}
				count = reader.read(cbuf);
			}
			if (StringUtils.isNotBlank(sql.toString())) {
				sqlList.add(sql.toString());
			}
			if (sqlList.size() > 0) {
				callBack.executeSQLs(sqlList, size);
			}
		} finally {
			reader.close();
		}

	}

	/**
	 * ISQLParsingCallback Description
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-2-17 created by Kevin Cao
	 */
	public static interface ISQLParsingCallback {

		/**
		 * Execute SQL list
		 * 
		 * @param sqlList List<String>
		 * @param size long
		 */
		public void executeSQLs(List<String> sqlList, long size);

		/**
		 * Control if commit now.
		 * 
		 * @param sqlsSize sqls count
		 * @return true if commit now, false will be continue parsing
		 */
		public boolean isCommitNow(int sqlsSize);
	}
}
