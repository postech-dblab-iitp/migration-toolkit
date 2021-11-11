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
package com.cubrid.cubridmigration.core.engine.task.imp;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;

import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;
import com.cubrid.cubridmigration.core.io.SQLParser;

/**
 * SQL Import Task Description
 * 
 * @author Kevin Cao
 */
public class ExecuteSQLTask extends
		ImportTask {
	private static final int COMMIT_COUNT = 1;
	//TODO: stc not used.
	protected final SourceTableConfig stc;
	private final String sql;

	public ExecuteSQLTask(SourceEntryTableConfig setc, String sql) {
		this.stc = setc;
		this.sql = sql;
	}

	/**
	 * Import foreign key.
	 */
	protected void executeImport() {
		try {
			//Standardization SQL: trim and auto append ';'
			String cleansql = sql.trim();
			if (!cleansql.endsWith(";")) {
				cleansql = cleansql + ";";
			}
			BufferedReader reader = new BufferedReader(new StringReader(cleansql));

			try {
				SQLParser.ISQLParsingCallback callBack = new SQLParser.ISQLParsingCallback() {

					public void executeSQLs(List<String> sqlList, long size) {
						for (String ss : sqlList) {
							try {
								importer.executeDDL(ss);
							} catch (Exception ex) {
								eventHandler.handleEvent(new MigrationErrorEvent(
										new NormalMigrationException(ex)));
							}
						}
					}

					public boolean isCommitNow(int sqlsSize) {
						return sqlsSize >= COMMIT_COUNT;
					}
				};
				SQLParser.executeSQLFile(reader, COMMIT_COUNT, callBack);
			} finally {
				reader.close();
			}
		} catch (NormalMigrationException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new NormalMigrationException(ex);
		}
	}
}
