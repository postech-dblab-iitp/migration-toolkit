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

import java.io.File;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.engine.JDBCConManager;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.MigrationDirAndFilesManager;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.event.ImportSQLsEvent;
import com.cubrid.cubridmigration.core.engine.exception.BreakMigrationException;
import com.cubrid.cubridmigration.core.engine.task.FileMergeRunnable;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;

/**
 * SQL Import Task Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-10 created by Kevin Cao
 */
public class SQLImportTask extends
		ImportTask {
	private final List<String> sqls;
	private final long size;
	private MigrationConfiguration config;
	private final String sqlFile;
	private MigrationContext mrManager;

	public void setMrManager(MigrationContext mrManager) {
		this.mrManager = mrManager;
	}

	public void setConfig(MigrationConfiguration config) {
		this.config = config;
	}

	public SQLImportTask(String sqlFile, List<String> sqls, long size) {
		this.sqls = sqls;
		this.size = size;
		this.sqlFile = sqlFile;
	}

	/**
	 * Import foreign key.
	 */
	protected void executeImport() {
		ConnParameters cp = config.getTargetConParams();
		if (cp == null) {
			throw new BreakMigrationException(
					"Target database was not sepecified.");
		}
		try {
			executeSQL(cp);
			eventHandler.handleEvent(new ImportSQLsEvent(sqlFile, sqls.size(),
					size));
		} catch (SQLException ex) {
			String totalFile = null;
			if (config.isWriteErrorRecords()) {
				final MigrationDirAndFilesManager dirAndFilesMgr = mrManager.getDirAndFilesMgr();

				String tempFileName = dirAndFilesMgr.getNewTempFile();
				totalFile = dirAndFilesMgr.getErrorFilesDir()
						+ new File(sqlFile).getName();

				writeErrorRecords(tempFileName, totalFile);
				writeErrorFile(tempFileName, totalFile, ex);
			}
			eventHandler.handleEvent(new ImportSQLsEvent(sqlFile, sqls.size(),
					size, ex, totalFile));
		}
	}

	/**
	 * writeErrorRecords
	 * @param tempFileName
	 * @param totalFileName
	 */
	private void writeErrorRecords(String tempFileName, String totalFileName) {
		String[] errorRecords = sqls.toArray(new String[0]);
		createErrorFile(errorRecords, tempFileName, totalFileName);
	}

	/**
	 * writeErrorFile
	 * @param tempFileName
	 * @param totalFile
	 * @param ex
	 */
	private void writeErrorFile(String tempFileName, String totalFile, SQLException ex) {
		String tempErrorFileName = tempFileName + ".err";
		String totalErrorFile = totalFile + ".err";
		String[] errorMessage = new String[] { createErrorMessage(ex) };
		createErrorFile(errorMessage, tempErrorFileName, totalErrorFile);
	}

	/**
	 * createErrorFile
	 * @param tempFileName
	 * @param contents
	 * @param totalFileName
	 */
	private void createErrorFile(String[] contents, String tempFileName, String totalFileName) {
		File tempFile = new File(tempFileName);
		createTempFile(tempFile, contents);
		if (tempFile != null) {
			mergeFile(tempFileName, totalFileName);
		}
	}

	/**
	 * createTempFile
	 * @param tempFile
	 * @param contents
	 * @return
	 */
	private void createTempFile(File tempFile, String[] contents) {
		try {
			PathUtils.createFile(tempFile);
			CUBRIDIOUtils.writeLines(tempFile, contents);
		} catch (IOException e) {
			tempFile = null;
		}
	}

	/**
	 * mergeFile
	 * @param srcFileName
	 * @param trgFileName
	 */
	private void mergeFile(String srcFileName, String trgFileName) {
	    FileMergeRunnable fmr = new FileMergeRunnable(srcFileName,
				trgFileName, "utf-8", null, true, true);
		mrManager.getMergeTaskExe().execute(fmr);
	}

	/**
	 * createErrorMessage
	 * @param ex
	 * @return
	 */
	private String createErrorMessage(SQLException ex) {
		return new StringBuffer()
		.append("/* ").append("Error code : ")
		.append(ex.getErrorCode())
		.append(" | ")
		.append("Error message : ")
		.append(ex.getMessage().replace(System.getProperty("line.separator"), "").trim())
		.append(" | ")
		.append("SQL : ")
		.append(getFirstErrorSql(ex))
		.append(" */")
		.toString();
	}

	/**
	 * getFirstErrorSql
	 * @param ex
	 * @return
	 */
	private String getFirstErrorSql(SQLException ex) {
		String firstErrorSql = "";
		if (ex instanceof BatchUpdateException) {
			BatchUpdateException bue = (BatchUpdateException) ex;
			int[] updateCounts = bue.getUpdateCounts();
			for (int i = 0; i < updateCounts.length; i++) {
				if (updateCounts[i] == Statement.EXECUTE_FAILED) {
					firstErrorSql = sqls.get(i).trim().toString();
					return firstErrorSql;
				}
			}
		}
		return firstErrorSql;
	}

	/**
	 * Execute sqls and if error raised, it will be tried 3 times.
	 * 
	 * @param cp ConnParameters
	 * @throws SQLException ex
	 */
	private void executeSQL(ConnParameters cp) throws SQLException {
		int iTry = 0;
		while (true) {
			try {
				executeSQLSingle(cp);
				return;
			} catch (SQLException e) {
				if (e.getErrorCode() != -2019 && e.getErrorCode() != -21003
						&& e.getErrorCode() != -2003) {
					throw e;
				}
				//If is broker connection error error code==-2029, it will retry 3 times.
				iTry++;
				if (iTry < 3) {
					ThreadUtils.threadSleep(500, null);
				} else {
					throw e;
				}
			}
		}
	}

	/**
	 * Execute sql once.
	 * 
	 * @param cp ConnParameters
	 * @throws SQLException ex
	 */
	private void executeSQLSingle(ConnParameters cp) throws SQLException {
		Connection con = null;
		Statement stmt = null;
		JDBCConManager connManager = mrManager.getConnManager();
		try {
			con = connManager.getTargetConnection();
			//con.setAutoCommit(false);
			stmt = con.createStatement();
			for (String sql : sqls) {
				stmt.addBatch(sql);
			}
			stmt.executeBatch();
			con.commit();
		} finally {
			Closer.close(stmt);
			connManager.closeTar(con);
		}
	}
}
