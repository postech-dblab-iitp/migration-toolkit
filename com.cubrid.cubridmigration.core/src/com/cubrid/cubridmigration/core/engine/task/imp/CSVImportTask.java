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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.JDBCConManager;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.event.ImportCSVEvent;
import com.cubrid.cubridmigration.core.engine.task.FileMergeRunnable;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;
import com.cubrid.cubridmigration.cubrid.Data2StrTranslator;

/**
 * SQL Import Task Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-10 created by Kevin Cao
 */
public class CSVImportTask extends
		ImportTask {
	private final static Logger LOG = LogUtil.getLogger(CSVImportTask.class);

	private final List<String[]> data;
	private final long size;
	private final SourceCSVConfig csv;
	private MigrationContext mrManager;

	private List<SourceCSVColumnConfig> columnConfigs;
	private List<String> nullStrings;

	public CSVImportTask(SourceCSVConfig csv, List<String[]> data, long size) {
		this.data = data;
		this.size = size;
		this.csv = csv;
		columnConfigs = csv.getColumnConfigs();
	}

	/**
	 * Set MigrationResourceManager
	 * 
	 * @param mrManager MigrationResourceManager
	 */
	public void setMrManager(MigrationContext mrManager) {
		this.mrManager = mrManager;
		nullStrings = mrManager.getConfig().getCsvSettings().getNullStrings();
	}

	/**
	 * Import foreign key.
	 */
	protected void executeImport() {
		Exception error = null;
		List<String[]> errorRecords = null;
		try {
			errorRecords = execute();
			if (!CollectionUtils.isEmpty(errorRecords)) {
				throw new SQLException("Insert CSV data error");
			}
			eventHandler.handleEvent(new ImportCSVEvent(csv, data.size(), size));
		} catch (SQLException ex) {
			error = ex;
		}
		//If no errors, return
		if (error == null) {
			return;
		}
		errorRecords = errorRecords == null ? data : errorRecords;
		File file = writeError2File(errorRecords);
		if (errorRecords.size() < data.size()) {
			eventHandler.handleEvent(new ImportCSVEvent(csv, data.size()
					- errorRecords.size(), 0));
		}
		eventHandler.handleEvent(new ImportCSVEvent(csv, errorRecords.size(),
				size, error, file == null ? null : file.getAbsolutePath()));
	}

	/**
	 * Write error records to file.
	 * 
	 * @param errorRecords List<String[]>
	 * @return the file has been written.
	 */
	private File writeError2File(List<String[]> errorRecords) {
		if (!mrManager.getConfig().isWriteErrorRecords()) {
			return null;
		}
		String tempFileName = mrManager.getDirAndFilesMgr().getNewTempFile();
		String totalFile = mrManager.getDirAndFilesMgr().getErrorFilesDir()
				+ new File(csv.getName()).getName() + ".sql";
		File file = new File(tempFileName);
		try {
			PathUtils.createFile(file);
			String[] lines = new String[errorRecords.size()];
			int i = 0;
			StringBuffer sbColumns = new StringBuffer();
			sbColumns.append("insert into \"");
			sbColumns.append(csv.getTarget()).append("\"(");
			for (SourceCSVColumnConfig sccc : columnConfigs) {
				if (!sccc.isCreate()) {
					continue;
				}
				sbColumns.append("\"").append(sccc.getTarget()).append("\",");
			}
			sbColumns.deleteCharAt(sbColumns.length() - 1).append(")values(");
			for (String[] dd : errorRecords) {
				StringBuffer sb = new StringBuffer();
				for (int j = 0; j < dd.length; j++) {
					if (!columnConfigs.get(j).isCreate()) {
						continue;
					}
					String value = dd[j];
					if (sb.length() > 0) {
						sb.append(",");
					}
					sb.append(Data2StrTranslator.quoteString(value));
				}
				lines[i] = sbColumns.toString() + sb.toString() + ");";
				i++;
			}
			CUBRIDIOUtils.writeLines(file, lines);
			FileMergeRunnable fmr = new FileMergeRunnable(tempFileName,
					totalFile, "utf8", null, true, true);
			mrManager.getMergeTaskExe().execute(fmr);
		} catch (IOException ex1) {
			LOG.error("Write file error", ex1);
		}
		return new File(totalFile);
	}

	/**
	 * Execute sqls and if error raised, it will be tried 3 times.
	 * 
	 * @return successfully inserted count
	 * @throws SQLException ex
	 */
	private List<String[]> execute() throws SQLException {
		int iTry = 0;
		int result = 0;
		while (true) {
			try {
				result = batchExecute();
				break;
			} catch (SQLException e) {
				//Only connection error will be thrown here.
				//If is broker connection error error code==-2029, it will retry 3 times.
				iTry++;
				if (iTry > 3) {
					throw e; //Throw connection error after tried 3 times.
				}
				ThreadUtils.threadSleep(500, null);
			}
		}
		//Other errors, try to execute one by one
		if (result == 0) {
			return executeOneByOne();
		}
		return null;
	}

	/**
	 * If the error is about connection.
	 * 
	 * @param ex Error
	 * @return true if it is not an error about connection failed.
	 */
	private boolean isConnectionError(SQLException ex) {
		return !(ex.getErrorCode() != -2019 && ex.getErrorCode() != -21003 && ex.getErrorCode() != -2003);
	}

	/**
	 * Insert the record one by one.
	 * 
	 * @return count successfully inserted
	 * @throws SQLException ex
	 */
	private List<String[]> executeOneByOne() throws SQLException {
		final JDBCConManager connManager = mrManager.getConnManager();
		Connection con = null;
		PreparedStatement stmt = null;
		try {
			con = connManager.getTargetConnection();
			stmt = con.prepareStatement(getPreparedSQL());

			final List<String[]> result = new ArrayList<String[]>();
			for (String[] dd : data) {
				int parameterIndex = 1;
				for (int i = 0; i < dd.length; i++) {
					if (!columnConfigs.get(i).isCreate()) {
						continue;
					}
					String value = dd[i];
					if (nullStrings.indexOf(value) >= 0) {
						stmt.setNull(parameterIndex, Types.NULL);
					} else {
						stmt.setString(parameterIndex, value);
					}
					parameterIndex++;
				}
				try {
					stmt.execute();
					con.commit();
				} catch (SQLException ex) {
					//If connection is cut down, reconnect it.
					if (isConnectionError(ex)) {
						LOG.error("Connection lost.", ex);
						Closer.close(stmt);
						Closer.close(con);
						con = connManager.getTargetConnection();
						stmt = con.prepareStatement(getPreparedSQL());
					} else {
						LOG.error("Insert CSV data error.", ex);
						con.rollback();
					}
					result.add(dd);
				}
			}
			return result;
		} finally {
			Closer.close(stmt);
			Closer.close(con);
		}
	}

	/**
	 * Execute sql once.
	 * 
	 * @return succeed count
	 * @throws SQLException ex
	 */
	private int batchExecute() throws SQLException {
		Connection con = null;
		PreparedStatement stmt = null;
		JDBCConManager connManager = mrManager.getConnManager();
		try {
			con = connManager.getTargetConnection();
			try {
				stmt = con.prepareStatement(getPreparedSQL());
				for (String[] dd : data) {
					int parameterIndex = 1;
					for (int i = 0; i < dd.length; i++) {
						if (!columnConfigs.get(i).isCreate()) {
							continue;
						}
						String value = dd[i];
						if (nullStrings.indexOf(value) >= 0) {
							stmt.setNull(parameterIndex, Types.NULL);
						} else {
							stmt.setString(parameterIndex, value);
						}
						parameterIndex++;
					}
					stmt.addBatch();
				}

				int[] results = stmt.executeBatch();
				int rst = 0;
				for (int i : results) {
					rst = rst + i;
				}
				con.commit();
				return rst;
			} catch (SQLException ex) {
				con.rollback();
				//Only connection error will be thrown out.
				if (isConnectionError(ex)) {
					throw ex;
				}
				return 0;
			}
		} finally {
			Closer.close(stmt);
			connManager.closeTar(con);
		}
	}

	/**
	 * Retrieves the prepares statment's SQL
	 * 
	 * @return SQL
	 */
	private String getPreparedSQL() {
		StringBuffer sb = new StringBuffer("insert into \"");
		sb.append(csv.getTarget()).append("\"(");

		int count = 0;
		boolean first = true;
		for (SourceCSVColumnConfig sccc : columnConfigs) {
			if (sccc.isCreate()) {
				if (!first) {
					sb.append(",");
				}
				sb.append("\"").append(sccc.getTarget()).append("\"");
				first = false;
				count++;
			}
		}
		sb.append(")values(");
		for (int i = 0; i < count; i++) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append("?");
		}
		sb.append(")");
		return sb.toString();
	}
}
