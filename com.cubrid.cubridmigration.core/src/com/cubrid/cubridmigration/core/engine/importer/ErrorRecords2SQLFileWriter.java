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
package com.cubrid.cubridmigration.core.engine.importer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.math.RandomUtils;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.MigrationDirAndFilesManager;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.exception.UserDefinedHandlerException;
import com.cubrid.cubridmigration.core.engine.task.FileMergeRunnable;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.Data2StrTranslator;

/**
 * Records2SQLFileWriter Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-11-26 created by Kevin Cao
 */
public class ErrorRecords2SQLFileWriter {

	private final MigrationContext mrManager;
	private final Data2StrTranslator dataFileUtils;

	public ErrorRecords2SQLFileWriter(MigrationContext mrManager) {
		this.mrManager = mrManager;
		dataFileUtils = new Data2StrTranslator(
				mrManager.getDirAndFilesMgr().getErrorFilesDir(),
				mrManager.getConfig(), MigrationConfiguration.DEST_SQL);
	}

	/**
	 * write Records to a random file.
	 * 
	 * @param stc SourceTableConfig
	 * @param records List<Record> should be the translated records.
	 * 
	 * @return the file's full name
	 */
	public String writeSQLRecords(SourceTableConfig stc, List<Record> records) {

		ThreadUtils.threadSleep(RandomUtils.nextInt(100), null);
		final MigrationDirAndFilesManager dirAndFilesMgr = mrManager.getDirAndFilesMgr();
		String newTempFile = dirAndFilesMgr.getNewTempFile();
		File file = new File(newTempFile);
		try {
			PathUtils.createFile(file);
		} catch (IOException e) {
			return null;
		}
		writeRecords2SQLFile(file, stc, records);
		//merge to one data file, TODO:if file is full, create new file.
		String totalFile = dirAndFilesMgr.getErrorFilesDir() + stc.getName()
				+ ".sql";
		FileMergeRunnable fmr = new FileMergeRunnable(newTempFile, totalFile,
				mrManager.getConfig().getTargetCharSet(), null, true, true);
		mrManager.getMergeTaskExe().execute(fmr);
		return totalFile;
	}

	/**
	 * Write records to a SQL file
	 * 
	 * @param file File
	 * @param stc SourceTableConfig
	 * @param records records
	 */
	public void writeRecords2SQLFile(File file, SourceTableConfig stc,
			List<Record> records) {
		try {

			final Table tt = mrManager.getConfig().getTargetTableSchema(
					stc.getTarget());
			if (tt == null) {
				return;
			}
			Writer pw = new BufferedWriter(new PrintWriter(file, "utf-8"),
					CUBRIDIOUtils.DEFAULT_MEMORY_CACHE_SIZE);
			try {
				for (Record rc : records) {
					if (rc == null) {
						continue;
					}
					List<String> values = getRecordString(stc, tt, rc);
					if (CollectionUtils.isEmpty(values)) {
						continue;
					}
					StringBuffer sb = new StringBuffer("INSERT INTO \"").append(
							stc.getTarget()).append("\"(");
					boolean isFirst = true;
					for (ColumnValue cv : rc.getColumnValueList()) {
						//Find target column configuration 
						SourceColumnConfig tColCfg = stc.getColumnConfigByTarget(cv.getColumn().getName());
						if (tColCfg == null) {
							continue;
						}
						if (isFirst) {
							isFirst = false;
						} else {
							sb.append(',');
						}
						sb.append('"').append(tColCfg.getTarget()).append('"');
					}
					sb.append(")VALUES(");

					isFirst = true;
					for (String vv : values) {
						if (isFirst) {
							isFirst = false;
						} else {
							sb.append(',');
						}
						sb.append(vv);
					}
					sb.append(");");
					pw.write(sb.toString());
					pw.write("\n");
				}

				pw.flush();
			} finally {
				pw.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Retrieves the string for load DB command of record.
	 * 
	 * @param stc SourceTableConfig
	 * @param tt target Table
	 * @param re source Record
	 * @return string of record
	 */
	protected List<String> getRecordString(SourceTableConfig stc, Table tt,
			Record re) {
		try {
			List<String> dataList = new ArrayList<String>();
			//get target table
			Map<String, Object> recordMap = re.getColumnValueMap();
			for (Record.ColumnValue cv : re.getColumnValueList()) {
				SourceColumnConfig scc = stc.getColumnConfigByTarget(cv.getColumn().getName());
				if (scc == null) {
					return null;
				}
				Column targetColumn = tt.getColumnByName(scc.getTarget());
				if (targetColumn == null) {
					return null;
				}
				DBTransformHelper dbHelper = mrManager.getConfig().getDBTransformHelper();
				Object targetValue;
				try {
					targetValue = dbHelper.convertValueToTargetDBValue(
							mrManager.getConfig(), recordMap, scc,
							cv.getColumn(), targetColumn, cv.getValue());
				} catch (UserDefinedHandlerException ex) {
					targetValue = cv.getValue();
				}
				String fileStr = dataFileUtils.stringValueOf(targetValue,
						targetColumn, null);
				//				if (fileStr.equals('\'')) {
				//					System.out.println();
				//				}
				dataList.add(fileStr);
			}
			return dataList;
		} catch (Exception ex) {
			return null;
		}
	}
}
