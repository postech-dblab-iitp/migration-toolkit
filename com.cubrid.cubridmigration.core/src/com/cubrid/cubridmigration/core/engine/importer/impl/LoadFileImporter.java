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
package com.cubrid.cubridmigration.core.engine.importer.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.MigrationDirAndFilesManager;
import com.cubrid.cubridmigration.core.engine.MigrationStatusManager;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.task.FileMergeRunnable;
import com.cubrid.cubridmigration.core.engine.task.RunnableResultHandler;
import com.cubrid.cubridmigration.cubrid.Data2StrTranslator;

/**
 * LoadDBImporter : Use LoadDB and CSQL commands to import database objects.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-3 created by Kevin Cao
 */
public class LoadFileImporter extends
		OfflineImporter {
	/**
	 * ExportStatus records the export status
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-3-5 created by Kevin Cao
	 */
	private static class CurrentDataFileInfo {
		String fileHeader;
		String fileFullName;
		int currentFileNO = 1;
		String fileExt;

		public CurrentDataFileInfo(String header, String ext) {
			this.fileHeader = header;
			this.fileExt = ext;
			this.fileFullName = header + ext;
		}

		/**
		 * Create next file.
		 * 
		 */
		public void nextFile() {
			final StringBuffer sb = new StringBuffer(fileHeader);
			currentFileNO++;
			sb.append("_").append(currentFileNO);
			fileFullName = sb.append(fileExt).toString();
			//If has old file ,remove it firstly
			PathUtils.deleteFile(new File(fileFullName));
		}
	}

	protected final static Logger LOGGER = LogUtil.getLogger(LoadFileImporter.class);

	private final Map<String, CurrentDataFileInfo> tableFiles = new HashMap<String, CurrentDataFileInfo>();

	private final Object lockObj = new Object();

	public LoadFileImporter(MigrationContext mrManager) {
		super(mrManager);
		unloadFileUtil = new Data2StrTranslator(mrManager.getDirAndFilesMgr().getMergeFilesDir(),
				config, config.getDestType());
	}

	/**
	 * 
	 * Execute merge file tasks
	 * 
	 * @param sourceFile is file to be read.
	 * @param targetFile specify schema or data
	 * @param listener call back method
	 * @param deleteFile delete file after task finished
	 * @param isSchemaFile true if file is schema file
	 */
	private void executeTask(String sourceFile, String targetFile, RunnableResultHandler listener,
			boolean deleteFile, boolean isSchemaFile) {
		cmTaskService.execute(new FileMergeRunnable(sourceFile, targetFile,
				config.getTargetCharSet(), listener, deleteFile, isSchemaFile
						|| !config.targetIsXLS()));
	}

	/**
	 * Send schema file and data file to server for loadDB command.
	 * 
	 * @param fileName the file to be sent.
	 * @param stc source table configuration.
	 * @param impCount the count of records in file.
	 * @param expCount exported record count
	 */
	protected void handleDataFile(String fileName, final SourceTableConfig stc, final int impCount,
			final int expCount) {
		synchronized (lockObj) {
			MigrationDirAndFilesManager mdfm = mrManager.getDirAndFilesMgr();
			CurrentDataFileInfo es = tableFiles.get(stc.getName());
			if (es == null) {
				final StringBuffer sb = new StringBuffer(
						mrManager.getDirAndFilesMgr().getMergeFilesDir()).append(
						config.getFullTargetFilePrefix()).append(stc.getTarget());
				es = new CurrentDataFileInfo(sb.toString(), config.getDataFileExt());
				PathUtils.deleteFile(new File(es.fileFullName));
				tableFiles.put(stc.getName(), es);
			}
			//If the target file is full. 
			if (mdfm.isDataFileFull(es.fileFullName)) {
				//Full name will be changed.
				es.nextFile();
			}
			final String fileFullName = es.fileFullName;
			mdfm.addDataFile(fileFullName, impCount);
			executeTask(fileName, fileFullName, new RunnableResultHandler() {

				public void success() {
					eventHandler.handleEvent(new ImportRecordsEvent(stc, impCount));
					final MigrationStatusManager sm = mrManager.getStatusMgr();
					sm.addImpCount(stc.getOwner(), stc.getName(), expCount);
					//CSV, XLS file will not be merged into one data file.
					if (config.targetIsCSV() || config.targetIsXLS()) {
						return;
					}
					if (config.isOneTableOneFile()) {
						return;
					}
					final Table st = config.getSrcTableSchema(stc.getOwner(), stc.getName());
					if (null == st) {
						return;
					}
					final long totalEc = sm.getExpCount(stc.getOwner(), stc.getName());
					final long totalIc = sm.getImpCount(stc.getOwner(), stc.getName());
					final boolean expEnd = sm.getExpFlag(stc.getOwner(), stc.getName());
					//If it is the last merging,Merge data files to one data file
					if (expEnd && totalEc == totalIc) {
						executeTask(fileFullName, config.getTargetDataFileName(), null, true, false);
					}
				}

				public void failed(String error) {
					mrManager.getStatusMgr().addImpCount(stc.getOwner(), stc.getName(), expCount);
					eventHandler.handleEvent(new ImportRecordsEvent(stc, impCount,
							new NormalMigrationException(error), null));
				}
			}, config.isDeleteTempFile(), false);
		}
	}

	/**
	 * Send schema file and data file to server for loadDB command.
	 * 
	 * @param fileName the file to be sent.
	 * @param tableName tableName
	 * 
	 */
	protected void sendLOBFile(String fileName, String tableName) {
		///home/cmt/CUBRID/database/mt/log/blob1/blob.xxxxxxx
		String targetFile = getLOBDir(tableName) + new File(fileName).getName();
		//Copy to target if it is local
		try {
			CUBRIDIOUtils.mergeFile(fileName, targetFile);
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}
	}

	/**
	 * Send schema file and data file to server for loadDB command.
	 * 
	 * @param fileName the file to be sent.
	 * @param listener a call interface.
	 * @param isIndex true if the DDL is about index
	 */
	protected void sendSchemaFile(String fileName, RunnableResultHandler listener, boolean isIndex) {
		executeTask(fileName,
				isIndex ? config.getTargetIndexFileName() : config.getTargetSchemaFileName(),
				listener, config.isDeleteTempFile(), true);
	}

	/**
	 * Get lob files directory
	 * 
	 * @param tableName string
	 * @return lob directory
	 */
	protected String getLOBDir(String tableName) {
		return mrManager.getDirAndFilesMgr().getLobFilesDir() + tableName + File.separatorChar;
	}
}
