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
package com.cubrid.cubridmigration.core.engine;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.exception.BreakMigrationException;

/**
 * 
 * MigrationDirAndFilesManager provides the directories and file names in the
 * migration process.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-6-25 created by Kevin Cao
 */
public class MigrationDirAndFilesManager implements
		ICanDispose {

	/**
	 * DataFileInfo reserves data file name and how many records are in the file
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-6-27 created by Kevin Cao
	 */
	static class DataFileInfo {
		private long recordCount;
		private final String fileName;

		DataFileInfo(String fileName) {
			this.fileName = fileName;
		}

		public long getRecordCount() {
			return recordCount;
		}

		/**
		 * Add recount count
		 * 
		 * @param recordCount to be added
		 */
		private void addRecordCount(long recordCount) {
			this.recordCount = this.recordCount + recordCount;
		}

		public String getFileName() {
			return fileName;
		}
	}

	private final MigrationConfiguration config;

	private String privateTempDir = null;
	private String privateErrorDir = null;
	private String mergeFilesDir = null;
	private String lobFilesDir = null;

	private final Map<String, DataFileInfo> dataFiles = new HashMap<String, DataFileInfo>();

	public MigrationDirAndFilesManager(MigrationConfiguration config) {
		this.config = config;
	}

	/**
	 * Initialize before migration started.
	 * 
	 */
	public void initialize() {
		//Temporary dir
		long cmtTimeStamp = System.currentTimeMillis();
		String tempFilePath = PathUtils.getBaseTempDir();
		if (StringUtils.isEmpty(tempFilePath)) {
			tempFilePath = PathUtils.getBaseTempDir() + File.separatorChar + cmtTimeStamp
					+ File.separatorChar;
		}
		privateTempDir = tempFilePath;
		File tempDir = new File(privateTempDir);
		if (!tempDir.exists() && !tempDir.mkdirs()) {
			throw new BreakMigrationException("Invalid path:" + privateTempDir);
		}

		//Error dir
		privateErrorDir = PathUtils.getErrorsDir() + cmtTimeStamp + File.separator;
		File errorDir = new File(privateErrorDir);
		if (!errorDir.exists() && !errorDir.mkdirs()) {
			throw new BreakMigrationException("Invalid path:" + privateErrorDir);
		}
		privateErrorDir = errorDir.getAbsolutePath() + File.separator;
		//mergeFilesDir
		if (config.targetIsFile()) {
			mergeFilesDir = config.getFileRepositroyPath();
		} else {
			mergeFilesDir = privateTempDir + "cache" + File.separatorChar;
		}
		File mergeDir = new File(mergeFilesDir);
		if (!mergeDir.exists() && !mergeDir.mkdirs()) {
			throw new BreakMigrationException("Invalid path:" + mergeFilesDir);
		}

		if (!privateTempDir.endsWith(File.separatorChar + "")) {
			privateTempDir = privateTempDir + File.separatorChar;
		}
		if (!privateErrorDir.endsWith(File.separatorChar + "")) {
			privateErrorDir = privateErrorDir + File.separatorChar;
		}
		if (!mergeFilesDir.endsWith(File.separatorChar + "")) {
			mergeFilesDir = mergeFilesDir + File.separatorChar;
		}

		//lobFilesDir
		lobFilesDir = mergeFilesDir + "lob" + File.separatorChar;
		File lobDir = new File(lobFilesDir);
		if (!lobDir.exists() && !lobDir.mkdirs()) {
			throw new BreakMigrationException("Invalid path:" + lobDir);
		}
	}

	/**
	 * One migration process has his private temporary directory.
	 * 
	 * @return full path with File.separatorChar ended
	 */
	public String getPrivateTempDir() {
		if (StringUtils.isEmpty(privateTempDir)) {
			throw new BreakMigrationException("Path has not been initialized");
		}
		return privateTempDir;
	}

	/**
	 * Retrieves the error files' directory.
	 * 
	 * @return directory with separator
	 */
	public String getErrorFilesDir() {
		if (StringUtils.isEmpty(privateErrorDir)) {
			throw new BreakMigrationException("Path has not been initialized");
		}
		return privateErrorDir;
	}

	/**
	 * Directory merge temporary data file and schema file. If target is local
	 * files, the returned value is output directory, if target is offline, the
	 * returned value is a temporary path.
	 * 
	 * @return MergeFilesDir
	 */
	public String getMergeFilesDir() {
		if (StringUtils.isEmpty(mergeFilesDir)) {
			throw new BreakMigrationException("Path has not been initialized");
		}
		return mergeFilesDir;
	}

	/**
	 * Retrieves a New Temporary File
	 * 
	 * @return full file name
	 */
	public String getNewTempFile() {
		return privateTempDir + UUID.randomUUID().toString();
	}

	/**
	 * Full name of schema file
	 * 
	 * @return Full name of schema file
	 */
	public String getSchemaFile() {
		String ss = "schema";
		if (config.targetIsFile()) {
			ss = config.getTargetFilePrefix() + "_schema";
		}
		return mergeFilesDir + ss;
	}

	/**
	 * Full name of index file name
	 * 
	 * @return Full name of index file name
	 */
	public String getIndexFile() {
		String ss = "index";
		if (config.targetIsDBDump()) {
			ss = config.getTargetFilePrefix() + "_schema";
		} else if (config.targetIsFile()) {
			return getSchemaFile();
		}
		return mergeFilesDir + ss;
	}

	/**
	 * Directory to save lob files
	 * 
	 * @return Directory to save lob files
	 */
	public String getLobFilesDir() {
		if (StringUtils.isEmpty(lobFilesDir)) {
			throw new BreakMigrationException("Path has not been initialized");
		}
		return lobFilesDir;
	}

	public String getGraphListFile() {
		String ss = "list";
		if (config.targetIsFile()) {
			ss = config.getTargetFilePrefix() + "_list";
		}
		return mergeFilesDir + ss;
	}
	
	/**
	 * Add a data file to manager
	 * 
	 * @param fileName String
	 * @param recordCount long
	 */
	public void addDataFile(String fileName, long recordCount) {
		synchronized (MigrationDirAndFilesManager.class) {
			DataFileInfo dfi = dataFiles.get(fileName);
			if (dfi == null) {
				dfi = new DataFileInfo(fileName);
				dataFiles.put(fileName, dfi);
			}
			dfi.addRecordCount(recordCount);
		}
	}

	/**
	 * Check if the data file is full
	 * 
	 * @param fileName String
	 * @return true if full.
	 */
	public boolean isDataFileFull(String fileName) {
		if (!config.isOneTableOneFile() || config.getMaxCountPerFile() <= 0) {
			return false;
		}
		synchronized (MigrationDirAndFilesManager.class) {
			DataFileInfo dfi = dataFiles.get(fileName);
			if (dfi == null) {
				return false;
			}
			return dfi.getRecordCount() >= config.getMaxCountPerFile();
		}
	}

	//	public String getDataFile() {
	//		if (config.targetIsFile()
	//				&& !config.isOneTableOneFile()) {
	//		}
	//		if (config.targetIsOffline()) {
	//
	//		} else if (config.targetIsDBDump()) {
	//
	//		} else if (config.targetIsCSV()) {
	//
	//		} else if (config.targetIsSQL()) {
	//
	//		} else if (config.targetIsXLS()) {
	//
	//		}
	//
	//		return "";
	//	}

	/**
	 * Dispose, remove useless directories.
	 */
	public void dispose() {
		//Remove privateTempDir
		CUBRIDIOUtils.clearFileOrDir(privateTempDir);
		//Remove privateErrorDir 
		File errorDir = new File(privateErrorDir);
		final String[] list = errorDir.list();
		if (list == null || list.length == 0) {
			CUBRIDIOUtils.clearFileOrDir(errorDir);
		}
		//Remove lobFilesDir;
		File lobDir = new File(lobFilesDir);
		final String[] listLOB = lobDir.list();
		if (listLOB == null || listLOB.length == 0) {
			CUBRIDIOUtils.clearFileOrDir(lobDir);
		}
	}
}