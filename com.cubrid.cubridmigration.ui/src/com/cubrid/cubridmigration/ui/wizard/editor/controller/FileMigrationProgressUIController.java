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
package com.cubrid.cubridmigration.ui.wizard.editor.controller;

import java.util.List;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.engine.IMigrationMonitor;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.event.ImportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportSQLsEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;

/**
 * FileMigrationProgressUIController serves CSV and SQL migration.
 * 
 * @author Kevin Cao
 * 
 */
public class FileMigrationProgressUIController extends
		MigrationProgressUIController {

	/**
	 * @return File lists (CSV/SQL)
	 */
	public String[][] getProgressTableInput() {
		if (config.sourceIsSQL()) {
			List<String> sqlFiles = config.getSqlFiles();
			tableItems = new String[sqlFiles.size()][3];
			for (int i = 0; i < sqlFiles.size(); i++) {
				tableItems[i] = new String[] {sqlFiles.get(i), "0", "0"};
			}
			return tableItems;
		}
		if (config.sourceIsCSV()) {
			List<SourceCSVConfig> csvFiles = config.getCSVConfigs();
			tableItems = new String[csvFiles.size()][3];
			for (int i = 0; i < csvFiles.size(); i++) {
				tableItems[i] = new String[] {csvFiles.get(i).getName(), "0", "0"};
			}
			return tableItems;
		}
		return new String[0][0];
	}

	/**
	 * Update the export count
	 * 
	 * @param tableName to be updated
	 * @param exp count
	 * 
	 * @return the row updated
	 */
	public String[] updateTableExpData(String tableName, long exp) {
		if (exp <= 0) {
			return new String[] {};
		}
		for (String[] item : tableItems) {
			if (item[0].equals(tableName)) {
				long newExp = getCellValue(item[1]) + exp;
				item[1] = String.valueOf(newExp);
				return item;
			}
		}
		return new String[] {};
	}

	/**
	 * Update import count of table
	 * 
	 * @param tableName to be updated
	 * @param imp count
	 * 
	 * @return the row updated
	 */
	public String[] updateTableImpData(String tableName, long imp) {
		for (String[] item : tableItems) {
			if (item[0].equals(tableName)) {
				long newImp = getCellValue(item[2]) + imp;
				item[2] = String.valueOf(newImp);
				return item;
			}
		}
		return new String[] {};
	}

	/**
	 * @return 100
	 */
	public int getTotalProgress() {
		return 100;
	}

	private long totalFileSize;
	private long processedFileSize;

	/**
	 * Start migration process
	 * 
	 * @param monitor The migration monitor to handle events
	 * @param startMode start by scheduler or user
	 */
	public void startMigration(IMigrationMonitor monitor, int startMode) {
		processedFileSize = 0;
		calculateTotalFileSize();
		super.startMigration(monitor, startMode);
	}

	/**
	 * Calculate total files' size
	 */
	protected void calculateTotalFileSize() {
		totalFileSize = 1;
		if (config.sourceIsCSV()) {
			List<SourceCSVConfig> files = config.getCSVConfigs();
			for (SourceCSVConfig file : files) {
				totalFileSize = totalFileSize + CUBRIDIOUtils.getFileLength(file.getName());
			}
		} else {
			List<String> sqlfiles = config.getSqlFiles();
			for (String file : sqlfiles) {
				totalFileSize = totalFileSize + CUBRIDIOUtils.getFileLength(file);
			}
		}
	}

	/**
	 * @param event from migration process
	 * @return calculate the every growth of progress of the migration event
	 */
	public int getProgressBarProgressValue(MigrationEvent event) {
		if (event instanceof ImportCSVEvent) {
			ImportCSVEvent ire = (ImportCSVEvent) event;
			processedFileSize = processedFileSize + ire.getSize();
		} else if (event instanceof ImportSQLsEvent) {
			ImportSQLsEvent ire = (ImportSQLsEvent) event;
			processedFileSize = processedFileSize + ire.getSize();
		}
		long onePercent = totalFileSize / 100;
		//If the current size is less then one percent of total, current size will be accumulation
		if (processedFileSize < onePercent) {
			return 0;
		}
		int progress = (int) (processedFileSize / onePercent);
		processedFileSize = processedFileSize % onePercent;
		return progress;
	}
}
