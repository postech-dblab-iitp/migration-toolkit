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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;

/**
 * CleanDBTask is to clear the database objects which were set to be replaced in
 * target database
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-9-5 created by Kevin Cao
 */
public class UpdateStatisticsTask extends
		ImportTask {

	private final static Logger LOG = LogUtil.getLogger(UpdateStatisticsTask.class);

	private final MigrationConfiguration config;

	public UpdateStatisticsTask(MigrationConfiguration config) {
		this.config = config;
	}

	/**
	 * Retrieves the SQL's about UPDATE STATISTICS ON
	 * <table>
	 * 
	 * @return UPDATE STATISTICS ON SQLs
	 */
	private List<String> getUpdateStatisticsSQLs() {
		List<String> result = new ArrayList<String>();
		if (config.sourceIsSQL()) {
			return result;
		}
		List<String> objectsToBeUpdated = new ArrayList<String>();
		if (config.sourceIsCSV()) {
			List<SourceCSVConfig> csvConfigs = config.getCSVConfigs();
			for (SourceCSVConfig csvf : csvConfigs) {
				objectsToBeUpdated.add(csvf.getTarget());
			}
		} else {
			List<SourceEntryTableConfig> expEntryTableCfg = config.getExpEntryTableCfg();
			for (SourceEntryTableConfig setc : expEntryTableCfg) {
				if (setc.isMigrateData() && !objectsToBeUpdated.contains(setc.getTarget())) {
					objectsToBeUpdated.add(setc.getTarget());
				}
			}
			List<SourceSQLTableConfig> expSQLCfg = config.getExpSQLCfg();
			for (SourceSQLTableConfig sstc : expSQLCfg) {
				if (sstc.isMigrateData() && !objectsToBeUpdated.contains(sstc.getTarget())) {
					objectsToBeUpdated.add(sstc.getTarget());
				}
			}
		}
		for (String target : objectsToBeUpdated) {
			String sql = "UPDATE STATISTICS ON \"" + target + "\"";
			result.add(sql);
		}
		return result;
	}

	/**
	 * Execute import
	 */
	protected void executeImport() {
		if (config.targetIsOnline()) {
			if (config.isUpdateStatistics()) {
				LOG.debug("Execute update statistics for CUBRID");
				execSQLList(getUpdateStatisticsSQLs());
			}
			return;
		}
		String tfile = config.getTargetIndexFileName();
		File file = new File(tfile);
		//if no indexes, return.
		if (!file.exists() || file.length() == 0) {
			return;
		}
		OutputStream os = null; //NO PMD
		try {
			os = new BufferedOutputStream(new FileOutputStream(file, true));
			List<String> sqlList = getUpdateStatisticsSQLs();
			byte[] enterBytes = "\n".getBytes();
			for (String sql : sqlList) {
				os.write(sql.getBytes());
				os.write(enterBytes);
			}
			os.flush();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			Closer.close(os);
		}
	}

	/**
	 * Execute sqls
	 * 
	 * @param sqlList String
	 */
	private void execSQLList(List<String> sqlList) {
		for (String sql : sqlList) {
			try {
				importer.executeDDL(sql);
			} catch (Exception ex) {
				LOG.warn("Execute SQL error:" + sql, ex);
			}
		}
	}
}
