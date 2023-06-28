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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceFKConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceIndexConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;

/**
 * CleanDBTask is to clear the database objects which were set to be replaced in
 * target database
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-9-5 created by Kevin Cao
 */
public class CleanDBTask extends
		ImportTask {

	private final static Logger LOG = LogUtil.getLogger(CleanDBTask.class);

	private final MigrationConfiguration config;

	public CleanDBTask(MigrationConfiguration config) {
		this.config = config;
	}

	/**
	 * Execute import
	 */
	protected void executeImport() {
		List<String> sb = new ArrayList<String>();
		for (SourceEntryTableConfig setc : config.getExpEntryTableCfg()) {
			if (!setc.isCreateNewTable()) {
				continue;
			}
			for (SourceFKConfig sfkc : setc.getFKConfigList()) {
				if (sfkc.isCreate() && sfkc.isReplace()) {
					String sql = "ALTER TABLE \"" + setc.getTarget()
							+ "\" DROP CONSTRAINT \"" + sfkc.getTarget() + "\"";
					sb.add(sql + ";");
					execDDL(sql);
				}
			}
			for (SourceIndexConfig idx : setc.getIndexConfigList()) {
				if (idx.isCreate() && idx.isReplace()) {
					String sql = "ALTER TABLE \"" + setc.getTarget()
							+ "\" DROP CONSTRAINT \"" + idx.getTarget() + "\"";
					sb.add(sql + ";");
					execDDL(sql);
				}
			}
		}
		for (SourceEntryTableConfig setc : config.getExpEntryTableCfg()) {
			if (setc.isCreateNewTable() && setc.isReplace()) {
				String sql = "DROP TABLE \"" + setc.getTarget() + "\"";
				sb.add(sql + ";");
				execDDL(sql);
			}
		}
		for (SourceSQLTableConfig sstc : config.getExpSQLCfg()) {
			if (sstc.isCreateNewTable() && sstc.isReplace()) {
				String sql = "DROP TABLE \"" + sstc.getTarget() + "\"";
				sb.add(sql + ";");
				execDDL(sql);
			}
		}
		for (SourceCSVConfig scc : config.getCSVConfigs()) {
			if (scc.isCreate() && scc.isReplace()) {
				String sql = "DROP TABLE \"" + scc.getTarget() + "\"";
				sb.add(sql + ";");
				execDDL(sql);
			}
		}
		for (SourceConfig sc : config.getExpViewCfg()) {
			if (sc.isCreate() && sc.isReplace()) {
				String sql = "DROP VIEW \"" + sc.getTarget() + "\"";
				sb.add(sql + ";");
				execDDL(sql);
			}
		}
		for (SourceConfig sc : config.getExpSerialCfg()) {
			if (sc.isCreate() && sc.isReplace()) {
				String sql = "DROP SERIAL \"" + sc.getTarget() + "\"";
				sb.add(sql + ";");
				execDDL(sql);
			}
		}
//		if (config.targetIsFile() && !sb.isEmpty()) {
//			File clearFile = new File(config.getFileRepositroyPath()
//					+ File.separator + "clear_"
//					+ config.getSrcCatalog().getName() + ".sql");
//			try {
//				PathUtils.deleteFile(clearFile);
//				PathUtils.createFile(clearFile);
//				CUBRIDIOUtils.writeLines(clearFile, sb.toArray(new String[]{}),
//						config.getTargetCharSet());
//			} catch (IOException e) {
//				LOG.error("", e);
//			}
//		}
	}

	/**
	 * Execute DDL
	 * 
	 * @param sql String
	 */
	private void execDDL(String sql) {
		if (config.targetIsFile()) {
			return;
		}
		try {
			importer.executeDDL(sql);
		} catch (Exception ex) {
			LOG.warn("Clearn SQL:" + sql, ex);
		}
	}
}
