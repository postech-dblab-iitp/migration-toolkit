/*
 * Copyright (C) 2012 Search Solution Corporation. All rights reserved by Search Solution. 
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
package com.cubrid.cubridmigration.command;

import java.io.File;
import java.util.List;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceFKConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceIndexConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceViewConfig;
import com.cubrid.cubridmigration.core.engine.report.DefaultMigrationReporter;

/**
 * MigrationReporter
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-1 created by Kevin Cao
 */
public class ConsoleMigrationReporter extends
		DefaultMigrationReporter {

	public ConsoleMigrationReporter(File file) {
		super(file);
	}

	public ConsoleMigrationReporter(MigrationConfiguration config, int startMode) {
		super(config, startMode);
	}

	/**
	 * Get the config configuration summary for console tool
	 * 
	 */
	protected void getSummary() {
		String lineSeparator = System.getProperty("line.separator");
		String tabSeparator = "\t";
		StringBuilder text = new StringBuilder();

		//source db
		text.append("Source Database :").append(lineSeparator);
		text.append(tabSeparator).append("Type : ");
		if (config.sourceIsOnline()) {
			ConnParameters srcConnParameters = config.getSourceConParams();
			text.append("Online").append(lineSeparator);
			text.append(tabSeparator).append("Host IP :").append(srcConnParameters.getHost()).append(
					lineSeparator);
			text.append(tabSeparator).append("Database name :").append(
					srcConnParameters.getDbName()).append(lineSeparator);
			text.append(tabSeparator).append("Port :").append(srcConnParameters.getPort()).append(
					lineSeparator);
			text.append(tabSeparator).append("User name :").append(srcConnParameters.getConUser()).append(
					lineSeparator);
			text.append(tabSeparator).append("Charset :").append(srcConnParameters.getCharset()).append(
					lineSeparator);

			//cubrid source doesn't read time zone
			text.append(tabSeparator).append("Timezone :");
			if (srcConnParameters.getTimeZone() == null) {
				text.append("Default").append(lineSeparator);
			} else {
				int length = srcConnParameters.getTimeZone().length() > 9 ? 9
						: srcConnParameters.getTimeZone().length();
				text.append(srcConnParameters.getTimeZone().substring(0, length)).append(
						lineSeparator);
			}
		} else if (config.sourceIsSQL()) {
			text.append("SQL").append(lineSeparator);
			text.append("SQL Files List:\r\n");
			List<String> sqls = config.getSqlFiles();
			for (String sql : sqls) {
				text.append(tabSeparator).append(sql).append("\r\n");
			}
		} else if (config.sourceIsCSV()) {
			text.append("CSV").append(lineSeparator);
			text.append("CSV Files List:\r\n");
			List<SourceCSVConfig> csvs = config.getCSVConfigs();
			for (SourceCSVConfig csv : csvs) {
				text.append(tabSeparator).append(csv.getName()).append("  >>  ").append(
						csv.getTarget()).append("\r\n");
			}
		}

		//target db
		text.append("\r\nTarget Database :").append(lineSeparator);
		text.append(tabSeparator).append("Type : ");
		if (config.targetIsOnline()) {
			ConnParameters targetConnParameters = config.getTargetConParams();
			text.append("Online").append(lineSeparator);
			text.append(tabSeparator).append("Host IP :").append(targetConnParameters.getHost()).append(
					lineSeparator);
			text.append(tabSeparator).append("Database name :").append(
					targetConnParameters.getDbName()).append(lineSeparator);
			text.append(tabSeparator).append("Port :").append(targetConnParameters.getPort()).append(
					lineSeparator);
			text.append(tabSeparator).append("Charset :").append(targetConnParameters.getCharset()).append(
					lineSeparator);
			text.append(tabSeparator).append("Timezone :");
			String timeZone = targetConnParameters.getTimeZone();
			timeZone = timeZone == null ? "Default" : timeZone;
			int length = timeZone.length() > 9 ? 9 : timeZone.length();
			text.append(timeZone.substring(0, length)).append(lineSeparator);
		} else if (config.targetIsFile()) {
			text.append("File Repository").append(lineSeparator);
			text.append(tabSeparator).append("Path :").append(config.getFileRepositroyPath()).append(
					lineSeparator);
			text.append(tabSeparator).append("Schema : ").append(config.getTargetSchemaFileName()).append(
					lineSeparator);
			text.append(tabSeparator).append("Index :").append(config.getTargetIndexFileName()).append(
					lineSeparator);
			if (config.isOneTableOneFile()) {
				text.append(tabSeparator).append("Data : One table one file").append(lineSeparator);
			} else {
				text.append(tabSeparator).append("Data :").append(config.getTargetDataFileName()).append(
						lineSeparator);
			}

			text.append(tabSeparator).append("Timezone :");
			int length = config.getTargetFileTimeZone().length() > 9 ? 9
					: config.getTargetFileTimeZone().length();
			text.append(config.getTargetFileTimeZone().substring(0, length)).append(lineSeparator);
		}

		if (config.targetIsGraph()) {
			report.setConfigSummary(text.toString());
			return;
		}
		
		//table
		List<SourceEntryTableConfig> sourceTableConfigList = config.getExpEntryTableCfg();
		if (!sourceTableConfigList.isEmpty()) {
			text.append("Export Tables :").append(lineSeparator);
			for (SourceTableConfig sourceTableConfig : sourceTableConfigList) {
				text.append(tabSeparator).append("source : ").append(sourceTableConfig.getName());
				text.append(tabSeparator).append("    target : ").append(
						sourceTableConfig.getTarget()).append(lineSeparator);
			}
		}

		//view
		List<SourceViewConfig> sourceConfigViewList = config.getExpViewCfg();
		if (!sourceConfigViewList.isEmpty()) {
			text.append("Export Views :").append(lineSeparator);
			for (SourceConfig sourceConfig : sourceConfigViewList) {
				text.append(tabSeparator).append("source : ").append(sourceConfig.getName());
				text.append(tabSeparator).append("target : ").append(sourceConfig.getTarget()).append(
						lineSeparator);
			}
		}

		//sql
		List<SourceSQLTableConfig> sourceSQLTableConfigList = config.getExpSQLCfg();
		if (!sourceSQLTableConfigList.isEmpty()) {
			text.append("SQL Tables : ").append(lineSeparator);
			for (SourceSQLTableConfig sourceSQLTableConfig : sourceSQLTableConfigList) {
				text.append(tabSeparator).append("sql : ").append(sourceSQLTableConfig.getSql());
				text.append(tabSeparator).append("target : ").append(
						sourceSQLTableConfig.getTarget()).append(lineSeparator);
			}
		}

		//FK
		if (config.hasFKExports()) {
			text.append("Export FKs :").append(lineSeparator);
			for (SourceEntryTableConfig sourceEntryTableConfig : config.getExpEntryTableCfg()) {
				List<SourceFKConfig> sourceFKConfigList = sourceEntryTableConfig.getFKConfigList();
				if (!sourceTableConfigList.isEmpty()) {
					for (SourceFKConfig sourceFKConfig : sourceFKConfigList) {
						text.append(tabSeparator).append("source fk : ").append(
								sourceFKConfig.getName());
						text.append(tabSeparator).append("target : ").append(
								sourceFKConfig.getTarget()).append(lineSeparator);
					}
				}
			}
		}

		//index
		if (config.hasIndexExports()) {
			text.append("Export Indexs :").append(lineSeparator);
			for (SourceEntryTableConfig sourceEntryTableConfig : config.getExpEntryTableCfg()) {
				List<SourceIndexConfig> sourceIndexConfigList = sourceEntryTableConfig.getIndexConfigList();
				if (!sourceTableConfigList.isEmpty()) {
					for (SourceIndexConfig sourceIndexConfig : sourceIndexConfigList) {
						text.append(tabSeparator).append("source index : ").append(
								sourceIndexConfig.getName());
						text.append(tabSeparator).append("target : ").append(
								sourceIndexConfig.getTarget()).append(lineSeparator);
					}
				}
			}
		}

		//sequence
		List<SourceSequenceConfig> sourceConfigSequencesList = config.getExpSerialCfg();
		if (!sourceConfigSequencesList.isEmpty()) {
			text.append("Export Serial : ").append(lineSeparator);
			for (SourceConfig sourceConfig : sourceConfigSequencesList) {
				text.append(tabSeparator).append("source serial : ").append(sourceConfig.getName());
				text.append(tabSeparator).append("target : ").append(sourceConfig.getTarget()).append(
						lineSeparator);
			}
		}
		report.setConfigSummary(text.toString());
	}
}
