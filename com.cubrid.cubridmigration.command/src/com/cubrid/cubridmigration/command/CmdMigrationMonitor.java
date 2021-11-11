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
import java.io.PrintStream;
import java.util.List;

import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.IMigrationMonitor;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.event.CreateObjectEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportSQLsEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationFinishedEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationStartEvent;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;

/**
 * CommandMigrationMonitor Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-2-2 created by Kevin Cao
 */
public class CmdMigrationMonitor implements
		IMigrationMonitor {
	private long totalProgress = 0;
	private long currentProgress = 0;
	private long progress = 0;
	private MigrationFinishedEvent finalEvent = null;
	//private int circle = 0;
	private boolean hasError;
	private final int monitorMode;
	private PrintStream outPrinter = System.out;

	public CmdMigrationMonitor(MigrationConfiguration config, int monitorMode) {
		if (config.sourceIsOnline() || config.sourceIsXMLDump()) {
			List<SourceEntryTableConfig> tables = config.getExpEntryTableCfg();
			for (SourceEntryTableConfig tbl : tables) {
				Table table = config.getSrcTableSchema(tbl.getOwner(),
						tbl.getName());
				if (tbl.isCreatePK() && table.getPk() != null) {
					totalProgress++;
				}
				totalProgress = totalProgress + table.getTableRowCount();
			}
			List<SourceSQLTableConfig> sqlTables = config.getExpSQLCfg();
			for (SourceSQLTableConfig tbl : sqlTables) {
				Table table = config.getSrcTableSchema(tbl.getOwner(),
						tbl.getName());
				totalProgress = totalProgress
						+ (table == null ? 0 : table.getTableRowCount());
			}
			totalProgress = totalProgress + config.getExpObjCount();
		} else if (config.sourceIsSQL()) {
			for (String ss : config.getSqlFiles()) {
				totalProgress = totalProgress + new File(ss).length();
			}
		} else if (config.sourceIsCSV()) {
			for (SourceCSVConfig scc : config.getCSVConfigs()) {
				totalProgress = totalProgress
						+ new File(scc.getName()).length();
			}
		}
		this.monitorMode = monitorMode;
		hasError = false;
	}

	/**
	 * Print finished message.
	 */
	public void finished() {
	}

	/**
	 * Print started message.
	 */
	public void start() {
	}

	/**
	 * Print event message.
	 * 
	 * @param event MigrationEvent
	 */
	public void addEvent(MigrationEvent event) {
		if (finalEvent != null) {
			return;
		}

		if (event instanceof MigrationStartEvent) {
			outPrinter.println(event.toString());
			return;
		}

		if (event instanceof MigrationFinishedEvent) {
			finalEvent = (MigrationFinishedEvent) event;
			print('\b', String.valueOf(progress).length() + 2);
			outPrinter.print("100%");
			outPrinter.println();
			if (hasError) {
				outPrinter.println("Some errors occurred during migration.");
				outPrinter.println("Please see the report for more.");
			}
			outPrinter.println(event.toString());
			return;
		}

		boolean isError = false;
		if (event instanceof CreateObjectEvent) {
			CreateObjectEvent ev = (CreateObjectEvent) event;
			if (ev.isSuccess()) {
				currentProgress++;
			} else {
				isError = true;
			}
		} else if (event instanceof ImportRecordsEvent) {
			final ImportRecordsEvent importRecordsEvent = (ImportRecordsEvent) event;
			if (importRecordsEvent.isSuccess()) {
				currentProgress = currentProgress
						+ importRecordsEvent.getRecordCount();
			} else {
				isError = true;
			}
		} else if (event instanceof ImportSQLsEvent) {
			ImportSQLsEvent ev = (ImportSQLsEvent) event;
			currentProgress = currentProgress + ev.getSize();
			if (!ev.isSuccess()) {
				isError = true;
			}
		} else if (event instanceof ImportCSVEvent) {
			ImportCSVEvent ev = (ImportCSVEvent) event;
			currentProgress = currentProgress + ev.getSize();
			if (!ev.isSuccess()) {
				isError = true;
			}
		}
		hasError = isError;
		boolean isNewLine = false;
		if (event.getLevel() <= monitorMode) {
			outPrinter.println(CUBRIDTimeUtil.defaultFormatMilin(event.getEventTime())
					+ " " + event.toString());
			isNewLine = true;
		}
		if (monitorMode <= MigrationConfiguration.RPT_LEVEL_ERROR
				&& (totalProgress > 0)) {
			//print progress
			long tmpPro = currentProgress * 100 / totalProgress;
			tmpPro = tmpPro == 0 ? 1 : tmpPro;
			progress = tmpPro;
			if (!isNewLine) {
				print('\b', String.valueOf(tmpPro).length() + 2);
			}
			outPrinter.print(progress + "%");
		}
	}

	/**
	 * Print chars on screen.
	 * 
	 * @param ch char to be printed
	 * @param count repeat count
	 */
	private void print(char ch, int count) {
		for (int i = 0; i < count; i++) {
			outPrinter.print(ch);
		}
	}
}
