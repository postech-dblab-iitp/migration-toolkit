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
package com.cubrid.cubridmigration.command.handler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import com.cubrid.cubridmigration.command.ConsoleUtils;
import com.cubrid.cubridmigration.core.engine.report.MigrationReportFileUtils;

/**
 * LogCommandHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-1-2 created by Kevin Cao
 */
public class LogCommandHandler extends
		HistoryCommandHandler {

	/**
	 * getPageSize
	 * 
	 * @param args List<String>
	 * @return page size.
	 */
	private int getPageSize(List<String> args) {
		int page;
		String ps = ConsoleUtils.getParameter(args, "-ps");
		if (StringUtils.isBlank(ps)) {
			page = 50;
		} else if (NumberUtils.isNumber(ps)) {
			page = NumberUtils.toInt(ps);
		} else {
			page = 50;
		}
		return page;
	}

	/**
	 * Print migration history's log
	 * 
	 * @param args The parameters input by user
	 * 
	 */
	public void handleCommand(List<String> args) {
		if (CollectionUtils.isEmpty(args)) {
			printHelp();
			printHistoryFiles();
			return;
		}
		int page = getPageSize(args);
		File file = getHistoryFile(args);
		if (file == null) {
			printHistoryFiles();
			return;
		}
		try {
			outPrinter.println();
			outPrinter.println("Reading migration history file: <"
					+ file.getName() + ">");
			outPrinter.println();
			String logFile = MigrationReportFileUtils.extractLog(file.getName());
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(logFile), "utf-8"));
			try {
				String line = reader.readLine();
				int lineNumber = 1;
				while (line != null) {
					outPrinter.println(line);
					lineNumber++;
					if (lineNumber >= page) {
						if (!waitForEnter()) {
							return;
						}
						lineNumber = 1;
					}
					line = reader.readLine();
				}
			} finally {
				reader.close();
			}
		} catch (IOException e) {
			outPrinter.println("Reading migration history file error:"
					+ e.getMessage());
		}
	}

	/**
	 * printHelp
	 * 
	 */
	protected void printHelp() {
		ConsoleUtils.printHelp("/com/cubrid/cubridmigration/command/help_log.txt");
	}
}
