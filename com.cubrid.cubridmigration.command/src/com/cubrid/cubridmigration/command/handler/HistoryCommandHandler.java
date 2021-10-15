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

import java.io.Console;
import java.io.File;
import java.io.PrintStream;
import java.util.List;

import com.cubrid.cubridmigration.command.ConsoleCommandHandler;
import com.cubrid.cubridmigration.core.common.PathUtils;

/**
 * LogCommandHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-1-2 created by Kevin Cao
 */
public class HistoryCommandHandler implements
		ConsoleCommandHandler {

	protected PrintStream outPrinter = System.out;

	/**
	 * getHistoryFile
	 * 
	 * @param args List<String>
	 * @return File
	 */
	protected File getHistoryFile(List<String> args) {
		//Show latest migration history log.
		if (args.indexOf("-l") >= 0) {
			File dir = new File(PathUtils.getReportDir());
			final File[] listFiles = dir.listFiles();
			if (listFiles == null || listFiles.length == 0) {
				outPrinter.println("There is no migration history file.");
				return null;
			}
			File result = null;
			for (File ff : listFiles) {
				if (!ff.getName().endsWith(".mh")) {
					continue;
				}
				if (result == null) {
					result = ff;
				} else if (result.getName().compareTo(ff.getName()) < 0) {
					result = ff;
				}
			}
			return result;
		}
		if (args.isEmpty()) {
			outPrinter.println("Not enough parameters, please specify a migration history file name.");
			printHelp();
			return null;
		}
		String mh = args.get(args.size() - 1);
		File file = new File(mh);
		if (!file.exists()) {
			file = new File(PathUtils.getReportDir() + "/" + mh);
		}
		if (!file.exists() || file.isDirectory()) {
			outPrinter.println("Invalid migration history file.");
			return null;
		}
		return file;
	}

	/**
	 * Print migration history's log
	 * 
	 * @param args The parameters input by user
	 * 
	 */
	public void handleCommand(List<String> args) {

	}

	/**
	 * printHelp
	 * 
	 */
	protected void printHelp() {

	}

	/**
	 * Waiting for Enter
	 * 
	 * @return false to stop display the information
	 */
	protected boolean waitForEnter() {
		String ps = "<Press [enter] to continue...>";
		outPrinter.print(ps);
		Console console = System.console();
		String rl = console.readLine().trim();
		if ("q".equalsIgnoreCase(rl) || "exit".equalsIgnoreCase(rl)
				|| "quit".equalsIgnoreCase(rl)) {
			return false;
		}
		return true;
	}

	/**
	 * printHistoryFiles
	 * 
	 */
	protected void printHistoryFiles() {
		File dir = new File(PathUtils.getReportDir());
		String[] mhs = dir.list();
		if (mhs != null) {
			outPrinter.println();
			outPrinter.println("Available migration history file(s):");
			for (String ss : mhs) {
				if (!ss.endsWith(".mh")) {
					continue;
				}
				outPrinter.println("    " + ss);
			}
		}
	}
}
