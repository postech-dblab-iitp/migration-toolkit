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

import java.util.ArrayList;
import java.util.List;

import com.cubrid.cubridmigration.command.handler.CDCCommandHandler;
import com.cubrid.cubridmigration.command.handler.LogCommandHandler;
import com.cubrid.cubridmigration.command.handler.ReportCommandHandler;
import com.cubrid.cubridmigration.command.handler.ScriptCommandHandler;
import com.cubrid.cubridmigration.command.handler.StartCommandHandler;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.JDBCUtil;

/**
 * Run migration script in command line
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-2-2 created by Kevin Cao
 */
public class DoMigration {

	/**
	 * Run migration script in command line
	 * 
	 * @param args the first parameter should be the full name of the migration
	 *        script
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("Thank you for using CUBRID Migration Toolkit(CMT) Console.");
		PathUtils.initPaths();
		LogUtil.initLog(PathUtils.getLogDir());
		List<String> argList = new ArrayList<String>();
		for (String arg : args) {
			argList.add(arg);
		}
		if (argList.isEmpty()) {
			ConsoleUtils.printHelp("/com/cubrid/cubridmigration/command/help.txt");
			return;
		}
		final ConsoleCommandHandler commandHandler = handlerFactory(argList);
		commandHandler.handleCommand(argList);
	}

	/**
	 * handlerFactory
	 * 
	 * @param argList List<String>
	 * @return ConsoleCommandHandler
	 */
	private static ConsoleCommandHandler handlerFactory(List<String> argList) {
		final ConsoleCommandHandler commandHandler;
		final String exeType = argList.get(0);
		if ("log".equalsIgnoreCase(exeType)) {
			commandHandler = new LogCommandHandler();
			argList.remove(0);
		} else if ("report".equalsIgnoreCase(exeType)) {
			commandHandler = new ReportCommandHandler();
			argList.remove(0);
		} else if ("start".equalsIgnoreCase(exeType)) {
			JDBCUtil.initialJdbcByPath(PathUtils.getJDBCLibDir());
			commandHandler = new StartCommandHandler();
			argList.remove(0);
		} else if ("script".equalsIgnoreCase(exeType)) {
			JDBCUtil.initialJdbcByPath(PathUtils.getJDBCLibDir());
			commandHandler = new ScriptCommandHandler();
			argList.remove(0);
		} else if ("cdc".equalsIgnoreCase(exeType)) {
			JDBCUtil.initialJdbcByPath(PathUtils.getJDBCLibDir());
			commandHandler = new CDCCommandHandler();
			argList.remove(0);
		} else {
			JDBCUtil.initialJdbcByPath(PathUtils.getJDBCLibDir());
			commandHandler = new StartCommandHandler();
		}
		return commandHandler;
	}

}