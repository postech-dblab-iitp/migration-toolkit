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
package com.cubrid.cubridmigration.command;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

/**
 * 
 * ConsoleUtils provides methods that are used by console.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-11-14 created by Kevin Cao
 */
public class ConsoleUtils {

	/**
	 * Reading console input
	 * 
	 * @return String
	 * @throws IOException
	 */
	public static String readingInput() throws IOException {
		String script;
		StringBuffer sb = new StringBuffer();
		int ch = 0;
		while ((ch = System.in.read()) != '\n') {
			sb.append((char) ch);
		}
		script = sb.toString().trim();
		return script;
	}

	/**
	 * Print help information
	 * 
	 */
	public static void printHelp(String src) {
		final InputStream in = ConsoleUtils.class.getResourceAsStream(src);
		try {
			final List<String> readLines = IOUtils.readLines(in);
			for (String ss : readLines) {
				System.out.println(ss);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Retrieves the parameters, the parameters which was found will be removed
	 * from list
	 * 
	 * @param args all args
	 * @param paramName parameter name
	 * @return parameter value
	 */
	public static String getParameter(List<String> args, String paramName) {
		boolean found = false;
		String param = null;
		List<String> tempList = new ArrayList<String>(args);
		for (String str : tempList) {
			if (!str.startsWith("-")) {
				if (found) {
					param = str;
					args.remove(str);
					break;
				} else {
					continue;
				}
			}
			if (found) {
				break;
			}
			if (str.equalsIgnoreCase(paramName)) {
				args.remove(str);
				found = true;
			} else if (str.startsWith(paramName)) {
				param = str.substring(3);
				args.remove(str);
				break;
			}
		}
		return StringUtils.isBlank(param) ? null : param;
	}
}
