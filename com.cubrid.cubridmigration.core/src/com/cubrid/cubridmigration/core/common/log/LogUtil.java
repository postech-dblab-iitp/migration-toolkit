/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
 * Solution.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 */
package com.cubrid.cubridmigration.core.common.log;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * 
 * This class is common log4j interface and be convinent to Get Logger
 * 
 * @author pangqiren
 * @version 1.0 - 2009-6-4
 */

public final class LogUtil {
	//Constructor
	private LogUtil() {
		//do nothing
	}

	//	//static initialization
	//	static {
	//		initLog();
	//	}

	/**
	 * init Log
	 * 
	 * @param logDir log's directory
	 */
	public static void initLog(String logDir) {

		try {
			InputStream is = LogUtil.class.getResourceAsStream("log4j.properties");
			try {
				Properties pro = new Properties();
				pro.load(is);
				pro.setProperty("log4j.appender.logfile.file", logDir + "/cmt.log");
				PropertyConfigurator.configure(pro);
			} finally {
				is.close();
			}
		} catch (IOException e) {
			BasicConfigurator.configure();
		}
	}

	//	/**
	//	 * Override the debug mode
	//	 */
	//	public static void changeDebugMode() {
	//		Properties configPro = new Properties();
	//		InputStream in = null;
	//		try {
	//			in = new LogUtil().getClass().getResourceAsStream("log4j.properties");
	//			configPro.load(in);
	//		} catch (IOException e) {
	//			e.printStackTrace();
	//			configPro = null;
	//		} finally {
	//			Closer.close(in);
	//		}
	//		if (configPro == null) {
	//			return;
	//		}
	//		configPro.put("log4j.rootLogger", "DEBUG,stdout,logfile");
	//		PropertyConfigurator.configure(configPro);
	//	}

	/**
	 * 
	 * Get logger
	 * 
	 * @param clazz Class<?>
	 * @return Logger
	 */
	public static Logger getLogger(Class<?> clazz) {
		return Logger.getLogger(clazz);
	}

	/**
	 * 
	 * Get logger
	 * 
	 * @param name String
	 * @return Logger
	 */
	public static Logger getLogger(String name) {
		return Logger.getLogger(name);
	}

	/**
	 * getExceptionString
	 * 
	 * @param throwable Exception
	 * @return String
	 */
	public static String getExceptionString(Throwable throwable) {
		StringWriter stringWriter = new StringWriter();
		throwable.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}
}
