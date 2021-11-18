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
package com.cubrid.cubridmigration.core.connection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.log4j.Logger;

import com.cubrid.common.configuration.classloader.ClassLoaderManager;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;

/**
 * jdbc manager
 * 
 * @author moulinwang
 * @version 1.0 - 2010-10-21 created by moulinwang
 */
public class JDBCUtil { //NOPMD

	private static final Logger LOG = LogUtil.getLogger(JDBCUtil.class);

	/**
	 * initial jdbc drivers
	 * 
	 * @param jdbcDirectory String
	 */
	public static void initialJdbcByPath(String jdbcDirectory) {
		File jdbcDir = new File(jdbcDirectory);

		if (!jdbcDir.exists() && !jdbcDir.mkdirs()) {
			boolean flag = jdbcDir.mkdirs();
			LOG.info("make jdbc dir:" + flag);
			return;
		}
		try {
			LOG.info("JDBC Driver repository:" + jdbcDir.getCanonicalPath());
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		File[] driverFiles = jdbcDir.listFiles();
		driverFiles = driverFiles == null ? new File[0] : driverFiles;
		List<String> fullNames = new ArrayList<String>();
		List<ClassLoader> classLoaders = new ArrayList<ClassLoader>();
		for (int i = 0; i < driverFiles.length; i++) {
			File driverFile = driverFiles[i];
			if (driverFile.isDirectory()) {
				continue;
			}
			try {
				final String canonicalPath = driverFile.getCanonicalPath();
				ClassLoader cl = getJDBCDriverClassLoader(canonicalPath);
				fullNames.add(canonicalPath);
				classLoaders.add(cl);
			} catch (IOException e) {
				LOG.error("", e);
			}
		}
		for (DatabaseType dt : DatabaseType.getAllTypes()) {
			dt.addJDBCData(fullNames.toArray(new String[]{}),
					classLoaders.toArray(new ClassLoader[]{}));
		}
	}

	/**
	 * Create a new class loader of given file.
	 * 
	 * @param jdbcDriverPath String
	 * @return a new ClassLoader
	 */
	public static ClassLoader getJDBCDriverClassLoader(String jdbcDriverPath) {
		return ClassLoaderManager.getInstance().getClassLoader(jdbcDriverPath);
	}

	/**
	 * Retrieves all jdbc data of system
	 * 
	 * @return JDBCData list
	 */
	public static List<JDBCData> getAllJDBCData() {
		List<JDBCData> result = new ArrayList<JDBCData>();
		for (DatabaseType dt : DatabaseType.getAllTypes()) {
			result.addAll(dt.getJDBCDatas());
		}
		return result;
	}

	/**
	 * 
	 * Get JDBC jar version,if it is valid,return the version
	 * information(format:CUBRID-JDBC-8.2.0.1147);otherwise return null
	 * 
	 * @param jdbcURL the jdbc url
	 * @return the version (format:CUBRID-JDBC-8.2.0.1147)
	 * @throws IOException exception
	 */
	public static String getJdbcJarVersion(String jdbcURL) throws IOException {
		JarFile jarfile = new JarFile(new File(jdbcURL));
		Enumeration<JarEntry> entries = jarfile.entries();
		while (entries.hasMoreElements()) {
			JarEntry nextElement = entries.nextElement();
			if (nextElement.getName() != null
					&& nextElement.getName().startsWith("CUBRID-JDBC-")) {
				return nextElement.getName();
			}
		}
		return null;
	}
}
