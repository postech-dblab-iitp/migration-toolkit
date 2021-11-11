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
import java.sql.Driver;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;

/**
 * data structure to store jdbc data
 * 
 * @author moulinwang
 * @version 1.0 - 2010-10-21 created by moulinwang
 */
public class JDBCData {

	private static final Logger LOG = LogUtil.getLogger(JDBCData.class);

	private final DatabaseType databaseType;
	private final String jdbcDriverPath;
	private final String jdbcDriverName;
	private final ClassLoader jdbcClassLoader;
	private final String driverClassName;

	public JDBCData(DatabaseType databaseType, String jdbcDriverPath,
			ClassLoader jdbcClassLoader, String driverClassName) {
		this.databaseType = databaseType;
		this.jdbcDriverPath = PathUtils.toCanonicalPath(jdbcDriverPath);
		this.jdbcClassLoader = jdbcClassLoader;
		this.jdbcDriverName = new File(jdbcDriverPath).getName();
		this.driverClassName = driverClassName;
	}

	public DatabaseType getDatabaseType() {
		return databaseType;
	}

	/**
	 * Retrieves the driver of jdbc
	 * 
	 * @return Driver
	 */
	@SuppressWarnings("unchecked")
	public Driver getDriver() {
		try {
			Class<Driver> result = (Class<Driver>) jdbcClassLoader.loadClass(driverClassName);
			return result.newInstance();
		} catch (Exception e) {
			LOG.error("", e);
		}
		return null;
	}

	public ClassLoader getJdbcClassLoader() {
		return jdbcClassLoader;
	}

	public String getJdbcDriverName() {
		return jdbcDriverName;
	}

	public String getJdbcDriverPath() {
		return jdbcDriverPath;
	}

	/**
	 * Retrieves the JDBC data's description. And it is unique
	 * 
	 * @return description
	 */
	public String getDesc() {
		if (StringUtils.isBlank(jdbcDriverPath)) {
			return "";
		}
		final Driver driver = getDriver();
		return driver == null ? ""
				: new StringBuffer(databaseType.getName()).append("[").append(
						getVersion()).append("] - ").append(jdbcDriverPath).toString();
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	/**
	 * Retrieves the current JDBC driver's version string.
	 * 
	 * @return JDBC Version
	 */
	public String getVersion() {
		String version = null;
		if (DatabaseType.CUBRID.equals(databaseType)) {
			try {
				version = JDBCUtil.getJdbcJarVersion(jdbcDriverPath);
			} catch (IOException e) {
				//Ignore error
			}
		}
		if (version == null) {
			Driver driver = getDriver();
			if (driver == null) {
				return "";
			}
			version = driver.getMajorVersion() + "." + driver.getMinorVersion();
		} else if (version.split("-").length == 3) {
			version = version.split("-")[2];
		}
		return version;
	}
}
