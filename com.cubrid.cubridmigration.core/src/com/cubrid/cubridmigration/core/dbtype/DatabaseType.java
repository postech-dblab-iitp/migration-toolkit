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
package com.cubrid.cubridmigration.core.dbtype;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.connection.IConnHelper;
import com.cubrid.cubridmigration.core.connection.JDBCData;
import com.cubrid.cubridmigration.core.connection.JDBCUtil;
import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.export.DBExportHelper;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDDatabase;
import com.cubrid.cubridmigration.graph.GraphDatabase;
import com.cubrid.cubridmigration.mssql.MSSQLDatabase;
import com.cubrid.cubridmigration.mysql.MySQLDatabase;
import com.cubrid.cubridmigration.oracle.OracleDatabase;
import com.cubrid.cubridmigration.tibero.TiberoDatabase;

/**
 * Base class of Database Types
 * 
 * @author Kevin Cao
 * @version 1.0 - 2010-11-07
 */

public abstract class DatabaseType {

	//private static final Logger LOG = LogUtil.getLogger(DatabaseType.class);

	public static final DatabaseType MYSQL = new MySQLDatabase();

	public static final DatabaseType CUBRID = new CUBRIDDatabase();

	public static final DatabaseType MSSQL = new MSSQLDatabase();

	public static final DatabaseType ORACLE = new OracleDatabase();
	
	//GDB add graph Database into DB types
	public static final DatabaseType GRAPH = new GraphDatabase();
	
	public static final DatabaseType TIBERO = new TiberoDatabase();

	private static final DatabaseType[] DTS = new DatabaseType[] {MYSQL, CUBRID, ORACLE, MSSQL, GRAPH, TIBERO};

	/**
	 * Retrieves all Database types
	 * 
	 * @return DatabaseType[]
	 */
	public static final DatabaseType[] getAllTypes() {
		return DTS.clone();
	}

	/**
	 * return DatabaseType by database type ID
	 * 
	 * @param id Integer
	 * @return DatabaseType
	 */
	public static final DatabaseType getDatabaseTypeByID(int id) {
		for (DatabaseType databaseType : DTS) {
			if (databaseType.getID() == (id)) {
				return databaseType;
			}
		}
		throw new RuntimeException("Database Type [" + id + "] is not supported!");
	}

	/**
	 * getDatabaseTypeIDByDBName
	 * 
	 * @param name String
	 * @return int
	 */
	public static DatabaseType getDatabaseTypeIDByDBName(String name) {
		for (DatabaseType databaseType : DTS) {
			if (databaseType.getName().equalsIgnoreCase(name)) {
				return databaseType;
			}
		}
		throw new RuntimeException("Database Type [" + name + "] is not supported!");
	}

	private final Integer id;
	private final String name;
	private final String[] jdbcClassName;
	private final String defaultJdbcPort;
	private final AbstractJDBCSchemaFetcher dbObjectBuilder;
	private final DBExportHelper dbExportHelper;

	private final List<JDBCData> jdbcDatas = new ArrayList<JDBCData>();

	private final IConnHelper conHelper;

	private final boolean supportJDBCEncoding;

	protected DatabaseType(Integer databaseTypeID, String defaultDBSystemName, String[] jdbcClass,
			String defaultJdbcPort, AbstractJDBCSchemaFetcher dbObjectBuilder,
			DBExportHelper dbExportHelper, IConnHelper conHelper, boolean supportJDBCEncoding) {
		this.id = databaseTypeID;
		this.name = defaultDBSystemName;
		this.jdbcClassName = Arrays.copyOf(jdbcClass, jdbcClass.length);
		this.defaultJdbcPort = defaultJdbcPort;
		//		this.prefixQuote = prefixQuote;
		//		this.suffixQuote = suffixQuote;
		this.dbObjectBuilder = dbObjectBuilder;
		this.dbExportHelper = dbExportHelper;
		this.conHelper = conHelper;
		this.supportJDBCEncoding = supportJDBCEncoding;
	}

	/**
	 * Add jdbc data to current database type object
	 * 
	 * @param driverPath String
	 * @return true if add successfully
	 */
	public boolean addJDBCData(String driverPath) {
		ClassLoader cl;
		for (String cn : jdbcClassName) {
			try {
				cl = JDBCUtil.getJDBCDriverClassLoader(driverPath);
				cl.loadClass(cn);
			} catch (Exception ex) {
				continue;
			}
			jdbcDatas.add(new JDBCData(this, driverPath, cl, cn));
			return true;
		}
		return false;

	}

	/**
	 * Add several drivers to current database type object
	 * 
	 * @param driverPaths String[]
	 * @param cls ClassLoader[]
	 */
	public void addJDBCData(String[] driverPaths, ClassLoader[] cls) {
		for (int i = 0; i < cls.length; i++) {
			if (getJDBCData(driverPaths[i]) != null) {
				continue;
			}
			for (String cn : jdbcClassName) {
				try {
					cls[i].loadClass(cn);
				} catch (ClassNotFoundException e) {
					continue;
				}
				jdbcDatas.add(new JDBCData(this, driverPaths[i], cls[i], cn));
				break;
			}

		}
	}

	public IConnHelper getConHelper() {
		return conHelper;
	}

	public String getDefaultJDBCPort() {
		return defaultJdbcPort;
	}

	public DBExportHelper getExportHelper() {
		return dbExportHelper;
	}

	public int getID() {
		return id;
	}

	/**
	 * return best JDBCData for a given database ID and jdbc version
	 * 
	 * @param driverFile String
	 * @return JDBCData
	 */
	public JDBCData getJDBCData(String driverFile) {
		if (StringUtils.isBlank(driverFile)) {
			return null;
		}
		try {
			String fullName;
			final File file = new File(driverFile);
			if (file.exists()) {
				fullName = file.getCanonicalPath();
			} else {
				fullName = file.getName();
			}
			for (JDBCData jdbcData : jdbcDatas) {
				if (jdbcData.getJdbcDriverPath().indexOf(fullName) >= 0) {
					return jdbcData;
				} else if (fullName.endsWith(jdbcData.getJdbcDriverName())) {
					return jdbcData;
				}
			}
			//If file exists and did not find the JDBC data in the list, search it by file name
			for (JDBCData jdbcData : jdbcDatas) {
				if (jdbcData.getJdbcDriverPath().indexOf(file.getName()) >= 0) {
					return jdbcData;
				}
			}
		} catch (IOException e) {
			return null;
		}
		return null;
	}

	/**
	 * Retrieves a copy of jdbc datas of current database
	 * 
	 * @return List<JDBCData>
	 */
	public List<JDBCData> getJDBCDatas() {
		return new ArrayList<JDBCData>(jdbcDatas);
	}

	/**
	 * return meta data builder
	 * 
	 * @return DBObjectBuilder
	 */
	public AbstractJDBCSchemaFetcher getMetaDataBuilder() {
		return dbObjectBuilder;
	}

	public String getName() {
		return name;
	}

	public boolean isSupportJDBCEncoding() {
		return supportJDBCEncoding;
	}

	/**
	 * Remove JDBC data from list
	 * 
	 * @param jd JDBCData
	 */
	public void removeJDBCData(JDBCData jd) {
		jdbcDatas.remove(jd);
	}

	/**
	 * Retrieves the databases SQL helper
	 * 
	 * @param version Database version
	 * @return SQLHelper
	 */
	public abstract SQLHelper getSQLHelper(String version);

	/**
	 * Retrieves the databases data type helper
	 * 
	 * @param version Database version
	 * @return DBDataTypeHelper
	 */
	public abstract DBDataTypeHelper getDataTypeHelper(String version);

	/**
	 * The database type is supporting multi-schema.
	 * 
	 * @return true if supporting.
	 */
	public boolean isSupportMultiSchema() {
		return false;
	}
}
