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
package com.cubrid.cubridmigration.core.dbobject;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;

/**
 * 
 * Catalog
 * 
 * @author moulinwang fulei
 * @version 1.0 - 2009-9-15 created by moulinwang modify by fulei 2011-09-28
 */
public class Catalog implements
		Serializable {

	private static final long serialVersionUID = 1997070198874610025L;
	public static final String KEY_DB_NCHAR_CHARACTERSET = "NCHAR_CHARACTERSET";
	public static final String KEY_DB_CHARACTERSET = "CHARACTERSET";
	public static final String KEY_DB_VERSION = "RDBMS_VERSION";
	public static final String KEY_DB_TIMEZONE = "RDBMS_TIMEZONE";

	private String name;
	private List<Schema> schemas = new ArrayList<Schema>();

	private Version version;
	private String host;
	private int port;
	private int databaseType;
	private String createSql;
	private Map<String, String> additionalInfo = new HashMap<String, String>();
	private Map<String, List<DataType>> supportedDataType = new HashMap<String, List<DataType>>();

	private ConnParameters connectionParameters;

	private long createTime;

	private Map<String, Integer> allTablesCountMap = new HashMap<String, Integer>();
	private Map<String, Integer> allViewsCountMap = new HashMap<String, Integer>();
	private Map<String, Integer> allSequencesCountMap = new HashMap<String, Integer>();
	
	public Catalog() {
		createTime = System.currentTimeMillis();
	}

	public Map<String, String> getAdditionalInfo() {
		return additionalInfo;
	}

	public void setAdditionalInfo(Map<String, String> additionalInfo) {
		this.additionalInfo = additionalInfo;
	}

	public ConnParameters getConnectionParameters() {
		return connectionParameters;
	}

	public void setConnectionParameters(ConnParameters connectionParameters) {
		this.connectionParameters = connectionParameters;
	}

	public String getCharset() {
		return additionalInfo.get(KEY_DB_CHARACTERSET);
	}

	/**
	 * set charset
	 * 
	 * @param dbCharset String
	 */
	public void setCharset(String dbCharset) {
		additionalInfo.put(KEY_DB_CHARACTERSET, dbCharset);
	}

	public DatabaseType getDatabaseType() {
		return DatabaseType.getDatabaseTypeByID(databaseType);
	}

	public void setDatabaseType(DatabaseType databaseType) {
		this.databaseType = databaseType.getID();
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Schema> getSchemas() {
		return schemas;
	}

	/**
	 * Set schemas information
	 * 
	 * @param schemas List<Schema>
	 */
	public void setSchemas(List<Schema> schemas) {
		if (schemas == null) {
			this.schemas.clear();
			return;
		}
		for (Schema schema : schemas) {
			addSchema(schema);
		}
	}

	/**
	 * return schema by a given name
	 * 
	 * @param schemaName String
	 * @return Schema
	 */
	public Schema getSchemaByName(String schemaName) {
		//Default is the first schema.
		for (Schema schema : schemas) {
			if (schemaName == null) {
				return schema;
			}
			if (schemaName.equals(schema.getName())) {
				return schema;
			}
		}
		return null;
	}

	public Version getVersion() {
		return version;
	}

	public void setVersion(Version version) {
		this.version = version;
	}

	/**
	 * add schema
	 * 
	 * @param schema Schema
	 */
	public void addSchema(Schema schema) {
		if (schemas == null) {
			schemas = new ArrayList<Schema>();
		}
		if (schema == null) {
			return;
		}
		schema.setCatalog(this);
		schemas.add(schema);
	}

	public String getCreateSql() {
		return createSql;
	}

	public void setCreateSql(String createSql) {
		this.createSql = createSql;
	}

	public String getTimezone() {
		return additionalInfo.get(KEY_DB_TIMEZONE);
	}

	/**
	 * set time zone
	 * 
	 * @param timezone String
	 */
	public void setTimezone(String timezone) {
		additionalInfo.put(KEY_DB_TIMEZONE, timezone);
	}

	public Map<String, List<DataType>> getSupportedDataType() {
		return supportedDataType;
	}

	public void setSupportedDataType(Map<String, List<DataType>> supportedDataType) {
		this.supportedDataType = supportedDataType;
	}
	
	/**
	 * The Signature is used for Identification of the catalog, same Signature
	 * means same catalog.
	 * 
	 * @return Catalog's create Time
	 */
	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}
	
	public Map<String, Integer> getAllTablesCountMap() {
		return allTablesCountMap;
	}
	
	public Map<String, Integer> getAllViewsCountMap() {
		return allViewsCountMap;
	}
	
	public Map<String, Integer> getAllSequencesCountMap() {
		return allSequencesCountMap;
	}

	/**
	 * return hash code
	 * 
	 * @return integer
	 */
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + port;
		return result;
	}

	/**
	 * return equal flag
	 * 
	 * @param obj Object
	 * @return boolean
	 */
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		if (getClass() != obj.getClass()) {
			return false;
		}

		final Catalog other = (Catalog) obj;

		if (host == null) {
			if (other.host != null) {
				return false;
			}
		} else if (!host.equals(other.host)) {
			return false;
		}

		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}

		if (port != other.port) {
			return false;
		}

		return true;
	}

	/**
	 * Save to XML file
	 * 
	 * @param tempFile to be save
	 */
	public void saveXML(File tempFile) {
		ConnParameters tmpCP = getConnectionParameters();
		try {
			PathUtils.createFile(tempFile);
			FileOutputStream fos = new FileOutputStream(tempFile);
			XMLEncoder encoder = new XMLEncoder(fos);

			setConnectionParameters(null);
			for (Schema sc : getSchemas()) {
				sc.setCatalog(null);
				//For JDK 7: XMLEncoder of 7 has error when writing objects.
				//Set column's table property to NULL
				for (Table tbl : sc.getTables()) {
					for (Column col : tbl.getColumns()) {
						col.setTableOrView(null);
					}
				}
			}
			encoder.writeObject(this);
			encoder.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			setConnectionParameters(tmpCP);
		}
	}

	/**
	 * Save to XML file
	 * 
	 * @param schemaStr to be loaded
	 * @return Catalog
	 */
	public static Catalog loadXML(String schemaStr) {
		XMLDecoder decoder;
		try {
			byte[] bytes = schemaStr.getBytes("utf-8");
			decoder = new XMLDecoder(new ByteArrayInputStream(bytes));
			Catalog srcCatalog = (Catalog) decoder.readObject();
			decoder.close();
			return srcCatalog;
		} catch (UnsupportedEncodingException e) {
			return null;
			//Do nothing
		}
	}
}
