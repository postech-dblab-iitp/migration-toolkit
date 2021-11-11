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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;

import com.cubrid.common.configuration.jdbc.IJDBCConnectionChangedObserver;
import com.cubrid.common.configuration.jdbc.IJDBCInfoChangedSubject;
import com.cubrid.common.configuration.jdbc.JDBCChangingManager;
import com.cubrid.cubridmigration.core.common.CipherUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.common.xml.IXMLMemento;
import com.cubrid.cubridmigration.core.common.xml.XMLMemento;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;

/**
 * JDBCConnectionManager is response for managing local JDBC connection
 * information. The connection name is case sensitive. So the "test" and "Test"
 * are different connections. All input and output connection object are copies.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-4-23 created by Kevin Cao
 */
public final class CMTConParamManager implements
		IJDBCInfoChangedSubject {

	private final static Logger LOG = LogUtil.getLogger(CMTConParamManager.class);

	private static final CMTConParamManager MANAGER;
	static {
		MANAGER = new CMTConParamManager();
		//Do nothing here
		JDBCChangingManager jcm = JDBCChangingManager.getInstance();
		jcm.registerSubject(MANAGER);
		jcm.registerObservor(new JDBCInfoChangedObserver());
	}
	private List<ConnParameters> connections = new ArrayList<ConnParameters>();
	private Map<ConnParameters, Catalog> catalogs = new HashMap<ConnParameters, Catalog>();

	private File defaultFile = null;

	private List<IJDBCConnectionChangedObserver> observers = new ArrayList<IJDBCConnectionChangedObserver>();

	private CMTConParamManager() {
		//Do nothing here.
	}

	public static CMTConParamManager getInstance() {
		return MANAGER;
	}

	/**
	 * Load from a XML file
	 * 
	 * @param file File
	 */
	public void loadFromFile(File file) {
		try {
			FileInputStream reader = new FileInputStream(file);
			try {
				IXMLMemento memento = XMLMemento.loadMemento(reader);
				if (memento == null) {
					return;
				}
				IXMLMemento[] children = memento.getChildren("database");

				for (int i = 0; i < children.length; i++) {
					final IXMLMemento child = children[i];
					boolean isXmlDatabase = child.getBoolean("isXMLDatabase");

					if (isXmlDatabase) {
						continue;
					}
					String dbName = child.getString("dbName");
					Integer databaseTypeID = child.getInteger("databaseTypeID");
					String charSet = child.getString("charSet");
					String username = child.getString("user");
					String password;
					if (child.getBoolean("encrypted")) {
						password = CipherUtils.decrypt(child.getString("password"));
					} else {
						password = child.getString("password");
					}
					String hostIP = child.getString("hostIP");
					int port = Integer.parseInt(child.getString("port"));
					String driverPath = child.getString("driverPath");
					//String driverVersion = children[i].getString("driverVersion");
					DatabaseType dt = DatabaseType.getDatabaseTypeByID(databaseTypeID);

					String conName = child.getString("name");
					String schema = child.getString("schema");
					ConnParameters cp = ConnParameters.getConParam(conName,
							hostIP, port, dbName, dt, charSet, username,
							password, driverPath, schema);
					cp.setUserJDBCURL(child.getString("user_jdbc_url"));
					addConnection(cp, true);
				}
			} finally {
				reader.close();
			}
		} catch (FileNotFoundException ex) {
			LOG.error("", ex);
		} catch (IOException ex) {
			LOG.error("", ex);
		}
	}

	/**
	 * Save to file
	 * 
	 */
	public void save2File() {
		if (defaultFile == null) {
			return;
		}
		try {
			XMLMemento memento = XMLMemento.createWriteRoot("databases");

			for (ConnParameters cp : connections) {
				IXMLMemento child = memento.createChild("database");
				child.putBoolean("isXMLDatabase", false);

				child.putString("name", cp.getConName());
				child.putString("dbName", cp.getDbName());
				child.putInteger("databaseTypeID", cp.getDatabaseType().getID());
				child.putString("charSet", cp.getCharset());
				child.putString("driverClass", cp.getDriverClass());
				child.putString("user", cp.getConUser());
				child.putString("password",
						CipherUtils.encrypt(cp.getConPassword()));
				child.putBoolean("encrypted", true);
				child.putString("hostIP", cp.getHost());
				child.putString("port", cp.getPort() + "");
				child.putString("driverPath", cp.getDriverFileName());
				child.putString("user_jdbc_url", cp.getUserJDBCURL());
				//child.putString("schema", cp.getSchema());
			}
			FileOutputStream writer = new FileOutputStream(defaultFile);
			try {
				memento.save(writer);
			} finally {
				writer.close();
			}
		} catch (ParserConfigurationException ex) {
			LOG.error("", ex);
		} catch (IOException ex) {
			LOG.error("", ex);
		}

	}

	/**
	 * Add a new connection
	 * 
	 * @param cp ConnParameters
	 * @param silence if true the event will not be triggered.
	 */
	public void addConnection(ConnParameters cp, boolean silence) {
		if (cp == null || isNameUsed(cp.getConName()) || isConnectionExists(cp)) {
			return;
		}
		connections.add(cp.clone());
		save2File();
		if (silence) {
			return;
		}
		for (IJDBCConnectionChangedObserver ob : observers) {
			try {
				ob.afterAdd(this, cp);
			} catch (Exception ex) {
				LOG.error("", ex);
			}
		}
	}

	/**
	 * Update a connection by name
	 * 
	 * @param conName old connection name
	 * @param newcp new ConnParameters
	 * @param silence if true, the event will not be triggered.
	 */
	public void updateConnection(String conName, ConnParameters newcp,
			boolean silence) {
		ConnParameters cp = getInternalConParameter(conName);
		if (cp == null || newcp == null) {
			return;
		}
		if (!cp.isSameDB(newcp) && isConnectionExists(newcp)) {
			return;
		}
		ConnParameters oldCP = cp.clone();
		cp.copy(newcp);
		save2File();
		if (silence) {
			return;
		}
		for (IJDBCConnectionChangedObserver ob : observers) {
			try {
				ob.afterModify(this, oldCP, newcp);
			} catch (Exception ex) {
				LOG.error("", ex);
			}
		}
	}

	/**
	 * Update a catalog cache by connection name
	 * 
	 * @param conName String
	 * @param catalog Catalog
	 */
	public void updateCatalog(String conName, Catalog catalog) {
		ConnParameters cp = getInternalConParameter(conName);
		if (cp == null) {
			return;
		}
		catalogs.put(cp, catalog);
	}

	/**
	 * Retrieves a cached catalog by connection name
	 * 
	 * @param conName String
	 * @return Catalog
	 */
	public Catalog getCatalog(String conName) {
		ConnParameters cp = getInternalConParameter(conName);
		if (cp == null) {
			return null;
		}
		return catalogs.get(cp);
	}

	/**
	 * Get internal ConParameter
	 * 
	 * @param conName String
	 * @return ConnParameters
	 */
	private ConnParameters getInternalConParameter(String conName) {
		for (ConnParameters cp : connections) {
			if (cp.getConName().equals(conName)) {
				return cp;
			}
		}
		return null;
	}

	/**
	 * Retrieves a deep copy of connections
	 * 
	 * @return List<ConnParameters>
	 */
	public List<ConnParameters> getConnections() {
		final ArrayList<ConnParameters> result = new ArrayList<ConnParameters>();
		for (ConnParameters cp : connections) {
			result.add(cp.clone());
		}
		return result;
	}

	/**
	 * Retrieves a copy of the connection parameter searched by name
	 * 
	 * @param conName String
	 * @return a copy of the connection parameter
	 */
	public ConnParameters getConnection(String conName) {
		for (ConnParameters cp : connections) {
			if (cp.getConName().equals(conName)) {
				return cp.clone();
			}
		}
		return null;
	}

	/**
	 * Remove the connection by name.
	 * 
	 * @param conName String
	 * @param silence if true,no event will be triggered.
	 */
	public void removeConnection(String conName, boolean silence) {
		for (ConnParameters cp : connections) {
			if (cp.getConName().equals(conName)) {
				connections.remove(cp);
				catalogs.remove(cp);
				save2File();
				if (silence) {
					return;
				}
				for (IJDBCConnectionChangedObserver ob : observers) {
					try {
						ob.afterDelete(this, cp);
					} catch (Exception ex) {
						LOG.error("", ex);
					}
				}
				return;
			}
		}
	}

	/**
	 * Is the name is in used.
	 * 
	 * @param name String
	 * @return true if in used
	 */
	public boolean isNameUsed(String name) {
		for (ConnParameters cp : connections) {
			if (cp.getConName().equals(name)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Is same connection is exists.
	 * 
	 * @param newcp ConnParameters
	 * @return true if in used
	 */
	public boolean isConnectionExists(ConnParameters newcp) {
		for (ConnParameters cp : connections) {
			if (cp.isSameDB(newcp)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Set the file where the configuration will be saved.
	 * 
	 * @param defaultFile File
	 */
	public void setDefaultFile(File defaultFile) {
		this.defaultFile = defaultFile;
	}

	/**
	 * Add Observer
	 * 
	 * @param obv IJDBCConnectionChangedObserver
	 */
	public void addObservor(IJDBCConnectionChangedObserver obv) {
		if (!observers.contains(obv)) {
			observers.add(obv);
		}
	}
}
