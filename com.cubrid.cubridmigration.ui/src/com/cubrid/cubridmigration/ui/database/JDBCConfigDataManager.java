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
package com.cubrid.cubridmigration.ui.database;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.osgi.service.prefs.BackingStoreException;
import org.osgi.service.prefs.Preferences;

import com.cubrid.common.configuration.jdbc.JDBCDriverChangingManager;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.common.xml.IXMLMemento;
import com.cubrid.cubridmigration.core.common.xml.XMLMemento;
import com.cubrid.cubridmigration.core.connection.JDBCData;
import com.cubrid.cubridmigration.core.connection.JDBCDriverManager;
import com.cubrid.cubridmigration.core.connection.JDBCUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * 
 * JdbcConfigDataManager
 * 
 * @author Kevin Cao
 * @version 1.0
 */
public final class JDBCConfigDataManager {
	private static final Logger LOG = LogUtil.getLogger(JDBCConfigDataManager.class);

	private static final String PREFIX = "JDBC_SETTING";

	/**
	 * The constructor
	 */
	private JDBCConfigDataManager() {
	}

	/**
	 * 
	 * Load CUBRID JDBC driver from plugin preference
	 * 
	 */
	public static void loadJdbc() {
		synchronized (JDBCConfigDataManager.class) {
			//Synchronize from other system.
			List<String> allDrivers = JDBCDriverChangingManager.getInstance().getAllDrivers();
			for (String jdbcDriverPath : allDrivers) {
				JDBCDriverManager.getInstance().addDriver(jdbcDriverPath, false);
			}
			//Load self configuration
			Preferences preference = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
			String xmlString = preference.get(PREFIX, "");

			JDBCUtil.initialJdbcByPath(PathUtils.getJDBCLibDir());
			LOG.debug("initialJdbcByPath:" + PathUtils.getJDBCLibDir());
			if (StringUtils.isBlank(xmlString)) {
				return;
			}
			try {
				ByteArrayInputStream in = new ByteArrayInputStream(xmlString.getBytes("UTF-8"));
				IXMLMemento memento = XMLMemento.loadMemento(in);
				if (memento == null) {
					return;
				}
				IXMLMemento[] children = memento.getChildren("JdbcData");
				if (children == null || children.length == 0) {
					return;
				}
				//load saved JDBC configurations
				for (int i = 0; i < children.length; i++) {
					String jdbcDriverPath = children[i].getString("jdbcDriverPath");
					JDBCDriverManager.getInstance().addDriver(jdbcDriverPath, false);
					//					Integer databaseTypeID = children[i].getInteger("databaseTypeID");
					//					DatabaseType dt = DatabaseType.getDatabaseTypeByID(databaseTypeID);
					//					dt.addJDBCData(jdbcDriverPath);
				}
			} catch (IOException e) {
				LOG.error(LogUtil.getExceptionString(e));
			}
		}
	}

	/**
	 * 
	 * Add or delete JDBC driver Integer databaseTypeID; String jdbcVersion;
	 * String jdbcDriverPath; ClassLoader jdbcClassLoader;
	 * 
	 */
	public static void saveJdbcData() {
		synchronized (JDBCConfigDataManager.class) {
			String xmlString;
			try {
				XMLMemento memento = XMLMemento.createWriteRoot("JdbcSetting");
				Iterator<JDBCData> iterator = JDBCUtil.getAllJDBCData().iterator();
				while (iterator.hasNext()) {
					IXMLMemento child = memento.createChild("JdbcData");
					JDBCData jdbcData = iterator.next();
					child.putString("jdbcDriverPath", jdbcData.getJdbcDriverPath());
					child.putInteger("databaseTypeID", jdbcData.getDatabaseType().getID());
				}
				xmlString = memento.saveToString();
				Preferences preference = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
				preference.put(PREFIX, xmlString);
				preference.flush();
			} catch (IOException e) {
				LOG.error(e.getMessage());
			} catch (BackingStoreException e) {
				LOG.error(e.getMessage());
			} catch (ParserConfigurationException e) {
				LOG.error(e.getMessage());
			}
		}
	}
}
