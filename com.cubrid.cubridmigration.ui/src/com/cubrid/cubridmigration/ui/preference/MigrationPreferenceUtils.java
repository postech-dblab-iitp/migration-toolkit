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
package com.cubrid.cubridmigration.ui.preference;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.osgi.service.prefs.BackingStoreException;
import org.osgi.service.prefs.Preferences;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.common.xml.IXMLMemento;
import com.cubrid.cubridmigration.core.common.xml.XMLMemento;
import com.cubrid.cubridmigration.tibero.trans.MigrationParameters;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * Load and save the migration parameter value from preference
 * 
 * @author moulinwang
 */
public final class MigrationPreferenceUtils {
	private static final Logger LOG = LogUtil.getLogger(MigrationPreferenceUtils.class);
	private static final String ROOT_MIGRATION_PARAMETER = "MigrationParameter";
	private static final String TAG_MYSQLTOCUBRID_MIGRATION_PARAMETERS = "MySQLtoCUBRIDMigrationParameters";

	//Constructor
	private MigrationPreferenceUtils() {
		//do nothing
	}

	/**
	 * 
	 * Initial the data type mapping relation from client setting
	 * 
	 */
	public static void initMigrationParameters() {
		synchronized (MigrationPreferenceUtils.class) {
			Preferences preference = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
			String xmlString = preference.get(ROOT_MIGRATION_PARAMETER, "");

			if (xmlString == null || xmlString.trim().length() == 0) {
				MigrationParameters.restoreDefault();
			} else {
				MigrationParameters.loadFromPreference(xmlString);
			}
		}
	}

	/**
	 * 
	 * Save the data type mapping relation of this DataTypeMappingType to client
	 * 
	 */
	public static void save() {
		try {
			synchronized (MigrationPreferenceUtils.class) {
				XMLMemento memento = XMLMemento.createWriteRoot(ROOT_MIGRATION_PARAMETER);

				IXMLMemento dataTypeMapping = memento.createChild(TAG_MYSQLTOCUBRID_MIGRATION_PARAMETERS);

				String[] keys = {MigrationParameters.UNPARSED_TIME,
						MigrationParameters.UNPARSED_DATE,
						MigrationParameters.UNPARSED_TIMESTAMP, MigrationParameters.REPLAXE_CHAR0};

				for (String key : keys) {
					String value = MigrationParameters.getMigrationParamter(key);
					IXMLMemento childMemento = dataTypeMapping.createChild(key);
					childMemento.putTextData(value == null ? "" : value);
				}

				String xmlString = memento.saveToString();
				Preferences preference = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
				preference.put(ROOT_MIGRATION_PARAMETER, xmlString);
				preference.flush();
			}
		} catch (ParserConfigurationException ex) {
			LOG.error(LogUtil.getExceptionString(ex));
		} catch (IOException ex) {
			LOG.error(LogUtil.getExceptionString(ex));
		} catch (BackingStoreException ex) {
			LOG.error(LogUtil.getExceptionString(ex));
		}
	}
}
