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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.osgi.service.prefs.BackingStoreException;
import org.osgi.service.prefs.Preferences;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.common.xml.IXMLMemento;
import com.cubrid.cubridmigration.core.common.xml.XMLMemento;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.MapItem;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;
import com.cubrid.cubridmigration.core.trans.MigrationTransFactory;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * Load and save the value from preference
 * 
 * @author pangqiren
 * @version 1.0 - 2010-1-11 created by pangqiren
 */
public final class DataTypeMappingUtil {
	private static final Logger LOG = LogUtil.getLogger(DataTypeMappingUtil.class);

	private static final String TAG_SCALE = "scale";
	private static final String TAG_PRECISION = "precision";
	private static final String TAG_DATA_TYPE = "type";

	private static final String TAG_TARGET_DATA_TYPE = "TargetDataType";
	private static final String TAG_SOURCE_DATA_TYPE = "SourceDataType";

	//Constructor
	private DataTypeMappingUtil() {
		//do nothing
	}

	/**
	 * 
	 * Initial the data type mapping relation from client setting
	 * 
	 */
	public static void initDataTypeMapping() {
		for (DatabaseType dt : DatabaseType.getAllTypes()) {
			AbstractDataTypeMappingHelper dtm = MigrationTransFactory.getTransformHelper(dt,
					DatabaseType.CUBRID).getDataTypeMappingHelper();
			try {
				dtm.loadFromPreference(load(dtm));
			} catch (UnsupportedEncodingException ex) {
				LOG.error("", ex);
			}
		}
	}

	/**
	 * 
	 * Load data type mapping relation of the DataTypeMappingType from client
	 * saving relation
	 * 
	 * @param dataTypeMappingType the mapping type
	 * @return the xml string
	 */
	private static String load(AbstractDataTypeMappingHelper dataTypeMappingType) {
		synchronized (DataTypeMappingUtil.class) {
			Preferences preference = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
			String xmlString = preference.get(dataTypeMappingType.getName(), "");
			return xmlString;
		}
	}

	/**
	 * 
	 * Save the data type mapping relation of this DataTypeMappingType to client
	 * 
	 * @param dataTypeMappingType the mapping type
	 * @param map the mapping relation
	 */
	public static void save(AbstractDataTypeMappingHelper dataTypeMappingType,
			Map<String, MapItem> map) {
		try {
			synchronized (DataTypeMappingUtil.class) {
				String xmlString = getXMLString(dataTypeMappingType, map);
				Preferences preference = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
				preference.put(dataTypeMappingType.getName(), xmlString);
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

	/**
	 * Retrieves the data type mapping's XML string
	 * 
	 * @param dataTypeMappingType AbstractDataTypeMappingHelper
	 * @param map Map<String, MapItem>
	 * @return Map<String, MapItem>
	 * @throws ParserConfigurationException ex
	 * @throws IOException ex
	 */
	private static String getXMLString(AbstractDataTypeMappingHelper dataTypeMappingType,
			Map<String, MapItem> map) throws ParserConfigurationException, IOException {
		XMLMemento memento = XMLMemento.createWriteRoot(dataTypeMappingType.getName());
		Iterator<MapItem> it = map.values().iterator();

		while (it.hasNext()) {
			IXMLMemento dataTypeMapping = memento.createChild("DataTypeMapping");
			MapItem item = it.next();
			MapObject sourceTypeMapping = item.getSource();
			MapObject targetTypeMapping = item.getTarget();

			if (sourceTypeMapping != null) {
				IXMLMemento sourceDataTypeMapping = dataTypeMapping.createChild(TAG_SOURCE_DATA_TYPE);
				String dataType = sourceTypeMapping.getDatatype();
				IXMLMemento typeMemento = sourceDataTypeMapping.createChild(TAG_DATA_TYPE);
				typeMemento.putTextData(dataType == null ? "" : dataType);
				String precision = sourceTypeMapping.getPrecision();
				IXMLMemento precisionMemento = sourceDataTypeMapping.createChild(TAG_PRECISION);
				precisionMemento.putTextData(precision == null ? "" : precision);
				String scale = sourceTypeMapping.getScale();
				IXMLMemento scaleMemento = sourceDataTypeMapping.createChild(TAG_SCALE);
				scaleMemento.putTextData(scale == null ? "" : scale);
			}
			if (targetTypeMapping != null) {
				IXMLMemento targetDataTypeMapping = dataTypeMapping.createChild(TAG_TARGET_DATA_TYPE);
				String dataType = targetTypeMapping.getDatatype();
				IXMLMemento typeMemento = targetDataTypeMapping.createChild(TAG_DATA_TYPE);
				typeMemento.putTextData(dataType == null ? "" : dataType);
				String precision = targetTypeMapping.getPrecision();
				IXMLMemento precisionMemento = targetDataTypeMapping.createChild(TAG_PRECISION);
				precisionMemento.putTextData(precision == null ? "" : precision);
				String scale = targetTypeMapping.getScale();
				IXMLMemento scaleMemento = targetDataTypeMapping.createChild(TAG_SCALE);
				scaleMemento.putTextData(scale == null ? "" : scale);
			}
		}

		String xmlString = memento.saveToString();
		return xmlString;
	}

	/**
	 * Save configuration to a file
	 * 
	 * @param dataTypeMapping DataTypeMappingHelper to be saved
	 * @param preferenceConfigMap Map<String, MapItem> detail settings
	 * @param fileName String file name to save
	 */
	public static void saveAs(AbstractDataTypeMappingHelper dataTypeMapping,
			Map<String, MapItem> preferenceConfigMap, String fileName) {
		try {
			String xmlString = getXMLString(dataTypeMapping, preferenceConfigMap);
			File file = new File(fileName);
			if (file.exists()) {
				PathUtils.deleteFile(file);
			}
			PathUtils.createFile(file);
			InputStream is = new ByteArrayInputStream(xmlString.getBytes("UTF-8"));
			try {
				CUBRIDIOUtils.writeToFile(fileName, is);
			} finally {
				is.close();
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
