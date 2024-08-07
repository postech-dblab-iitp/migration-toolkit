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
package com.cubrid.cubridmigration.core.mapping;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.common.xml.IXMLMemento;
import com.cubrid.cubridmigration.core.common.xml.XMLMemento;
import com.cubrid.cubridmigration.core.mapping.model.MapItem;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;

/**
 * 
 * AbstractDataTypeMappingHelper Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-29 created by Kevin Cao
 */
public abstract class AbstractDataTypeMappingHelper {
	private static final String TAG_SCALE = "scale";
	private static final String TAG_PRECISION = "precision";
	private static final String TAG_DATA_TYPE = "type";
	private static final String TAG_TARGET_DATA_TYPE = "TargetDataType";
	private static final String TAG_SOURCE_DATA_TYPE = "SourceDataType";
	public static final String MAP_KEY_SEPARATOR = "_";

	private static final Logger LOG = LogUtil.getLogger(AbstractDataTypeMappingHelper.class);

	//the preference config
	private Map<String, MapItem> preferenceConfigMap = new HashMap<String, MapItem>();
	//the xml file config
	private final Map<String, MapItem> xmlConfigMap;

	private final String name;
	private final String dataTypeMappingFileName;

	/**
	 * @param name
	 * @param dataTypeMappingFileName
	 */
	public AbstractDataTypeMappingHelper(String name,
			String dataTypeMappingFileName) {
		this.name = name;
		this.dataTypeMappingFileName = dataTypeMappingFileName;
		//load default config from xml file
		xmlConfigMap = loadDefault(); //NOPMD
		//load config from preference
		preferenceConfigMap = restoreDefault(); //NOPMD
	}

	/**
	 * getMapKey
	 * 
	 * @param datatype String
	 * @param precision String
	 * @param scale String
	 * @return String
	 */
	public abstract String getMapKey(String datatype, String precision,
			String scale);

	/**
	 * 
	 * return the database name
	 * 
	 * @return name String
	 */
	public String getName() {
		return name;
	}

	/**
	 * 
	 * Get data type mapping
	 * 
	 * @return Map<String, MapItem>
	 */
	public Map<String, MapItem> getPreferenceConfigMap() {
		return this.preferenceConfigMap;
	}

	/**
	 * getDataTypeMapping
	 * 
	 * @param datatype String
	 * @return DataTypeMappingItem
	 */
	public MapObject getTargetFromPreference(String datatype) {
		MapItem entry = preferenceConfigMap.get(datatype);

		if (entry == null) {
			return null;
		}

		return entry.getTarget();
	}

	/**
	 * getDataTypeMapping
	 * 
	 * @param datatype String
	 * @param precision Integer
	 * @param scale Integer
	 * @return DataTypeMappingItem
	 */
	public MapObject getTargetFromPreference(String datatype,
			Integer precision, Integer scale) {
		String key = getMapKey(datatype, precision.toString(), scale.toString());
		MapItem entry = preferenceConfigMap.get(key);
		if (entry == null) {
			return null;
		}
		return entry.getTarget();
	}

	/**
	 * return the XmlConfigMap
	 * 
	 * @return xmlConfigMap Map<String, MapItem>
	 */
	public Map<String, MapItem> getXmlConfigMap() {
		return xmlConfigMap;
	}

	/**
	 * get the TargetMappingItem
	 * 
	 * @param datatype String
	 * @param precision String
	 * @param scale String
	 * @return MapItem
	 */
	public MapItem getXmlConfigMapItem(String datatype, String precision,
			String scale) {

		String key = getMapKey(datatype, precision, scale);

		if (xmlConfigMap.containsKey(key)) {
			MapItem mapItem = xmlConfigMap.get(key);

			return mapItem;
		} else {
			LOG.error("ERROR:Can't find the key in defaultMap.The key is:"
					+ key);
		}

		return null;
	}

	/**
	 * return data type mapping from default
	 * 
	 * @return Map<String, MapItem>
	 */
	private Map<String, MapItem> loadDefault() {
		synchronized (this) {
			InputStream in = this.getClass().getResourceAsStream(
					dataTypeMappingFileName);
			try {
				if (in == null) {
					throw new RuntimeException();
				}
				IXMLMemento memento = XMLMemento.loadMemento(in);
				if (null != memento) {
					return loadFromXML(memento);
				}
			} catch (Exception ignored) {
				LOG.error("Initialize Data Type Mapping error:"
						+ dataTypeMappingFileName + ":"
						+ this.getClass().getName());

			}
			return new HashMap<String, MapItem>();
		}
	}

	/**
	 * 
	 * Load the data type mapping from xml
	 * 
	 * @param memento IXMLMemento
	 * @return the mapping map
	 */
	private Map<String, MapItem> loadFromPreference(IXMLMemento memento) {
		IXMLMemento[] children = memento.getChildren("DataTypeMapping");
		Map<String, MapItem> map = new HashMap<String, MapItem>();

		for (IXMLMemento dataTypeMemento : children) {
			IXMLMemento sourceDataTypeMemento = dataTypeMemento.getChild(TAG_SOURCE_DATA_TYPE);
			IXMLMemento targetDataTypeMemento = dataTypeMemento.getChild(TAG_TARGET_DATA_TYPE);

			String srcDataType = sourceDataTypeMemento.getChild(TAG_DATA_TYPE).getTextData().trim();
			String srcPrecision = sourceDataTypeMemento.getChild(TAG_PRECISION).getTextData();
			String srcScale = sourceDataTypeMemento.getChild(TAG_SCALE).getTextData();

			MapObject sourceItem = new MapObject();
			sourceItem.setDatatype(srcDataType);
			sourceItem.setPrecision(srcPrecision);
			sourceItem.setScale(srcScale);

			String targetDataType = targetDataTypeMemento.getChild(
					TAG_DATA_TYPE).getTextData().trim();
			String targetPrecision = targetDataTypeMemento.getChild(
					TAG_PRECISION).getTextData();
			String targetScale = targetDataTypeMemento.getChild(TAG_SCALE).getTextData();

			MapObject targetItem = new MapObject();
			targetItem.setDatatype(targetDataType);
			targetItem.setPrecision(targetPrecision);
			targetItem.setScale(targetScale);

			String key = getMapKey(srcDataType, srcPrecision == null ? ""
					: srcPrecision, srcScale == null ? "" : srcScale);
			map.put(key, new MapItem(this, sourceItem, targetItem));
		}

		return map;
	}

	/**
	 * load data type mapping from preference
	 * 
	 * @param xmlString String
	 * @throws UnsupportedEncodingException ex
	 */
	public void loadFromPreference(String xmlString) throws UnsupportedEncodingException {
		synchronized (this) {
			if (StringUtils.isBlank(xmlString)) {
				return;
			}
			//Validate XML
			if (xmlString.indexOf(name) < 0) {
				throw new RuntimeException(
						"Invalid mapping configuration file.");
			}
			ByteArrayInputStream inputStream = new ByteArrayInputStream(
					xmlString.getBytes("UTF-8"));
			IXMLMemento memento = XMLMemento.loadMemento(inputStream);
			if (null != memento) {
				setDataTypeMap(loadFromPreference(memento));
			} else {
				throw new RuntimeException(
						"Invalid mapping configuration file.");
			}
		}
	}

	/**
	 * 
	 * Load the data type mapping from xml
	 * 
	 * @param memento IXMLMemento
	 * @return the mapping map
	 */
	private Map<String, MapItem> loadFromXML(IXMLMemento memento) {
		IXMLMemento[] children = memento.getChildren("DataTypeMapping");
		Map<String, MapItem> configMap = new HashMap<String, MapItem>();

		try {
			for (IXMLMemento dataTypeMemento : children) {
				//create MapItem
				MapItem mapItem = new MapItem(this);

				IXMLMemento sourceDataTypeMemento = dataTypeMemento.getChild(TAG_SOURCE_DATA_TYPE);
				IXMLMemento[] targetDataTypeMementos = dataTypeMemento.getChildren(TAG_TARGET_DATA_TYPE);

				String srcDataType = sourceDataTypeMemento.getChild(
						TAG_DATA_TYPE).getTextData().trim();
				String srcPrecision = sourceDataTypeMemento.getChild(
						TAG_PRECISION).getTextData();
				String srcScale = sourceDataTypeMemento.getChild(TAG_SCALE).getTextData();

				MapObject sourceItem = new MapObject();
				sourceItem.setDatatype(srcDataType);
				sourceItem.setPrecision(srcPrecision);
				sourceItem.setScale(srcScale);

				//set sourceItem to mapItem
				mapItem.setSource(sourceItem);

				for (IXMLMemento targetDataTypeMemento : targetDataTypeMementos) {
					String targetDataType = targetDataTypeMemento.getChild(
							TAG_DATA_TYPE).getTextData().trim();
					String targetPrecision = targetDataTypeMemento.getChild(
							TAG_PRECISION).getTextData();
					String targetScale = targetDataTypeMemento.getChild(
							TAG_SCALE).getTextData();

					MapObject targetItem = new MapObject();
					targetItem.setDatatype(targetDataType);
					targetItem.setPrecision(targetPrecision);
					targetItem.setScale(targetScale);

					//add targetItem to mapItem
					mapItem.getAvailableTargetList().add(targetItem);
					if (mapItem.getTarget() == null) {
						mapItem.setTarget(targetItem);
					}
				}
				//process the key
				String key = getMapKey(srcDataType, srcPrecision, srcScale);

				configMap.put(key, mapItem);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return configMap;
	}

	/**
	 * restore default data type mapping
	 * 
	 * @return - preferenceConfigMap
	 */
	public final Map<String, MapItem> restoreDefault() {
		synchronized (this) {
			//clear the map and load from defaultMap
			preferenceConfigMap.clear();
			//init map from defaultMap
			Set<String> keys = xmlConfigMap.keySet();

			for (String key : keys) {
				MapItem itemMatrix = xmlConfigMap.get(key);
				preferenceConfigMap.put(
						key,
						new MapItem(this, itemMatrix.getSource(),
								itemMatrix.getFirstTarget()));
			}
		}
		return preferenceConfigMap;
	}

	//	/**
	//	 * get the SuggestTargetMappingItem that we advised
	//	 * 
	//	 * @param datatype String
	//	 * @param precision Integer
	//	 * @param scale Integer
	//	 * @return target MapObject
	//	 */
	//	public MapObject getSuggestTarget(String datatype, Integer precision,
	//			Integer scale) {
	//
	//		String key = getMapKey(datatype, precision.toString(), scale.toString());
	//
	//		return getSuggestTarget(key);
	//	}
	//
	//	/**
	//	 * get the SuggestTargetMappingItem that we advised
	//	 * 
	//	 * @param key String
	//	 * @return MapObject
	//	 */
	//	public MapObject getSuggestTarget(String key) {
	//
	//		if (xmlConfigMap.containsKey(key)) {
	//			MapItem mapItem = xmlConfigMap.get(key);
	//
	//			if (mapItem.getAvailableTargetList().size() > 0) {
	//				return mapItem.getAvailableTargetList().get(0);
	//			}
	//		} else {
	//			LOG.error("ERROR:Can't find the key in defaultMap.The key is:"
	//					+ key);
	//		}
	//
	//		return null;
	//	}

	/**
	 * 
	 * Set data type mapping
	 * 
	 * @param map the Map<String, MapItem>
	 */
	public void setDataTypeMap(Map<String, MapItem> map) {
		this.preferenceConfigMap.clear();
		if (map == null) {
			return;
		}
		this.preferenceConfigMap.putAll(map);
	}

	//	/**
	//	 * verify the char column
	//	 * 
	//	 * @param sourceColumn Column
	//	 * @param targetColumn Column
	//	 * @return VerifyInfo
	//	 */
	//	protected VerifyInfo validateChar(Column sourceColumn, Column targetColumn) {
	//		VerifyInfo info = null;
	//
	//		int sourcePrecision = sourceColumn.getPrecision();
	//
	//		double sourceCharsetByte = CharsetUtils.getCharsetByte(sourceColumn.getCharset());
	//		double targetCharsetByte = CharsetUtils.getCharsetByte(targetColumn.getCharset());
	//
	//		int factor = (int) Math.ceil(sourceCharsetByte / targetCharsetByte);
	//
	//		if (factor < 1) {
	//			factor = 1;
	//		}
	//
	//		if (targetCharsetByte * factor < Integer.valueOf(sourcePrecision)) {
	//			LOG.error("ERROR: The target precision should equal or greater than "
	//					+ targetCharsetByte * factor);
	//			info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
	//					"ERROR: The target precision should equal or greater than "
	//							+ targetCharsetByte * factor);
	//			return info;
	//		}
	//
	//		info = new VerifyInfo(VerifyInfo.TYPE_MATCH, "");
	//
	//		return info;
	//	}

}
