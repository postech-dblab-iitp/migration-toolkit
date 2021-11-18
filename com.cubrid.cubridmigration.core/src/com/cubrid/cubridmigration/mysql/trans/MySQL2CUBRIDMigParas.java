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
package com.cubrid.cubridmigration.mysql.trans;

import java.io.ByteArrayInputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.common.xml.IXMLMemento;
import com.cubrid.cubridmigration.core.common.xml.XMLMemento;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;

/**
 * 
 * MySQLtoCUBRIDMigrationParameters
 * 
 * @author moulingwang
 * @version 1.0 - 2010-12-15
 */
public final class MySQL2CUBRIDMigParas {
	public static final String UNPARSED_TIME = "unparsed_time";
	public static final String UNPARSED_DATE = "unparsed_date";
	public static final String UNPARSED_TIMESTAMP = "unparsed_timestamp";
	public static final String REPLAXE_CHAR0 = "replace_char0";

	public static final String DEFAULT_UNPARSED_TIME_VALUE = "00:00:00";
	public static final String DEFAULT_UNPARSED_DATE_VALUE = "0001-01-01";
	public static final String DEFAULT_UNPARSED_TIMESTAMP_VALUE = "1970-01-02 01:00:00.000";
	public static final String DEFAULT_REPLAXE_CHAR0_VALUE = "' '";

	private static Map<String, String> map = loadDefault();;

	/**
	 * return migration paramters
	 * 
	 * @param paramName
	 *        UNPARSED_TIME/UNPARSED_DATE/UNPARSED_TIMESTAMP/REPLAXE_CHAR0
	 * @param value to be saved
	 */
	public static void putMigrationParamter(String paramName, String value) {
		map.put(paramName, value);
	}

	/**
	 * return migration paramter by name
	 * 
	 * @param paramName
	 *        UNPARSED_TIME/UNPARSED_DATE/UNPARSED_TIMESTAMP/REPLAXE_CHAR0
	 * @return String
	 */
	public static String getMigrationParamter(String paramName) {
		return map.get(paramName);
	}

	/**
	 * return data type mapping from default
	 * 
	 * @return Map<String, MapItem>
	 */
	public static Map<String, String> loadDefault() {
		Map<String, String> defaultMap = new HashMap<String, String>();

		defaultMap.put(UNPARSED_TIME, DEFAULT_UNPARSED_TIME_VALUE);
		defaultMap.put(UNPARSED_TIMESTAMP, DEFAULT_UNPARSED_TIMESTAMP_VALUE);
		defaultMap.put(REPLAXE_CHAR0, DEFAULT_REPLAXE_CHAR0_VALUE);
		defaultMap.put(UNPARSED_DATE, DEFAULT_UNPARSED_DATE_VALUE);
		return defaultMap;
	}

	/**
	 * return the replaced data
	 * 
	 * @param dateValue String
	 * @param tz TimeZone
	 * @return String
	 */
	public static Date getReplacedDate(String dateValue, TimeZone tz) {
		try {
			if (null == dateValue) {
				return null;
			}

			long time = CUBRIDTimeUtil.getDateFormat("yyyy-MM-dd",
					Locale.ENGLISH,
					(tz == null ? TimeZone.getTimeZone("GMT") : tz)).parse(
					dateValue).getTime();
			return new Date(time);
		} catch (ParseException ignored) {
			return null;
		}
	}

	/**
	 * return the replaced time
	 * 
	 * @param timeValue String
	 * @param tz TimeZone
	 * @return String
	 */
	public static Time getReplacedTime(String timeValue, TimeZone tz) {
		try {
			if (null == timeValue) {
				return null;
			}

			long time = CUBRIDTimeUtil.getDateFormat("HH:mm:ss",
					Locale.ENGLISH,
					(tz == null ? TimeZone.getTimeZone("GMT") : tz)).parse(
					timeValue).getTime();
			return new Time(time);
		} catch (ParseException ignored) {
			return null;
		}
	}

	/**
	 * return the replaced timestamp
	 * 
	 * @param timestampValue String
	 * @param tz TimeZone
	 * @return Timestamp
	 */
	public static Timestamp getReplacedTimestamp(String timestampValue,
			TimeZone tz) {
		try {
			if (null == timestampValue) {
				return null;
			}

			DateFormat dateFormat = CUBRIDTimeUtil.getDateFormat(
					"yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH,
					(tz == null ? TimeZone.getTimeZone("GMT") : tz));

			long time = dateFormat.parse(timestampValue).getTime();
			return new Timestamp(time);
		} catch (ParseException ignored) {
			return null;
		}
	}

	private MySQL2CUBRIDMigParas() {
		//Do nothing
	}

	/**
	 * load data type mapping from preference
	 * 
	 * @param xmlString String
	 */
	public static void loadFromPreference(String xmlString) {
		if (StringUtils.isBlank(xmlString)) {
			return;
		}

		ByteArrayInputStream inputStream;
		try {
			inputStream = new ByteArrayInputStream(xmlString.getBytes("UTF-8"));
			IXMLMemento memento = XMLMemento.loadMemento(inputStream);
			if (memento != null) {
				map = load(memento);
			}

		} catch (Exception e) {
			return;
		}
	}

	/**
	 * restore default data type mapping
	 */
	public static void restoreDefault() {
		map = loadDefault();
	}

	/**
	 * 
	 * Load the data type mapping from xml
	 * 
	 * @param memento IXMLMemento
	 * @return the mapping map
	 */
	private static Map<String, String> load(IXMLMemento memento) {
		IXMLMemento[] children = memento.getChildren("MySQLtoCUBRIDMigrationParameters");
		Map<String, String> map = new HashMap<String, String>();

		for (IXMLMemento dataTypeMemento : children) {
			String timeValue = dataTypeMemento.getChild(UNPARSED_TIME).getTextData();
			String dateValue = dataTypeMemento.getChild(UNPARSED_DATE).getTextData();
			String timestampValue = dataTypeMemento.getChild(UNPARSED_TIMESTAMP).getTextData();
			String charValue = dataTypeMemento.getChild(REPLAXE_CHAR0).getTextData().trim();

			timeValue = StringUtils.isEmpty(timeValue) ? null : timeValue;
			timestampValue = StringUtils.isEmpty(timestampValue) ? null
					: timestampValue;
			charValue = StringUtils.isEmpty(charValue) ? DEFAULT_REPLAXE_CHAR0_VALUE
					: charValue;

			map.put(UNPARSED_TIME, timeValue);
			map.put(UNPARSED_DATE, dateValue);
			map.put(UNPARSED_TIMESTAMP, timestampValue);
			map.put(REPLAXE_CHAR0, charValue);
		}

		return map;
	}
}
