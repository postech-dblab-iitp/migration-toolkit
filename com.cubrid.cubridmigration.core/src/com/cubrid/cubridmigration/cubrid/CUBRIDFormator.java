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
package com.cubrid.cubridmigration.cubrid;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;

/**
 * CUBRID column value formatter
 *
 * @author moulinwang
 *
 */
public class CUBRIDFormator { //NOPMD
	private static final String DATE_FORMAT = "MM/dd/yyyy";
	private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	private static final String TIME_FORMAT = "HH:mm:ss";
	private static final String TIMESTAMP_FORMAT = "MM/dd/yyyy HH:mm:ss";

	private static Map<Integer, HashMap<String, String>> formatQueryMap = new HashMap<Integer, HashMap<String, String>>();
	private static final Logger LOG = LogUtil.getLogger(CUBRIDFormator.class);

	static {
		HashMap<String, String> dateTimeMap = new HashMap<String, String>();
		dateTimeMap.put("", "");
		dateTimeMap.put("sysdatetime", "sysdatetime");
		dateTimeMap.put("sys_datetime", "sysdatetime");
		dateTimeMap.put("current_datetime", "current_datetime");
		dateTimeMap.put("currentdatetime", "current_datetime");
		formatQueryMap.put(DataTypeConstant.CUBRID_DT_DATETIME, dateTimeMap);

		HashMap<String, String> timeStampMap = new HashMap<String, String>();
		timeStampMap.put("", "");
		timeStampMap.put("systimestamp", "systimestamp");
		timeStampMap.put("sys_timestamp", "systimestamp");
		timeStampMap.put("currenttimestamp", "current_timestamp");
		timeStampMap.put("current_timestamp", "current_timestamp");
		formatQueryMap.put(DataTypeConstant.CUBRID_DT_TIMESTAMP, timeStampMap);

		HashMap<String, String> dateMap = new HashMap<String, String>();
		dateMap.put("", "");
		dateMap.put("sysdate", "sysdate");
		dateMap.put("sys_date", "sysdate");
		dateMap.put("currentdate", "current_date");
		dateMap.put("current_date", "current_date");
		formatQueryMap.put(DataTypeConstant.CUBRID_DT_DATE, dateMap);

		HashMap<String, String> timeMap = new HashMap<String, String>();
		timeMap.put("", "");
		timeMap.put("systime", "systime");
		timeMap.put("sys_time", "systime");
		timeMap.put("currenttime", "current_time");
		timeMap.put("current_time", "current_time");
		formatQueryMap.put(DataTypeConstant.CUBRID_DT_TIME, timeMap);

	}

	/**
	 * to format customs' many types of attribute default value into standard
	 * attribute default value
	 *
	 * @param dti Integer
	 * @param columnValue String column default value
	 * @return String standard column default value
	 */
	public static FormatDataResult format(DataTypeInstance dti, String columnValue) {
		//check null
		CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
		if (columnValue == null) {
			return new FormatDataResult(null, true, null);
		}
		Integer typeID = dataTypeHelper.getCUBRIDDataTypeID(dti.getName());
		int scale = dti.getScale() == null ? 0 : dti.getScale();
		if (typeID == DataTypeConstant.CUBRID_DT_DATETIME) {
			return formatDataTime(columnValue);
		} else if (typeID == DataTypeConstant.CUBRID_DT_TIMESTAMP) {
			return formatTimeStamp(columnValue);
		} else if (typeID == DataTypeConstant.CUBRID_DT_DATE) {
			return formatDate(columnValue);
		} else if (typeID == DataTypeConstant.CUBRID_DT_TIME) {
			return formatTime(columnValue);
		} else if (typeID == DataTypeConstant.CUBRID_DT_CHAR
				|| typeID == DataTypeConstant.CUBRID_DT_VARCHAR) {
			return formatString(columnValue);
		} else if (typeID == DataTypeConstant.CUBRID_DT_INTEGER
				|| typeID == DataTypeConstant.CUBRID_DT_SMALLINT
				|| typeID == DataTypeConstant.CUBRID_DT_BIGINT
				|| typeID == DataTypeConstant.CUBRID_DT_NUMERIC && scale == 0) {
			return formatNumber(columnValue);
		} else if (typeID == DataTypeConstant.CUBRID_DT_NUMERIC && scale > 0
				|| typeID == DataTypeConstant.CUBRID_DT_FLOAT
				|| typeID == DataTypeConstant.CUBRID_DT_DOUBLE
				|| typeID == DataTypeConstant.CUBRID_DT_MONETARY) {
			return formatFloatNumber(columnValue);
		} else if (typeID == DataTypeConstant.CUBRID_DT_NCHAR
				|| typeID == DataTypeConstant.CUBRID_DT_NVARCHAR) {
			return formatNationalString(columnValue);
		} else if (typeID == DataTypeConstant.CUBRID_DT_BIT
				|| typeID == DataTypeConstant.CUBRID_DT_VARBIT) {
			return formatBytes(columnValue);
		} else if (typeID == DataTypeConstant.CUBRID_DT_ENUM) {
			return formatEnum(dti.getElments(), columnValue);
		} else if (dataTypeHelper.isCollection(dti.getName())) {
			return formatCollections(dti, scale, columnValue);
		}

		return new FormatDataResult(null, false, null);
	}

	/**
	 *
	 * @param elments like 1,2,3,4
	 * @param columnValue ENUM value
	 * @return formated ENUM value
	 */
	private static FormatDataResult formatEnum(String elments, String columnValue) {
		if (StringUtils.isBlank(elments) || columnValue == null) {
			return new FormatDataResult(null, true, null);
		}
		String[] eleArray = elments.split(",");
		final String quotedValue;
		if (isValueQuoted(columnValue)) {
			quotedValue = columnValue;
		} else {
			quotedValue = "'" + columnValue + "'";
		}
		for (String str : eleArray) {
			if (str.trim().equals(quotedValue)) {
				return new FormatDataResult(quotedValue, true, null);
			}
		}
		return new FormatDataResult(columnValue, true, null);
	}

	/**
	 * @param value to be judged
	 * @return true if like 'xxx'
	 */
	protected static boolean isValueQuoted(String value) {
		return value.startsWith("'") && value.endsWith("'");
	}

	/**
	 * format bytes
	 *
	 * @param columnValue String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatBytes(String columnValue) {
		if (columnValue.startsWith("B'") && columnValue.endsWith("'")
				|| columnValue.startsWith("X'") && columnValue.endsWith("'")) {
			return new FormatDataResult(columnValue, true, null);
		} else if (StringUtils.isEmpty(columnValue)) {
			return new FormatDataResult("B'0'", true, null);
		} else {
			String formatResult = "X'" + columnValue + "'";
			return new FormatDataResult(formatResult, true, null);
		}
	}

	/**
	 * format collection values
	 *
	 * @param dti Should be the full type DataTypeInstance, not the sub
	 *        DataTypeInstance.
	 * @param scale Integer
	 * @param columnValue String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatCollections(DataTypeInstance dti, Integer scale,
			String columnValue) {
		if (columnValue.startsWith("{") && columnValue.endsWith("}")) {
			return new FormatDataResult(columnValue, true, null);
		} else if (StringUtils.isEmpty(columnValue)) {
			return new FormatDataResult(null, true, null);
		}
		DataTypeInstance subDTI = dti.getSubType();
		StringBuffer bf = new StringBuffer();

		if (-1 == columnValue.indexOf(',')) {
			FormatDataResult result2 = format(subDTI, columnValue);
			if (result2.success) {
				bf.append(result2.formatResult);
			} else {
				return new FormatDataResult(null, false, result2.throwable);
			}
		} else {
			String[] values = columnValue.split(",");

			for (int j = 0; j < values.length; j++) {
				String value = values[j];

				if (j > 0) {
					bf.append(',');
				}

				FormatDataResult result2 = format(subDTI, value);

				if (result2.success) {
					bf.append(result2.formatResult);
				} else {
					return new FormatDataResult(null, false, result2.throwable);
				}
			}
		}
		String formatResult = "{" + bf.toString() + "}";
		return new FormatDataResult(formatResult, true, null);
	}

	/**
	 * format datetime
	 *
	 * @param data String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatDataTime(String data) {
		//query in table
		HashMap<String, String> dateTimeMap = formatQueryMap.get(DataTypeConstant.CUBRID_DT_DATETIME);
		String formatValue = dateTimeMap.get(data.trim().toLowerCase(Locale.ENGLISH));

		if (formatValue != null) {
			String formatResult = StringUtils.isEmpty(formatValue) ? null : formatValue;
			return new FormatDataResult(formatResult, true, null);
		}

		String datetimeString = data;

		if (data.trim().toLowerCase(Locale.ENGLISH).startsWith("datetime")) {
			String str = data.trim().substring("datetime".length()).trim();

			if (!StringUtils.isEmpty(str) && '\'' == str.charAt(0) && str.endsWith("'")
					&& str.length() > 2) {
				datetimeString = str.substring(1, str.length() - 1);
			}
		}

		//check whether it is  a long data type
		try {
			long datetime = Long.parseLong(datetimeString);
			String formatResult = "DATETIME'"
					+ CUBRIDTimeUtil.formatDateTime(datetime, DATETIME_FORMAT,
							TimeZone.getDefault()) + "'";
			return new FormatDataResult(formatResult, true, null);
		} catch (NumberFormatException ignord) {
			ignord.getMessage();
		}

		try {
			String formatValue2 = CUBRIDTimeUtil.formatDateTime(datetimeString, DATETIME_FORMAT,
					TimeZone.getDefault());

			if (formatValue2 != null) {
				datetimeString = formatValue2;
			}

			long datetime = CUBRIDTimeUtil.parseDatetime2Long(datetimeString, TimeZone.getDefault());
			String formatResult = "DATETIME'"
					+ CUBRIDTimeUtil.formatDateTime(datetime, DATETIME_FORMAT,
							TimeZone.getDefault()) + "'";
			return new FormatDataResult(formatResult, true, null);
		} catch (ParseException e) {
			if (CUBRIDTimeUtil.validateDateString(datetimeString, "HH:mm:ss mm/dd")
					|| CUBRIDTimeUtil.validateDateString(datetimeString, "mm/dd HH:mm:ss")
					|| CUBRIDTimeUtil.validateDateString(datetimeString, "hh:mm:ss a mm/dd")
					|| CUBRIDTimeUtil.validateDateString(datetimeString, "mm/dd hh:mm:ss a")) {
				String formatResult = "DATETIME'" + datetimeString + "'";
				return new FormatDataResult(formatResult, true, null);
			}
		}

		return new FormatDataResult(null, false, null);
	}

	/**
	 * format date
	 *
	 * @param data String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatDate(String data) {
		//query in table
		HashMap<String, String> dateMap = formatQueryMap.get(DataTypeConstant.CUBRID_DT_DATE);
		String formatValue = dateMap.get(data.trim().toLowerCase(Locale.ENGLISH));

		if (formatValue != null) {
			String formatResult = StringUtils.isEmpty(formatValue) ? null : formatValue;
			return new FormatDataResult(formatResult, true, null);
		}

		String dateString = data;

		if (data.toLowerCase(Locale.ENGLISH).startsWith("date")) {
			String str = data.trim().substring("date".length()).trim();

			if (!StringUtils.isEmpty(str) && '\'' == str.charAt(0) && str.endsWith("'")
					&& str.length() > 2) {
				dateString = str.substring(1, str.length() - 1);
			}
		}

		try {
			long timestamp = CUBRIDTimeUtil.parseDate2Long(dateString, TimeZone.getDefault());
			String formatResult = "DATE'"
					+ CUBRIDTimeUtil.formatTimestampLong(timestamp, DATE_FORMAT,
							TimeZone.getDefault()) + "'";
			return new FormatDataResult(formatResult, true, null);
		} catch (ParseException e) {
			if (CUBRIDTimeUtil.validateDateString(dateString, "MM/dd")) {
				String formatResult = "DATE'" + dateString + "'";
				return new FormatDataResult(formatResult, true, null);
			}
		}

		return new FormatDataResult(null, false, null);
	}

	/**
	 * format float number
	 *
	 * @param columnValue String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatFloatNumber(String columnValue) {
		try {
			Double.parseDouble(columnValue);
		} catch (NumberFormatException e) {
			return new FormatDataResult(null, false, e);
		}

		return new FormatDataResult(columnValue, true, null);
	}

	/**
	 * format nationla string
	 *
	 * @param columnValue String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatNationalString(String columnValue) {
		if (columnValue.startsWith("N'") && columnValue.endsWith("'")
				|| StringUtils.isEmpty(columnValue)) {
			return new FormatDataResult(columnValue, true, null);
		} else {
			String formatResult = "N'" + columnValue.replaceAll("'", "''") + "'";
			return new FormatDataResult(formatResult, true, null);
		}
	}

	/**
	 * format number
	 *
	 * @param columnValue String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatNumber(String columnValue) {
		try {
			Long.parseLong(columnValue);
		} catch (NumberFormatException e) {
			return new FormatDataResult(null, false, e);
		}

		return new FormatDataResult(columnValue, true, null);
	}

	/**
	 * format string
	 *
	 * @param columnValue String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatString(String columnValue) {
		if (StringUtils.isEmpty(columnValue)) {
			return new FormatDataResult("''", true, null);
		} else if (columnValue.startsWith("'") && columnValue.endsWith("'")
				&& columnValue.length() > 1) {
			return new FormatDataResult(columnValue, true, null);
		} else {
			String formatResult = "'" + columnValue.replaceAll("'", "''") + "'";
			return new FormatDataResult(formatResult, true, null);
		}
	}

	/**
	 * format time
	 *
	 * @param data String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatTime(String data) {
		//query in table
		HashMap<String, String> dateMap = formatQueryMap.get(DataTypeConstant.CUBRID_DT_TIME);
		String dateMapKey = data.trim().toLowerCase(Locale.ENGLISH);
		String dateMapValue = dateMap.get(dateMapKey);

		if (dateMapValue != null) {
			String formatResult = StringUtils.isEmpty(dateMapValue) ? null : dateMapValue;
			return new FormatDataResult(formatResult, true, null);
		}

		String timeString = data;
		if (data.toLowerCase(Locale.ENGLISH).startsWith("time")) {
			String str = data.trim().substring("time".length()).trim();

			if (!StringUtils.isEmpty(str) && '\'' == str.charAt(0) && str.endsWith("'")
					&& str.length() > 2) {
				timeString = str.substring(1, str.length() - 1);
			}
		}

		try {
			long timestamp = Long.parseLong(timeString);
			String formatResult = "TIME'"
					+ CUBRIDTimeUtil.formatTimestampLong(timestamp, TIME_FORMAT,
							TimeZone.getDefault()) + "'";
			return new FormatDataResult(formatResult, true, null); //NOPMD
		} catch (NumberFormatException ignored) {
			ignored.getMessage();
		}

		try {
			long timestamp = CUBRIDTimeUtil.parseTime2Long(timeString, TimeZone.getDefault());
			String formatResult = "TIME'"
					+ CUBRIDTimeUtil.formatTimestampLong(timestamp, TIME_FORMAT,
							TimeZone.getDefault()) + "'";
			return new FormatDataResult(formatResult, true, null); //NOPMD
		} catch (ParseException ignored) { //NOPMD
			ignored.getMessage();
		}

		return new FormatDataResult(null, false, null);
	}

	/**
	 * format time stamp
	 *
	 * @param data String
	 * @return FormatDataResult
	 */
	private static FormatDataResult formatTimeStamp(String data) {

		HashMap<String, String> timeStampMap = formatQueryMap.get(DataTypeConstant.CUBRID_DT_TIMESTAMP);

		//query in table
		String formatValue = timeStampMap.get(data.trim().toLowerCase(Locale.ENGLISH));

		if (formatValue != null) {
			String formatResult = StringUtils.isEmpty(formatValue) ? null : formatValue;
			return new FormatDataResult(formatResult, true, null);
		}

		String timestampString = data;
		if (data.toLowerCase(Locale.ENGLISH).startsWith("timestamp")) {
			String str = data.trim().substring("timestamp".length()).trim();

			if (!StringUtils.isEmpty(str) && '\'' == str.charAt(0) && str.endsWith("'")
					&& str.length() > 2) {
				timestampString = str.substring(1, str.length() - 1);
			}
		}

		try {
			long timestamp = Long.parseLong(timestampString);
			String formatResult = "TIMESTAMP'"
					+ CUBRIDTimeUtil.formatTimestampLong(timestamp, TIMESTAMP_FORMAT,
							TimeZone.getDefault()) + "'";
			return new FormatDataResult(formatResult, true, null);
		} catch (NumberFormatException ignored) {
			ignored.getMessage();
		}
		try {
			long timestamp = CUBRIDTimeUtil.parseTimestamp(timestampString, TimeZone.getDefault());
			String formatResult = "TIMESTAMP'"
					+ CUBRIDTimeUtil.formatTimestampLong(timestamp, TIMESTAMP_FORMAT,
							TimeZone.getDefault()) + "'";
			return new FormatDataResult(formatResult, true, null);
		} catch (ParseException e) {
			if (CUBRIDTimeUtil.validateDateString(timestampString, "HH:mm:ss mm/dd")
					|| CUBRIDTimeUtil.validateDateString(timestampString, "mm/dd HH:mm:ss")
					|| CUBRIDTimeUtil.validateDateString(timestampString, "hh:mm:ss a mm/dd")
					|| CUBRIDTimeUtil.validateDateString(timestampString, "mm/dd hh:mm:ss a")) {
				String formatResult = "TIMESTAMP'" + timestampString + "'";
				return new FormatDataResult(formatResult, true, null);
			}
		}

		return new FormatDataResult(null, false, null);
	}

	/**
	 * return Object[] array value from a collection value based the given data
	 * type, eg: data type: integer, collection value: {1,2,3} return Object[]:
	 * Integer[]{1,2,3}
	 *
	 * @param dti Full type DataTypeInstance
	 * @param value value with format which is like {xxx,xxx,xxx}
	 *
	 * @return Object[]
	 * @throws ParseException p
	 * @throws NumberFormatException e
	 */
	public static Object[] getCollectionValues(DataTypeInstance dti, String value) throws NumberFormatException,
			ParseException {
		String[] values = getStringValues(value);
		if (values == null) {
			return null;
		}
		Object[] ret;

		CUBRIDDataTypeHelper dtHelper = CUBRIDDataTypeHelper.getInstance(null);
		int componentDataType = dtHelper.getCUBRIDDataTypeID(dti.getSubType().getName());
		int scale = dti.getScale() == null ? 0 : dti.getScale();
		if (componentDataType == DataTypeConstant.CUBRID_DT_SMALLINT
				|| componentDataType == DataTypeConstant.CUBRID_DT_INTEGER) {
			ret = toIntegerArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_BIGINT) {
			ret = toLongArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_NUMERIC && scale == 0) {
			ret = toLongArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_FLOAT) {
			ret = toDoubleArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_DOUBLE
				|| componentDataType == DataTypeConstant.CUBRID_DT_MONETARY) {
			ret = toDoubleArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_NUMERIC && scale > 0) {
			ret = toDoubleArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_CHAR
				|| componentDataType == DataTypeConstant.CUBRID_DT_VARCHAR) {
			ret = toStringArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_TIME) {
			ret = toTimeArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_DATE) {
			ret = toDateArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_TIMESTAMP) {
			ret = toTimestampArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_DATETIME) {
			ret = toDatetimeArray(values);
		} else if (componentDataType == DataTypeConstant.CUBRID_DT_BIT
				|| componentDataType == DataTypeConstant.CUBRID_DT_VARBIT) {
			ret = toByteArray(dti, values);
		} else {
			ret = toStringArray(values);
		}
		return ret;
	}

	/**
	 * get values of String
	 *
	 * @param value String
	 * @return String[]
	 */
	private static String[] getStringValues(String value) {
		String strs = value;

		if ('{' == value.charAt(0) && value.endsWith("}")) {
			strs = value.substring(1, value.length() - 1);
		} else if ('[' == value.charAt(0) && value.endsWith("]")) {
			//If the string is from List.toString
			strs = value.substring(1, value.length() - 1);
		}

		CSVReader reader = new CSVReader(new StringReader(strs));

		String[] values = new String[0];
		try {
			values = reader.readNext();
			reader.close();
		} catch (IOException ignored) {
			LOG.error(ignored.getMessage());
		}
		return values;
	}

	/**
	 * to Byte array
	 *
	 * @param dti Full type DataTypeInstance
	 * @param values String[]
	 * @return Object[]
	 */
	private static Object[] toByteArray(DataTypeInstance dti, String[] values) {
		Object[] ret;
		ret = new String[values.length];

		for (int i = 0; i < values.length; i++) {
			ret[i] = format(dti.getSubType(), values[i].trim()).getFormatResult();
		}
		return ret;
	}

	/**
	 * to date array
	 *
	 * @param values String[]
	 * @return Object[]
	 */
	private static Object[] toDateArray(String[] values) {
		Object[] ret;
		ret = new java.sql.Date[values.length];

		for (int i = 0; i < values.length; i++) {
			ret[i] = java.sql.Date.valueOf(values[i].trim());
		}
		return ret;
	}

	/**
	 * to datetime array
	 *
	 * @param values String[]
	 * @return Object[]
	 * @throws ParseException e
	 */
	private static Object[] toDatetimeArray(String[] values) throws ParseException {
		Object[] ret;
		ret = new java.sql.Timestamp[values.length];

		for (int i = 0; i < values.length; i++) {
			String formatValue = CUBRIDTimeUtil.formatDateTime(values[i], DATETIME_FORMAT,
					TimeZone.getDefault());

			if (formatValue == null) {
				formatValue = values[i];
			}

			long time = CUBRIDTimeUtil.parseDatetime2Long(formatValue, TimeZone.getDefault());
			java.sql.Timestamp timestamp = new java.sql.Timestamp(time);
			ret[i] = timestamp;
		}
		return ret;
	}

	/**
	 * to Double array
	 *
	 * @param values String[]
	 * @return Object[]
	 */
	private static Object[] toDoubleArray(String[] values) {
		Object[] ret;
		ret = new Double[values.length];

		for (int i = 0; i < values.length; i++) {
			ret[i] = Double.valueOf(values[i].trim());
		}
		return ret;
	}

	/**
	 * to Integer array
	 *
	 * @param values String[]
	 * @return Object[]
	 */
	private static Object[] toIntegerArray(String[] values) {
		Object[] ret = new Integer[values.length];

		for (int i = 0; i < values.length; i++) {
			ret[i] = Integer.valueOf(values[i].trim());
		}
		return ret;
	}

	/**
	 * to Long array
	 *
	 * @param values String[]
	 * @return Object[]
	 */
	private static Object[] toLongArray(String[] values) {
		Object[] ret;
		ret = new Long[values.length];

		for (int i = 0; i < values.length; i++) {
			ret[i] = Long.valueOf(values[i].trim());
		}
		return ret;
	}

	/**
	 * to String array
	 *
	 * @param values String[]
	 * @return Object[]
	 */
	private static Object[] toStringArray(String[] values) {
		Object[] ret;
		ret = new String[values.length];

		for (int i = 0; i < values.length; i++) {
			ret[i] = values[i];
		}
		return ret;
	}

	/**
	 * to time array
	 *
	 * @param values String[]
	 * @return Object[]
	 */
	private static Object[] toTimeArray(String[] values) {
		Object[] ret;
		ret = new java.sql.Time[values.length];

		for (int i = 0; i < values.length; i++) {
			ret[i] = java.sql.Time.valueOf(values[i].trim());
		}
		return ret;
	}

	/**
	 * to timestamp array
	 *
	 * @param values String[]
	 * @return Object[]
	 * @throws ParseException e
	 */
	private static Object[] toTimestampArray(String[] values) throws ParseException {
		Object[] ret;
		ret = new java.sql.Timestamp[values.length];

		for (int i = 0; i < values.length; i++) {
			long time = CUBRIDTimeUtil.parseDatetime2Long(values[i].trim(), TimeZone.getDefault());
			java.sql.Timestamp timestamp = new java.sql.Timestamp(time);
			ret[i] = timestamp;
		}
		return ret;
	}

	//	/**
	//	 * try to validate whether attribute value is aligned with the given data
	//	 * type
	//	 *
	//	 * @param dataType String column data type
	//	 * @param componentDataType String
	//	 * @param scale Integer
	//	 * @param columnValue column value
	//	 * @return boolean
	//	 */
	//	public static boolean validateColumnValue(String dataType,
	//			String componentDataType, Integer scale, String columnValue) {
	//		return formatValue(dti, columnValue).success;
	//	}
}
