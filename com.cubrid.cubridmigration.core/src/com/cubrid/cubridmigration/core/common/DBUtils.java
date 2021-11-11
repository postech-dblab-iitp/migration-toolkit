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
package com.cubrid.cubridmigration.core.common;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.Table;

/**
 * 
 * about DB operation method
 * 
 * @author moulinwang
 * @author JessieHuang
 * @version 1.0 - 2009-9-18
 */
public final class DBUtils {

	private static Set<String> supportPartitionTypes;
	private static Map<String, String> supportPartitionExp;
	private static Map<String, String> supportCubridDataType;

	private DBUtils() {
		//do nothing
	}

	static {
		supportPartitionTypes = new HashSet<String>();
		supportPartitionTypes.add(PartitionInfo.PARTITION_METHOD_RANGE);
		supportPartitionTypes.add(PartitionInfo.PARTITION_METHOD_LIST);
		supportPartitionTypes.add(PartitionInfo.PARTITION_METHOD_HASH);

		supportPartitionExp = new HashMap<String, String>();
		supportPartitionExp.put("ABS", "ABS");
		supportPartitionExp.put("CEILING", "CEIL");
		supportPartitionExp.put("FLOOR", "FLOOR");
		supportPartitionExp.put("MOD", "MOD");
		supportPartitionExp.put("EXTRACT_YEAR", "EXTRACT_YEAR");
		supportPartitionExp.put("EXTRACT_MONTH", "EXTRACT_MONTH");
		supportPartitionExp.put("EXTRACT_DAY", "EXTRACT_DAY");
		supportPartitionExp.put("EXTRACT_HOUR", "EXTRACT_HOUR");
		supportPartitionExp.put("EXTRACT_MINUTE", "EXTRACT_MINUTE");
		supportPartitionExp.put("EXTRACT_SECOND", "EXTRACT_SECOND");

		supportCubridDataType = new HashMap<String, String>();
		supportCubridDataType.put("CHARACTER", "");
		supportCubridDataType.put("CHARACTER VARYING", "");
		supportCubridDataType.put("NATIONAL CHARACTER", "");
		supportCubridDataType.put("NATIONAL CHARACTER VARYING", "");
		supportCubridDataType.put("INTEGER", "ABS,CEIL,FLOOR,MOD");
		supportCubridDataType.put("BIGINT", "ABS,CEIL,FLOOR,MOD");
		supportCubridDataType.put("SMALLINT", "ABS,CEIL,FLOOR,MOD");
		supportCubridDataType.put("DATE",
				"EXTRACT_YEAR,EXTRACT_MONTH,EXTRACT_DAY");
		supportCubridDataType.put("TIME",
				"EXTRACT_HOUR,EXTRACT_MINUTE,EXTRACT_SECOND");
		supportCubridDataType.put(
				"DATETIME",
				"EXTRACT_YEAR,EXTRACT_MONTH,EXTRACT_DAY,EXTRACT_HOUR,EXTRACT_MINUTE,EXTRACT_SECOND");
		supportCubridDataType.put(
				"TIMESTAMP",
				"EXTRACT_YEAR,EXTRACT_MONTH,EXTRACT_DAY,EXTRACT_HOUR,EXTRACT_MINUTE,EXTRACT_SECOND");
	}

	public static DateFormat getDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS z", Locale.ENGLISH);
	}

	/**
	 * getCubridPartitionExp
	 * 
	 * @param partition Partition
	 * @return String
	 */
	public static String getCubridPartitionExp(PartitionInfo partition) {
		String funcStr = partition.getPartitionFunc();

		String columnName = partition.getPartitionColumns().get(0).getName();

		if (supportPartitionExp.containsKey(funcStr)) {
			String newExp;

			if (funcStr.startsWith("EXTRACT_")) {
				newExp = supportPartitionExp.get(funcStr);
				String dateExp = newExp.substring(funcStr.indexOf('_') + 1);
				newExp = "EXTRACT(" + dateExp + " FROM " + columnName + ")";
			} else {
				if ("MOD".equals(funcStr)) {
					newExp = partition.getPartitionExp(); // process in MySQL2CUBRID.resetColumnsAndIndexes()
				} else {
					newExp = supportPartitionExp.get(funcStr) + "("
							+ columnName + ")";
				}

			}

			return newExp;
		} else {
			return funcStr + "(" + columnName + ")";
		}
	}

	/**
	 * supportedCubridPartitionDataType
	 * 
	 * @param cubridPartition Partition
	 * @return boolean
	 */
	public static boolean supportedCubridPartition(PartitionInfo cubridPartition) {
		return cubridPartition != null
				&& StringUtils.isNotEmpty(cubridPartition.getDDL());
	}

	/**
	 * get partition Columns
	 * 
	 * @param table Table
	 * @param exp String
	 * @return int
	 */
	public static List<Column> parsePartitionColumns(Table table, String exp) {
		if (StringUtils.isBlank(exp)) {
			return null;
		}

		List<Column> list = new ArrayList<Column>();
		String newExp = exp;
		int beginPos = newExp.indexOf('('); //>0 the first char is not '('
		int endPos = newExp.indexOf(')');

		String colNameStr;
		if (beginPos > 0 && endPos > 0 && endPos > beginPos) {
			colNameStr = newExp.substring(beginPos + 1, endPos).trim();

			if (newExp.startsWith("EXTRACT")) { //EXTRACT(unit FROM date) -->unit FROM date-->date
				colNameStr = colNameStr.trim().substring(
						colNameStr.trim().lastIndexOf(' ') + 1);
			} else if (colNameStr.startsWith("cast")) {
				colNameStr = colNameStr.trim().substring(5,
						colNameStr.trim().indexOf(' '));
			}
		} else {
			colNameStr = newExp;
		}

		String[] names = colNameStr.split(",");

		//******** column name: t1   exp=year(T1)
		//******** but  t1 = T1
		//******** can not use:::table.getColumnByName(columnName);

		Map<String, Column> map = new HashMap<String, Column>();

		for (Column column : table.getColumns()) {
			map.put(column.getName(), column); //UPPER
		}

		for (String str : names) {
			//column add '[]' ,such as [column] so cut the '[]'
			if (str.startsWith("[") && str.endsWith("]")) {
				str = str.substring(1, str.length() - 1);
			}
			if (map.containsKey(str.trim())) { //UPPER
				list.add(map.get(str.trim())); //UPPER
			}
		}

		return list;
	}

	/**
	 * parsePartitionFunc
	 * 
	 * @param exp String
	 * @return String
	 */
	public static String parsePartitionFunc(String exp) {
		if (exp == null) {
			return null;
		}

		String newExp = exp;

		int beginPos = newExp.indexOf('('); //>0 the first char is not '('
		int endPos = newExp.indexOf(')');

		if (beginPos > 0 && endPos > 0 && endPos > beginPos) {
			if (newExp.startsWith("EXTRACT")) { //EXTRACT(unit  FROM date) -->unit  FROM date-->unit
				String tmp = newExp.substring(beginPos + 1, endPos);
				String dateExp = tmp.trim().substring(0,
						tmp.trim().indexOf(' '));
				return "EXTRACT_" + dateExp;
			} else {
				return newExp.substring(0, beginPos);
			}
		} else {
			return null;
		}
	}

	/**
	 * Return HEX String of byte array
	 * 
	 * @param bytes byte[]
	 * @param len the byte count reading from parameter "bytes",it should not be
	 *        less then the length of parameter "bytes"
	 * @return String like '0a01ff00'
	 */
	public static String getBitString(byte[] bytes, int len) {
		StringBuffer bf = new StringBuffer();

		for (int i = 0; i < len; i++) {
			byte b1 = bytes[i];
			int value = b1 & 0x00ff;
			String hs = Integer.toHexString(value);
			if (hs.length() == 1) {
				bf.append("0").append(hs);
			} else {
				bf.append(hs);
			}
		}
		return bf.toString();
	}

	/**
	 * This is for record values that are instanceof Reader. Returns the String
	 * as read from the Reader.
	 * 
	 * @param reader Reader
	 * @return String
	 * @throws IOException e
	 */
	public static String reader2String(Reader reader) throws IOException {

		StringWriter sw = new StringWriter();
		int c;

		while ((c = reader.read()) != -1) {
			sw.write(c);
		}

		Closer.close(sw);
		Closer.close(reader);
		return sw.toString();
	}

	/**
	 * rollback
	 * 
	 * @param conn Connection
	 */
	public static void rollback(Connection conn) {
		try {
			if (conn == null) {
				return;
			}
			conn.rollback();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * commit
	 * 
	 * @param conn Connection
	 */
	public static void commit(Connection conn) {
		try {
			if (conn == null || conn.isClosed()) {
				return;
			}
			conn.commit();
		} catch (SQLException e) {
			throw new RuntimeException("Committed error:" + e.getMessage(), e);
		}
	}

	/**
	 * return identity string for multi-threads
	 * 
	 * @return String
	 */
	public static String getIdentity() {
		synchronized (DBUtils.class) {
			return UUID.randomUUID().toString();
		}
	}
}
