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

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Time util
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-18
 */
public class TimeZoneUtils { //NOPMD

	private static List<String> allTimeZoneIDs = Arrays.asList(TimeZone.getAvailableIDs());
	private static Map<String, StringBuffer> normtoIDsMap;

	public static Map<String, StringBuffer> getNormtoIDsMap() {
		return normtoIDsMap;
	}

	private static Map<String, String> id2NormMap;

	static {
		id2NormMap = new TreeMap<String, String>();
		normtoIDsMap = new TreeMap<String, StringBuffer>();

		for (String tzID : allTimeZoneIDs) {
			String normTimeZone = getGMTFormat(TimeZone.getTimeZone(tzID));
			id2NormMap.put(tzID, normTimeZone);

			if (normtoIDsMap.containsKey(normTimeZone)) {
				StringBuffer tmp = normtoIDsMap.get(normTimeZone);

				if (tmp.length() > 100) {
					if (tmp.indexOf("...") > 0) {
						continue;
					} else {
						tmp.append("...");
					}
				} else {
					tmp.append(",").append(tzID);
				}

				normtoIDsMap.put(normTimeZone, tmp);
			} else {
				StringBuffer value = new StringBuffer(101);
				value.append(normTimeZone).append("--").append(tzID);
				normtoIDsMap.put(normTimeZone, value);
			}
		}
	}

	/**
	 * getTimeZonesList
	 * 
	 * @return List<String>
	 */
	public static List<String> getTimeZonesList() {
		List<String> list = new ArrayList<String>();
		for (Entry<String, StringBuffer> en : normtoIDsMap.entrySet()) {
			list.add(en.getValue().toString());
		}
		return list;
	}

	/**
	 * get special time zone offset
	 * 
	 * @param tm String
	 * @return String
	 */
	public static String getGMTByDisplay(String tm) {
		String gmt = null;
		if (null == tm || "Default".equals(tm)) {
			return getDefaultID2GMT();
		}
		for (Entry<String, StringBuffer> en : normtoIDsMap.entrySet()) {
			String enKey = en.getValue().toString();
			if (enKey.indexOf(tm) >= 0 || enKey.equalsIgnoreCase(tm)) {
				gmt = en.getKey();
				break;
			}
		}

		return gmt;
	}

	/**
	 * getDisplayByGMT
	 * 
	 * @param gmtStr String
	 * @return String
	 */
	public static String getDisplayByGMT(String gmtStr) {
		return normtoIDsMap.get(gmtStr) == null ? null : normtoIDsMap.get(
				gmtStr).toString();
	}

	/**
	 * get spec timezone offset
	 * 
	 * @param tm String
	 * @return String
	 */
	public static String getGMTFormat(String tm) {
		TimeZone timeZone;
		if (tm == null) {
			timeZone = TimeZone.getDefault();
		} else {
			timeZone = TimeZone.getTimeZone(tm);
		}
		return getGMTFormat(timeZone);
	}

	/**
	 * return timezone for a given offset
	 * 
	 * @param rawOffset int
	 * @return String
	 */
	public static String getGMTFormat(int rawOffset) {
		int hour = rawOffset / (1000 * 60 * 60);
		NumberFormat format = new DecimalFormat("00':00'");

		return hour >= 0 ? "GMT+" + format.format(hour) : "GMT"
				+ format.format(hour);
	}

	/**
	 * Get the GMT format time zone string from hour offset.
	 * 
	 * @param offset of hours
	 * @return GMT format time zone string
	 */
	public static String getTZFromOffset(int offset) {
		String timezone;
		String prefix;
		if (offset >= 0) {
			prefix = "+";
		} else {
			prefix = "-";
		}
		String tmp = prefix + new DecimalFormat("00").format(Math.abs(offset));
		timezone = "GMT" + tmp + ":00";
		return timezone;
	}

	/**
	 * get spec timezone offset
	 * 
	 * @param tz TimeZone
	 * @return String
	 */
	private static String getGMTFormat(TimeZone tz) {
		return getGMTFormat(tz.getRawOffset());
	}

	/**
	 * default timezone to GMT
	 * 
	 * @return string
	 */
	public static String getDefaultID2GMT() {
		return getGMTFormat(TimeZone.getDefault().getID());
	}

	/**
	 * isValidTimeZone
	 * 
	 * @param timezone String
	 * @return boolean
	 */
	public static boolean isValidTimeZone(final String timezone) {
		return normtoIDsMap.containsKey(timezone);
	}

	/**
	 * return oralce support timezone string
	 * 
	 * @param gmt String
	 * @return String
	 */
	public static String getOracleTZID(String gmt) {
		String sign = "+".equals(gmt.substring(3, 4)) ? "-" : "+";
		int offset = Integer.parseInt(gmt.substring(4, 6));
		return "Etc/GMT" + sign + offset;
	}

	/**
	 * Transform the time in milliseconds to the time in "dd hh:mm:ss.SSS"
	 * format.
	 * 
	 * @param ms milliseconds
	 * @return dd hh:mm:ss.SSS
	 */
	public static String format(long ms) {
		int ss = 1000;
		int mi = ss * 60;
		int hh = mi * 60;
		int dd = hh * 24;

		long day = ms / dd;
		long hour = (ms - day * dd) / hh;
		long minute = (ms - day * dd - hour * hh) / mi;
		long second = (ms - day * dd - hour * hh - minute * mi) / ss;
		long milliSecond = ms - day * dd - hour * hh - minute * mi - second
				* ss;

		String strDay = day < 10 ? "0" + day : "" + day;
		String strHour = hour < 10 ? "0" + hour : "" + hour;
		String strMinute = minute < 10 ? "0" + minute : "" + minute;
		String strSecond = second < 10 ? "0" + second : "" + second;
		String strMilliSecond = milliSecond < 10 ? "0" + milliSecond : ""
				+ milliSecond;
		strMilliSecond = milliSecond < 100 ? "0" + strMilliSecond : ""
				+ strMilliSecond;
		return new StringBuffer(strDay).append(" ").append(strHour).append(":").append(
				strMinute).append(":").append(strSecond).append(".").append(
				strMilliSecond).toString();
	}
}
