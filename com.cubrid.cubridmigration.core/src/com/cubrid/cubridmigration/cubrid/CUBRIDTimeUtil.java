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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;

/**
 * CUBRID time util
 * 
 * @author moulinwang
 * 
 */
public class CUBRIDTimeUtil { //NOPMD
	private final static Logger LOG = LogUtil.getLogger(CUBRIDTimeUtil.class);
	private static String[] supportedDatePattern = {"MM/dd/yyyy", "yyyy/MM/dd",
			"yyyy-MM-dd", "''MM/dd/yyyy''", "''yyyy/MM/dd''", "''yyyy-MM-dd''" };

	private static String[] supportedDateTimePattern = {"MMM dd yyyy hh:mma",
			"yyyy/MM/dd a hh:mm:ss.SSS", "yyyy/MM/dd hh:mm:ss.SSS a",
			"yyyy-MM-dd a hh:mm:ss.SSS", "yyyy-MM-dd hh:mm:ss.SSS a",
			"yyyy/MM/dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SSS",
			"hh:mm:ss.SSS a MM/dd/yyyy", "a hh:mm:ss.SSS MM/dd/yyyy",
			"HH:mm:ss.SSS MM/dd/yyyy", "yyyy/MM/dd a hh:mm",
			"''yyyy/MM/dd a hh:mm:ss.SSS''", "''yyyy/MM/dd hh:mm:ss.SSS a''",
			"''yyyy-MM-dd a hh:mm:ss.SSS''", "''yyyy-MM-dd hh:mm:ss.SSS a''",
			"''yyyy/MM/dd HH:mm:ss.SSS''", "''yyyy-MM-dd HH:mm:ss.SSS''",
			"''hh:mm:ss.SSS a MM/dd/yyyy''", "''HH:mm:ss.SSS MM/dd/yyyy''",
			"yyyy-MM-dd a hh:mm:ss", "yyyy-MM-dd hh:mm:ss a",
			"yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
			"hh:mm:ss a MM/dd/yyyy", "HH:mm:ss MM/dd/yyyy",
			"yyyy/MM/dd a hh:mm", "yyyy-MM-dd a hh:mm", "yyyy/MM/dd HH:mm",
			"yyyy-MM-dd HH:mm", "hh:mm a MM/dd/yyyy", "HH:mm MM/dd/yyyy",
			"''yyyy/MM/dd a hh:mm:ss''", "''yyyy/MM/dd hh:mm:ss a''",
			"''yyyy-MM-dd a hh:mm:ss''", "''yyyy-MM-dd hh:mm:ss a''",
			"''yyyy/MM/dd HH:mm:ss''", "''yyyy-MM-dd HH:mm:ss''",
			"''hh:mm:ss a MM/dd/yyyy''", "''HH:mm:ss MM/dd/yyyy''",
			"''yyyy/MM/dd a hh:mm''", "''yyyy-MM-dd a hh:mm''",
			"''yyyy/MM/dd HH:mm''", "''yyyy-MM-dd HH:mm''",
			"''hh:mm a MM/dd/yyyy''", "''HH:mm MM/dd/yyyy''" };

	private static String[] supportedTimePattern = {"hh:mm:ss a", "a hh:mm:ss",
			"HH:mm:ss", "hh:mm a", "a hh:mm", "HH:mm", "''hh:mm:ss a''",
			"''a hh:mm:ss''", "''HH:mm:ss''", "''hh:mm a''", "''a hh:mm''",
			"''HH:mm''" };

	private static String[] supportedTimeStampPattern = {"MMM dd yyyy hh:mma",
			"yyyy/MM/dd a hh:mm:ss", "yyyy/MM/dd hh:mm:ss a",
			"yyyy-MM-dd a hh:mm:ss", "yyyy-MM-dd hh:mm:ss a",
			"yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
			"hh:mm:ss a MM/dd/yyyy", "a hh:mm:ss MM/dd/yyyy",
			"HH:mm:ss MM/dd/yyyy", "yyyy/MM/dd a hh:mm", "yyyy-MM-dd a hh:mm",
			"yyyy/MM/dd HH:mm", "yyyy-MM-dd HH:mm", "hh:mm a MM/dd/yyyy",
			"HH:mm MM/dd/yyyy", "MM/dd/yyyy hh:mm:ss a",
			"MM/dd/yyyy a hh:mm:ss", "MM/dd/yyyy HH:mm:ss",
			"MM-dd-yyyy hh:mm:ss a", "MM-dd-yyyy a hh:mm:ss",
			"MM-dd-yyyy HH:mm:ss", "''yyyy/MM/dd a hh:mm:ss''",
			"''yyyy-MM-dd a hh:mm:ss''", "''yyyy/MM/dd HH:mm:ss''",
			"''yyyy-MM-dd HH:mm:ss''", "''hh:mm:ss a MM/dd/yyyy''",
			"''HH:mm:ss MM/dd/yyyy''", "''yyyy/MM/dd a hh:mm''",
			"''yyyy-MM-dd a hh:mm''", "''yyyy/MM/dd HH:mm''",
			"''yyyy-MM-dd HH:mm''", "''hh:mm a MM/dd/yyyy''",
			"''HH:mm MM/dd/yyyy''", "''MM/dd/yyyy hh:mm:ss a''",
			"''MM/dd/yyyy a hh:mm:ss''", "''MM/dd/yyyy HH:mm:ss''",
			"''MM-dd-yyyy hh:mm:ss a''", "''MM-dd-yyyy a hh:mm:ss''",
			"''MM-dd-yyyy HH:mm:ss''" };

	/**
	 * Format date into yyyy-MM-dd with default time zone and default format
	 * 
	 * @param date Date
	 * @return yyyy-MM-dd
	 */
	public static String defaultFormatDate(Date date) {
		return CUBRIDTimeUtil.getDateFormat("yyyy-MM-dd", Locale.US,
				TimeZone.getDefault()).format(date);
	}

	/**
	 * Format date into yyyy-MM-dd HH:mm:ss with default time zone and default
	 * format
	 * 
	 * @param date Date
	 * @return yyyy-MM-dd HH:mm:ss
	 */
	public static String defaultFormatDateTime(Date date) {
		return CUBRIDTimeUtil.getDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US,
				TimeZone.getDefault()).format(date);
	}

	/**
	 * Format date into yyyy-MM-dd HH:mm:ss.SSS with default time zone and
	 * default format
	 * 
	 * @param date Date
	 * @return yyyy-MM-dd HH:mm:ss.SSS
	 */
	public static String defaultFormatMilin(Date date) {
		return CUBRIDTimeUtil.getDateFormat("yyyy-MM-dd HH:mm:ss.SSS",
				Locale.US, TimeZone.getDefault()).format(date);
	}

	/**
	 * Format date into HH:mm:ss with default time zone and default format
	 * 
	 * @param date Date
	 * @return HH:mm:ss
	 */
	public static String defaultFormatTime(Date date) {
		return CUBRIDTimeUtil.getDateFormat("HH:mm:ss", Locale.US,
				TimeZone.getDefault()).format(date);
	}

	/**
	 * Format date into HH:mm:ss.SSS with default time zone and default format
	 * 
	 * @param date Date
	 * @return HH:mm:ss.SSS
	 */
	public static String defaultFormatTimeMilin(Date date) {
		return CUBRIDTimeUtil.getDateFormat("HH:mm:ss.SSS", Locale.US,
				TimeZone.getDefault()).format(date);
	}

	/**
	 * getDateCUBRIDString
	 * 
	 * @param date Date
	 * @param datepattern String
	 * @param tz TimeZone
	 * @return String
	 */
	public static String formatDate(final Date date, final String datepattern,
			TimeZone tz) {
		final DateFormat formatter = new SimpleDateFormat(datepattern,
				Locale.US);
		formatter.setTimeZone(tz);
		formatter.setLenient(false);
		return formatter.format(date);
	}

	/**
	 * format a dateTime into a given date pattern string
	 * 
	 * @param dateTime long type dateTime, unit:second
	 * @param datepattern a given date pattern
	 * @param tz TimeZone
	 * @return String
	 */
	public static String formatDateTime(long dateTime, String datepattern,
			TimeZone tz) {
		DateFormat formatter = getDateFormat(datepattern, Locale.US, tz);
		Date date = new Date(dateTime);
		return formatter.format(date);
	}

	/**
	 * format a datetime string to another datetime string <br>
	 * Note: use SimpleDateFormat to parse a "2009/12/12 12:33:00.4", the result
	 * is "2009/12/12 12:33:00.004", but expected "2009/12/12 12:33:00.400", <br>
	 * so this function first to check whether the millisecond part is 3
	 * digital, if not, padding with 0
	 * 
	 * @param datestring String
	 * @param newDatetimePattern String
	 * @param tz TimeZone
	 * @return String
	 */
	public static String formatDateTime(final String datestring,
			String newDatetimePattern, TimeZone tz) {
		String srcDatetimePattern = getDatetimeFormatPattern(datestring);

		if (srcDatetimePattern == null) {
			return null;
		}

		long timestamp = 0;
		int start = srcDatetimePattern.indexOf("SSS");
		String paddingDateString = datestring;

		if (-1 != start) {
			String firstPartPattern = srcDatetimePattern.substring(0, start);
			ParsePosition position = new ParsePosition(0);
			DateFormat formatter = getDateFormat(firstPartPattern, Locale.US,
					null);
			formatter.parse(datestring, position);
			int firstIndex = position.getIndex();
			StringBuffer buf = new StringBuffer();

			buf.append(datestring.substring(0, firstIndex));
			int count = 0;
			int i;
			for (i = firstIndex; i < datestring.length(); i++) {
				char c = datestring.charAt(i);

				if (c >= '0' && c <= '9') {
					buf.append(c);
					count++;
				} else {
					break;
				}
			}
			if (count < 3) {
				for (int j = 0; j < 3 - count; j++) {
					buf.append("0");
				}
			}
			for (; i < datestring.length(); i++) {
				char c = datestring.charAt(i);
				buf.append(c);
			}

			paddingDateString = buf.toString();
		}
		try {
			DateFormat formatter = getDateFormat(srcDatetimePattern, Locale.US,
					tz);
			Date date = formatter.parse(paddingDateString);
			timestamp = date.getTime();
		} catch (ParseException ex) {
			//ignored, for datestring has been checked ahead
			LOG.error(LogUtil.getExceptionString(ex));
		}
		return formatDateTime(timestamp, newDatetimePattern, tz);

	}

	/**
	 * format a timestamp into a given date pattern string
	 * 
	 * @param timestamp long type timestamp, unit:second
	 * @param datepattern a given date pattern
	 * @param tz TimeZone
	 * @return String
	 */
	public static String formatTimestampLong(long timestamp,
			String datepattern, TimeZone tz) {
		long newTimestamp = timestamp;
		DateFormat formatter = getDateFormat(datepattern, Locale.US, tz);
		newTimestamp = newTimestamp / 1000;
		newTimestamp = newTimestamp * 1000;
		Date date = new Date(newTimestamp);
		return formatter.format(date);
	}

	/**
	 * return a standard DateFormat instance, which has a given Local, TimeZone,
	 * and with a strict check
	 * 
	 * @param datepattern String
	 * @param locale Locale
	 * @param timeZone TimeZone
	 * @return DateFormat
	 */
	public static DateFormat getDateFormat(String datepattern, Locale locale,
			TimeZone timeZone) {
		DateFormat formatter = new SimpleDateFormat(datepattern, locale);
		formatter.setLenient(false);
		if (timeZone == null) {
			formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		} else {
			formatter.setTimeZone(timeZone);
		}
		return formatter;
	}

	/**
	 * return the datetime pattern for a given datetime string
	 * 
	 * @param datetimeString String
	 * @return String
	 */
	private static String getDatetimeFormatPattern(final String datetimeString) {
		for (String datepattern : supportedDateTimePattern) {
			if (validateDateString(datetimeString, datepattern)) {
				return datepattern;
			}
		}
		return null;
	}

	/**
	 * support multi input date string, return the timestamp, a long type with
	 * unit second <li>"MM/dd/yyyy", <li>"yyyy/MM/dd", <li>"yyyy-MM-dd"
	 * 
	 * @param datestring String date string eg: 2009-02-20
	 * @param tz TimeZone
	 * @return long timestamp
	 * @throws ParseException e
	 */
	public static long parseDate2Long(final String datestring, TimeZone tz) throws ParseException {

		for (String datepattern : supportedDatePattern) {
			if (validateDateString(datestring, datepattern)) {
				try {
					return parseTimestamp2Long(datestring, datepattern, tz);
				} catch (Exception e) {
					//it is designed not to run at here,so throws nothing
					LOG.error("an unexpected exception is throwed.\n"
							+ e.getMessage());
				}
			}
		}

		throw new ParseException("Unparseable date: \"" + datestring + "\"", 0);
	}

	/**
	 * support multi input data string, return the timestamp, a long type with
	 * unit second <li>"hh:mm[:ss].[SSS] a MM/dd/yyyy", <li>
	 * "HH:mm[:ss].[SSS] MM/dd/yyyy", <li>"yyyy/MM/dd a hh:mm[:ss].[SSS]", <li>
	 * "yyyy-MM-dd a hh:mm[:ss].[SSS]", <li>"yyyy/MM/dd HH:mm[:ss].[SSS]", <li>
	 * "yyyy-MM-dd HH:mm[:ss].[SSS]"
	 * 
	 * @param datestring String date string eg: 2009-02-20 16:42:46
	 * @param tz TimeZone
	 * @return long timestamp
	 * @throws ParseException e
	 */
	public static long parseDatetime2Long(final String datestring, TimeZone tz) throws ParseException {
		for (String datepattern : supportedDateTimePattern) {
			if (validateDateString(datestring, datepattern)) {
				try {
					final DateFormat formatter = getDateFormat(datepattern,
							Locale.US, null);
					if (tz != null) {
						formatter.setTimeZone(tz);
					}
					Date date = formatter.parse(datestring);
					return date.getTime();
				} catch (Exception e) {
					//it is designed not to run at here,so throws nothing
					LOG.error("an unexpected exception is throwed.\n"
							+ e.getMessage());
				}
			}
		}

		throw new ParseException(
				"Unparseable datetime: \"" + datestring + "\"", 0);
	}

	/**
	 * support multi input time string, return the timestamp, a long type with
	 * unit second <li>"hh:mm[:ss] a" <li>"a hh:mm[:ss]" <li>"HH:mm[:ss]"
	 * 
	 * @param timestring String time string eg: 11:12:13 am
	 * @param tz TimeZone
	 * @return long timestamp
	 * @throws ParseException e
	 */
	public static long parseTime2Long(final String timestring, TimeZone tz) throws ParseException {

		for (String datepattern : supportedTimePattern) {
			if (validateDateString(timestring, datepattern)) {
				try {
					return parseTimestamp2Long(timestring, datepattern, tz);
				} catch (Exception e) {
					//it is designed not to run at here,so throws nothing
					LOG.error("an unexpected exception is throwed.\n"
							+ e.getMessage());
				}
			}
		}

		throw new ParseException("Unparseable time: \"" + timestring + "\"", 0);
	}

	/**
	 * support multi input data string, return the timestamp, a long type with
	 * unit second <li>"hh:mm[:ss] a MM/dd/yyyy", <li>"HH:mm[:ss] MM/dd/yyyy",
	 * <li>"yyyy/MM/dd a hh:mm[:ss]", <li>"yyyy-MM-dd a hh:mm[:ss]", <li>
	 * "yyyy/MM/dd HH:mm[:ss]", <li>"yyyy-MM-dd HH:mm[:ss]"
	 * 
	 * @param datestring String date string eg: 2009-02-20 16:42:46
	 * @param tz TimeZone
	 * @return long timestamp
	 * @throws ParseException e
	 */
	public static long parseTimestamp(final String datestring, TimeZone tz) throws ParseException {
		for (String datepattern : supportedTimeStampPattern) {
			if (validateDateString(datestring, datepattern)) {
				try {
					return parseTimestamp2Long(datestring, datepattern, tz);
				} catch (Exception e) {
					//it is designed not to run at here,so throws nothing
					LOG.error("an unexpected exception is throwed.\n"
							+ e.getMessage());
				}
			}
		}

		throw new ParseException("Unparseable date: \"" + datestring + "\"", 0);
	}

	/**
	 * parse date string with a given date pattern, return long type timestamp,
	 * unit:second
	 * 
	 * precondition: it is better to call
	 * cubridmanager.CommonTool.validateTimestamp(String, String) first to void
	 * throwing an ParseException
	 * 
	 * @param datestring String date string eg: 2009-02-20 16:42:46
	 * @param datepattern String date pattern eg: yyyy-MM-dd HH:mm:ss
	 * @param timeZone TimeZone if null ,the default will be GMT
	 * @return long timestamp
	 * @throws ParseException e
	 */
	private static long parseTimestamp2Long(String datestring,
			String datepattern, TimeZone timeZone) throws ParseException {
		DateFormat formatter = getDateFormat(datepattern, Locale.US, timeZone);
		Date date = formatter.parse(datestring);
		return date.getTime();
	}

	/**
	 * validate whether a date string can be parsed by a given date pattern
	 * 
	 * @param datestring String a date string
	 * @param datepattern String a given date pattern
	 * @return boolean true: can be parsed; false: can not
	 */
	public static boolean validateDateString(String datestring,
			String datepattern) {
		ParsePosition pp = new ParsePosition(0);

		DateFormat formatter = getDateFormat(datepattern, Locale.US, null);
		formatter.parse(datestring, pp);

		return pp.getIndex() == datestring.length();
	}
}
