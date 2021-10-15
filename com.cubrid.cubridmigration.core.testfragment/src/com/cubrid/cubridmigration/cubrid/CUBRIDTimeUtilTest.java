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
import java.util.Date;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;

public class CUBRIDTimeUtilTest {

	/**
	 * testGetAllTimeZones
	 * 
	 * @throws ParseException e
	 */
	@Test
	public final void testGetTime() throws ParseException {
		String timestring = "11:12:13 am";
		long t1 = CUBRIDTimeUtil.parseTime2Long(timestring, null);

		Assert.assertTrue(t1 > 0);

		timestring = "11:12:1311";
		try {
			t1 = CUBRIDTimeUtil.parseTime2Long(timestring, null);
		} catch (Exception e) {
			Assert.assertNotNull(LogUtil.getExceptionString(e));
			Assert.assertTrue(e instanceof ParseException);
		}
	}

	/**
	 * testGetAllTimeZones
	 * 
	 * @throws ParseException e
	 */
	@Test
	public final void testGetDate() throws ParseException {
		String datestring = "2009-11-01";
		//		datestring = "0000-00-00";
		long t1 = CUBRIDTimeUtil.parseDate2Long(datestring, null);
		Assert.assertTrue(t1 > 0);
		Date dt = new Date(t1 * 1000);
		System.err.println(dt);
		datestring = "2009";
		try {
			t1 = CUBRIDTimeUtil.parseDate2Long(datestring, null);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof ParseException);
		}
	}

	/**
	 * testGetAllTimeZones
	 * 
	 * @throws ParseException e
	 */
	@Test
	public final void testGetTimestamp() throws ParseException {
		String datestring = "2009-02-20 16:42:46";
		long res = CUBRIDTimeUtil.parseTimestamp(datestring, null);
		Assert.assertTrue(res > 0);

		datestring = "2009";
		try {
			res = CUBRIDTimeUtil.parseTimestamp(datestring, null);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof ParseException);
		}
	}

	/**
	 * testGetDatetime
	 * 
	 * @throws ParseException e
	 */
	@Test
	public final void testGetDatetime() throws ParseException {
		String datestring = "2009-02-20 16:42:46";
		long res = CUBRIDTimeUtil.parseDatetime2Long(datestring,
				TimeZone.getDefault());
		Assert.assertTrue(res > 0);

		datestring = "2009";
		try {
			res = CUBRIDTimeUtil.parseDatetime2Long(datestring,
					TimeZone.getDefault());
		} catch (Exception e) {
			Assert.assertTrue(e instanceof ParseException);
		}
	}

	/**
	 * testGetAllTimeZones
	 */
	@Test
	public final void testFormatDateTime() {
		String datestring = "06:44:15 AM 11/05/2009";
		String newDatetimePattern = "yyyy-MM-dd HH:mm:ss";
		String res = CUBRIDTimeUtil.formatDateTime(datestring,
				newDatetimePattern, TimeZone.getDefault());
		Assert.assertEquals("2009-11-05 06:44:15", res);

		datestring = "06:44:15.111 AM 11/05/2009";
		newDatetimePattern = "yyyy-MM-dd HH:mm:ss.SSS";
		res = CUBRIDTimeUtil.formatDateTime(datestring, newDatetimePattern,
				TimeZone.getDefault());
		Assert.assertEquals("2009-11-05 06:44:15.111", res);
	}

	/**
	 * testValidateTimestamp
	 */
	@Test
	public final void testValidateTimestamp() {
		String datepattern = "yyyy-MM-dd HH:mm:ss";
		String datestring = "2009-11-05 06:44:15";
		boolean flag = CUBRIDTimeUtil.validateDateString(datestring,
				datepattern);
		Assert.assertTrue(flag);
		datepattern = "yyyy-MM-dd HH:mm:ss";
		datestring = "06:44:15 AM 11/05/2009";
		flag = CUBRIDTimeUtil.validateDateString(datestring, datepattern);
		Assert.assertFalse(flag);
	}

	/**
	 * testGetDateFormat
	 */
	@Test
	public final void testGetDateFormat() {
		String datepattern = "hh:mm:ss.SSS a MM/dd/yyyy";
		Locale locale = Locale.CHINESE;
		TimeZone timeZone = SimpleTimeZone.getDefault();
		DateFormat df = CUBRIDTimeUtil.getDateFormat(datepattern, locale,
				timeZone);
		Assert.assertEquals(timeZone, df.getTimeZone());
	}

	//	/**
	//	 * testGetTimestampStringString
	//	 * 
	//	 * @throws ParseException e
	//	 */
	//	@Test
	//	public final void testGetTimestampStringString() throws ParseException {
	//		String datestring = "2009-11-05 06:44:15";
	//		String datepattern = "yyyy-MM-dd HH:mm:ss";
	//		long res = CUBRIDTimeUtil.parseTimestamp(datestring, datepattern);
	//		Assert.assertTrue(res > 0);
	//	}
	//
	//	/**
	//	 * testGetTimestampStringString
	//	 * 
	//	 * @throws ParseException e
	//	 */
	//	@Test
	//	public final void testGetTimestampString() throws ParseException {
	//		String datestring = "2009-11-05 06:44:15";
	//		String datepattern = "yyyy-MM-dd HH:mm:ss";
	//		long res = CUBRIDTimeUtil.parseTimestamp(datestring, datepattern);
	//
	//		datestring = CUBRIDTimeUtil.formatTimestampLong(res, datepattern);
	//		Assert.assertTrue("2009-11-05 06:44:15".equalsIgnoreCase(datestring));
	//	}

	//	/**
	//	 * testGetDatetimeString
	//	 */
	//	@Test
	//	public final void testGetDatetimeString() {
	//		long timestamp = System.currentTimeMillis();
	//		String datepattern = "yyyy-MM-dd";
	//		String str = CUBRIDTimeUtil.parseTimestamp(timestamp, datepattern);
	//		Assert.assertNotNull(str);
	//	}

	@Test
	public void testDefaultFormater() {
		Date date = new Date();
		Assert.assertEquals(10, CUBRIDTimeUtil.defaultFormatDate(date).length());
		Assert.assertEquals(19,
				CUBRIDTimeUtil.defaultFormatDateTime(date).length());
		Assert.assertEquals(23,
				CUBRIDTimeUtil.defaultFormatMilin(date).length());
		Assert.assertEquals(8, CUBRIDTimeUtil.defaultFormatTime(date).length());
		Assert.assertEquals(12,
				CUBRIDTimeUtil.defaultFormatTimeMilin(date).length());
	}
}
