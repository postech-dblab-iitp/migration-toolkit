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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TimeUtilTest {
	/**
	 * testGetAllTimeZones
	 */
	@Test
	public final void testGetAllTimeZones() {
		List<String> list = TimeZoneUtils.getTimeZonesList();

		for (String s : list) {
			System.out.println(s);
		}

		Assert.assertTrue(list.size() > 0);
	}

	@Test
	public final void testGetDisplayByGMT() {
		String gmtTz = "GMT+08:00";
		String displayTz = TimeZoneUtils.getDisplayByGMT(gmtTz);
		Assert.assertNotNull(displayTz);
	}

	@Test
	public final void testGetTimezoneGMT() {
		String tz = TimeZoneUtils.getGMTFormat("UTC");
		Assert.assertEquals("GMT+00:00", tz);
	}


	/**
	 * testIsValidTimeZone
	 */
	@Test
	public final void testIsValidTimeZone() {
		final String timezone = "GMT+12:00";
		final boolean flag = TimeZoneUtils.isValidTimeZone(timezone);
		Assert.assertTrue(flag);
	}

	@Test
	public final void testGetGMTByDisplay() {
		final String timezone = "GMT-12:00--Etc/GMT+12";
		String gmtFormat = TimeZoneUtils.getGMTByDisplay(timezone);
		String timezone2 = TimeZoneUtils.getDisplayByGMT(gmtFormat);
		Assert.assertEquals(timezone, timezone2);
		System.out.println(TimeZoneUtils.getDefaultID2GMT());
		Assert.assertNull(TimeZoneUtils.getDisplayByGMT(gmtFormat + "1"));
	}

}
