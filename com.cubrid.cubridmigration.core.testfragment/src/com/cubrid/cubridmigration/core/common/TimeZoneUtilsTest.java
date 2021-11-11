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

import junit.framework.Assert;

import org.junit.Test;

public class TimeZoneUtilsTest {

	@Test
	public void testGetGMTFormat() {
		Assert.assertNotNull(TimeZoneUtils.getGMTFormat("GMT+08"));
		Assert.assertNotNull(TimeZoneUtils.getGMTFormat(null));

		Assert.assertEquals("GMT+01:00", TimeZoneUtils.getTZFromOffset(1));
		Assert.assertEquals("GMT-01:00", TimeZoneUtils.getTZFromOffset(-1));
		Assert.assertEquals("GMT+11:00", TimeZoneUtils.getTZFromOffset(11));
		Assert.assertEquals("GMT-11:00", TimeZoneUtils.getTZFromOffset(-11));
	}

	@Test
	public void testFormat() {
		long sec = 1000;
		long min = sec * 60;
		long hour = min * 60;
		long day = hour * 24;
		TimeZoneUtils.format(0);
		TimeZoneUtils.format(99);
		TimeZoneUtils.format(sec);
		TimeZoneUtils.format(day + 12 * hour + 12 * min + 12 * sec + 888);
		TimeZoneUtils.format(100 * day + 12 * hour + 12 * min + 12 * sec + 888);
	}
}
