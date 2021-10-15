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

import org.junit.Assert;
import org.junit.Test;

public class ValidationUtilsTest {

	@Test
	public void test_ValidationUtils() {
		Assert.assertTrue(ValidationUtils.isDouble("1.1"));
		Assert.assertTrue(ValidationUtils.isDouble("1.111111111111111"));
		Assert.assertFalse(ValidationUtils.isDouble("1.1111111.11111111"));
		Assert.assertFalse(ValidationUtils.isDouble("1.1111111.111111a11"));
		Assert.assertFalse(ValidationUtils.isDouble("a"));
		Assert.assertFalse(ValidationUtils.isDouble(" "));
		Assert.assertFalse(ValidationUtils.isDouble(null));

		Assert.assertTrue(ValidationUtils.isInteger("1"));
		Assert.assertTrue(ValidationUtils.isInteger("1121"));
		Assert.assertFalse(ValidationUtils.isInteger("1.1111111.11111111"));
		Assert.assertFalse(ValidationUtils.isInteger("1.1111111.111111a11"));
		Assert.assertFalse(ValidationUtils.isInteger("a"));
		Assert.assertFalse(ValidationUtils.isInteger(" "));
		Assert.assertFalse(ValidationUtils.isInteger(null));

		Assert.assertTrue(ValidationUtils.isIP("1.1.1.1"));
		Assert.assertTrue(ValidationUtils.isIP("223.255.1.11"));
		Assert.assertFalse(ValidationUtils.isIP("1.1.a.1"));
		Assert.assertFalse(ValidationUtils.isIP("1.266.1.1"));
		Assert.assertFalse(ValidationUtils.isIP(" "));
		Assert.assertFalse(ValidationUtils.isIP(null));

		Assert.assertTrue(ValidationUtils.isPositiveDouble("1.1"));
		Assert.assertTrue(ValidationUtils.isPositiveDouble("1.111111111111111"));
		Assert.assertFalse(ValidationUtils.isPositiveDouble("-1.111111111111111"));
		Assert.assertFalse(ValidationUtils.isPositiveDouble("1.1111111.11111111"));
		Assert.assertFalse(ValidationUtils.isPositiveDouble("1.1111111.111111a11"));
		Assert.assertFalse(ValidationUtils.isPositiveDouble("a"));
		Assert.assertFalse(ValidationUtils.isPositiveDouble(" "));
		Assert.assertFalse(ValidationUtils.isPositiveDouble(null));

		Assert.assertTrue(ValidationUtils.isSciDouble("1.1"));
		Assert.assertTrue(ValidationUtils.isSciDouble("1.111111111111111"));
		Assert.assertTrue(ValidationUtils.isSciDouble("-1.111111111111111"));
		Assert.assertTrue(ValidationUtils.isSciDouble("1.111111111111111e+10"));
		Assert.assertFalse(ValidationUtils.isSciDouble("1.1111111.111111a11"));
		Assert.assertFalse(ValidationUtils.isSciDouble("a"));
		Assert.assertFalse(ValidationUtils.isSciDouble(" "));
		Assert.assertFalse(ValidationUtils.isSciDouble(null));

		Assert.assertFalse(ValidationUtils.isValidDBName(""));
		Assert.assertFalse(ValidationUtils.isValidDBName("a a"));
		Assert.assertFalse(ValidationUtils.isValidDBName("#"));
		Assert.assertFalse(ValidationUtils.isValidDBName("-"));
		Assert.assertFalse(ValidationUtils.isValidDBName("."));
		Assert.assertFalse(ValidationUtils.isValidDBName(".."));
		Assert.assertTrue(ValidationUtils.isValidDBName("a-a"));
		Assert.assertTrue(ValidationUtils.isValidDBName("A-Z-"));

		Assert.assertTrue(ValidationUtils.isValidDbNameLength("aaaaaaaaaaaaaaaaa"));
		Assert.assertFalse(ValidationUtils.isValidDbNameLength("aaaaaaaaaaaaaaaaaa"));

		Assert.assertFalse(ValidationUtils.isValidPort("1023"));
		Assert.assertTrue(ValidationUtils.isValidPort("1025"));
		Assert.assertFalse(ValidationUtils.isValidPort("65536"));
		Assert.assertTrue(ValidationUtils.isValidPort("65535"));
		Assert.assertFalse(ValidationUtils.isValidPort("aaa"));
	}
}
