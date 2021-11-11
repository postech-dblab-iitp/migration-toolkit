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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;

/**
 * CommonTool
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-18
 */
public final class CommonUtils {
	private final static Logger LOG = LogUtil.getLogger(CommonUtils.class);
	private static final String LOGIN_CFG = "com.sun.security.jgss.krb5.initiate'"
			+ "{'com.sun.security.auth.module.Krb5LoginModule "
			+ " required debug=\"false\" doNotPrompt=\"true\" useTicketCache=\"true\" ticketCache=\"{0}\";'}';\n"
			+ "com.sun.security.jgss.initiate'{'com.sun.security.auth.module.Krb5LoginModule"
			+ " required debug=\"false\" doNotPrompt=\"true\" useTicketCache=\"true\" ticketCache=\"{0}\";'}';";
	private final static String DEFAULT_NUMERIC_FORMAT = "#0.######################################";
	public static String newLine = System.getProperty("line.separator");
	public final static String AD_LOCALHOST = "localhost"; //NOPMD
	public final static String AD_LOCALADDRESS = "127.0.0.1"; //NOPMD

	private CommonUtils() {
	}

	/**
	 * translateIP
	 * 
	 * @param hostIP String
	 * @return real IP
	 */
	public static String translateIP(String hostIP) {
		if (AD_LOCALHOST.equalsIgnoreCase(hostIP) || AD_LOCALADDRESS.equalsIgnoreCase(hostIP)) {

			try {
				InetAddress localHost = InetAddress.getLocalHost();
				return localHost.getHostAddress();
			} catch (UnknownHostException e) {
				LOG.error(e);
			}
		}

		return hostIP;
	}

	/**
	 * string Encoding
	 * 
	 * @param str source String
	 * @param charset target charset
	 * @return string
	 */
	public static String toEncoding(final String str, final String charset) {
		try {

			if (StringUtils.isEmpty(str) || StringUtils.isEmpty(charset)) {
				return str;
			} else {
				return new String(str.getBytes(), charset);
			}

		} catch (UnsupportedEncodingException ex) {
			LOG.error(LogUtil.getExceptionString(ex));
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Compares the two lists ignoring the order of elements. No nulls may be
	 * contained
	 * 
	 * @param list1 List
	 * @param list2 List
	 * @return boolean
	 */
	public static boolean equalsListsIgnoreOrder(final List<?> list1, final List<?> list2) {

		// length differs
		if (list1 == null || list2 == null || list1.size() != list2.size()) {
			return false;
		}

		// both null or identical
		if (list1.equals(list2)) {
			return true;
		}

		for (int i = 0; i < list1.size(); i++) {
			final Object obj1 = list1.get(i);
			boolean found = false;

			for (int j = i; j < list2.size(); j++) {
				// try same order for performance
				final Object obj2 = list2.get(j);

				if (obj1.equals(obj2)) {
					found = true;
					break;
				}
			}

			for (int j = 0; j < i; j++) {
				// try others, seems to be in other order
				final Object obj2 = list2.get(j);

				if (obj1.equals(obj2)) {
					found = true;
					break;
				}
			}

			if (!found) {
				return false;
			}
		}

		return true;
	}

	/**
	 * @param obj1 Object
	 * @param obj2 Object
	 * @return boolean Returns false if objects are not equal or one of them is
	 *         null and the other not.
	 */
	public static boolean equals(final Object obj1, final Object obj2) {
		return obj1 == null ? obj2 == null : obj1.equals(obj2);
	} // null == null is true in Java

	//	/**
	//	 * @param obj Object
	//	 * @return int Returns zero for a null object, else the objects hash code.
	//	 */
	//	public static int hashCode(final Object obj) {
	//		return obj == null ? 0 : obj.hashCode();
	//	}

	/**
	 * 
	 * Convert string to int
	 * 
	 * @param str String
	 * @return int
	 */
	public static int str2Int(String str) {
		String reg = "^[-\\+]?\\d+$";

		if (str.matches(reg)) {
			return Integer.parseInt(str);
		}

		return 0;
	}

	/**
	 * 
	 * Convert string to Long
	 * 
	 * @param str String
	 * @return long
	 */
	public static long str2Long(final String str) {
		try {
			return Long.parseLong(str);
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	/**
	 * 
	 * Convert string to double
	 * 
	 * @param inval String
	 * @return double
	 */
	public static double str2Double(final String inval) {
		String sciReg = "^[-||+]?\\d+(\\.\\d+)?([e||E][-||\\+]?\\d+)?$";
		String plainReg = "^[-\\+]?\\d+(\\.\\d+)?$";

		if (inval.matches(sciReg) || inval.matches(plainReg)) {
			return Double.parseDouble(inval);
		}

		return 0.0;
	}

	/**
	 * 
	 * Convert string Y or N value to boolean
	 * 
	 * @param inval String
	 * @return boolean
	 */
	public static boolean strYN2Boolean(final String inval) {
		if (inval == null) {
			return false;
		}

		return inval.equalsIgnoreCase("y") ? true : false;
	}

	/**
	 * return true if a string s is a ascii string.
	 * 
	 * @param str String
	 * @return boolean
	 */
	public static boolean isASCII(String str) {
		for (int i = 0, len = str.length(); i < len; i++) {
			if (!Character.UnicodeBlock.of(str.charAt(i)).equals(Character.UnicodeBlock.BASIC_LATIN)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * 
	 * Validate and check identifier
	 * 
	 * @param identifier String
	 * @return String
	 */
	public static String validateCheckInIdentifier(String identifier) {
		StringBuffer retStr = new StringBuffer(); // add string if Identifier has invalid string. 

		//retStr = ""; // Last status is "", it is valid identifier.

		if (StringUtils.isEmpty(identifier)) {
			return "empty";
		}

		char[] invalidChars = new char[] {' ', '\t', '/', '.', '~', ',', '\\', '\"', '|', '[', ']',
				'{', '}', '(', ')', '=', '-', '+', '?', '<', '>', ':', ';', '!', '\'', '@', '$',
				'^', '&', '*', '`'};

		for (char c : invalidChars) {
			if (identifier.indexOf(c) >= 0) {
				retStr.append(c);
				break;
			}
		}

		return retStr.toString();
	}

	/**
	 * 
	 * This method encodes the url, removes the spaces from the url and replaces
	 * the same with <code>"%20"</code>.
	 * 
	 * @param input the input char array
	 * @return the string
	 */
	public static String urlEncodeForSpaces(char[] input) {
		StringBuffer retu = new StringBuffer(input.length);
		for (int i = 0; i < input.length; i++) {
			if (input[i] == ' ') {
				retu.append("%20");
			} else {
				retu.append(input[i]);
			}
		}
		return retu.toString();
	}

	//private static final String NUM_FORMAT = "0000000000000000000000000000000000000.00000000000000000000000000000000000000";
	/**
	 * Format number to fit for CUBRID numeric type.
	 * 
	 * @param value Decimal number
	 * @return string
	 */
	public static String formatCUBRIDNumber(BigDecimal value) {
		return new DecimalFormat(DEFAULT_NUMERIC_FORMAT).format(value);
	}

	/**
	 * Create a new array list with input array.
	 * 
	 * @param array Object[]
	 * @return List
	 */
	public static List<Object> createListWithArray(Object[] array) {
		final ArrayList<Object> result = new ArrayList<Object>();
		if (array == null) {
			return result;
		}
		for (Object obj : array) {
			result.add(obj);
		}
		return result;
	}

	/**
	 * Convert Byte[] to byte[]
	 * 
	 * @param byteObjects Byte[]
	 * @return byte[]
	 */
	public static byte[] getBytesFromByteArray(Byte[] byteObjects) {
		byte[] data = null;
		if (byteObjects != null) {
			data = new byte[byteObjects.length];
			for (int i = 0; i < byteObjects.length; i++) {
				data[i] = byteObjects[i].byteValue();
			}
		}
		return data;
	}

	/**
	 * 
	 * If there is an out of memory error risk, this will return true
	 * 
	 * @return true if there is a risk of OOM.
	 */
	public static boolean oomWarning() {
		Runtime rt = Runtime.getRuntime();
		final long totalMemory = rt.totalMemory();
		final long maxMemory = rt.maxMemory();
		final long freeMemory = rt.freeMemory();
		long percent = (totalMemory * 100 / maxMemory);
		long percent2 = (freeMemory * 100 / totalMemory);
		return percent > 90 && percent2 < 10;
	}

	/**
	 * 
	 * @param tkFile String
	 * @return content
	 */
	public static String getGSSLoginConfigContent(String tkFile) {
		return MessageFormat.format(LOGIN_CFG, tkFile, tkFile);
	}

	/**
	 * Retrieves if the input value is like the format (XXXXX)
	 * 
	 * @param value input string
	 * @return true if in parentheses.
	 */
	public boolean isInParentheses(String value) {
		if (StringUtils.isBlank(value)) {
			return false;
		}
		return value.startsWith("(") && value.endsWith(")");
	}

	/**
	 * Retrieves if the input value is like the format "XXXXX"
	 * 
	 * @param value input string
	 * @return true if in Double Quotation.
	 */
	public boolean isInDoubleQuotation(String value) {
		if (StringUtils.isBlank(value)) {
			return false;
		}
		return value.startsWith("\"") && value.endsWith("\"");
	}

	/**
	 * Retrieves if the input value is like the format 'XXXXX'
	 * 
	 * @param value input string
	 * @return true if in Single Quotation.
	 */
	public boolean isInSingleQuotation(String value) {
		if (StringUtils.isBlank(value)) {
			return false;
		}
		return value.startsWith("'") && value.endsWith("'");
	}

}