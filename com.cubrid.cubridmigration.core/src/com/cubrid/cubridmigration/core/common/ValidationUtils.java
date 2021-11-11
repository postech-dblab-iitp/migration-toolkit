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

import org.apache.commons.lang.StringUtils;

/**
 * 
 * This class include common validation method and check data validation
 * 
 * @author pangqiren
 */
public final class ValidationUtils {
	public final static int MAX_DB_NAME_LENGTH = 17;

	// USER,DBNAME,FUNCTION,PROCEDURE NAME
	public final static int MAX_NAME_LENGTH = 32;
	public final static int MAX_PASSWORD_LENGTH = 31; //db user PASSWORD max length

	public final static int MIN_PASSWORD_LENGTH = 4; //db user PASSWORD min length

	private ValidationUtils() {
		//empty
	}

	/**
	 * 
	 * validate the db name
	 * 
	 * @param dbName String
	 * @return boolean
	 */
	public static boolean isValidDBName(String dbName) {
		if (StringUtils.isBlank(dbName)) {
			return false;
		}

		/*
		 * it is better that unix file name does not contain space(" ")
		 * character
		 */
		if (dbName.indexOf(" ") >= 0) {
			return false;
		}

		/* Unix file name is not allowed to begin with "#" character */
		if (dbName.charAt(0) == '#') {
			return false;
		}

		/* Unix file name is not allowed to begin with "-" character */
		if (dbName.charAt(0) == '-') {
			return false;
		}

		/*
		 * 9 character(*&%$|^/~\) are not allowed in Unix file name if
		 * (dbName.matches(".*[*&%$\\|^/~\\\\].*")) { return false; } Unix file
		 * name is not allowed to be named as "." or ".."
		 */
		if (".".equals(dbName) || "..".equals(dbName)) {
			return false;
		}

		return dbName.matches("[\\w\\-]*");
	}

	/**
	 * 
	 * validate the path name
	 * 
	 * @param pathName String
	 * @return boolean
	 */
	public static boolean isValidPathName(String pathName) {
		if (StringUtils.isBlank(pathName)) {
			return false;
		}

		/* Unix file name is not allowed to begin with "#" character */
		if (pathName.charAt(0) == '#') {
			return false;
		}

		/* Unix file name is not allowed to begin with "-" character */
		if (pathName.charAt(0) == '-') {
			return false;
		}

		/* 9 character(*&%$|^~) are not allowed in Unix file name */
		if (pathName.matches(".*[*&%$|^].*")) {
			return false;
		}

		/* Unix file name is not allowed to be named as "." or ".." */
		if (".".equals(pathName) || "..".equals(pathName)) {
			return false;
		}

		return true;
	}

	/**
	 * 
	 * validate the database name in the system
	 * 
	 * @param fileName String
	 * @return boolean
	 */
	public static boolean isValidDbNameLength(String fileName) {
		return fileName.length() <= MAX_DB_NAME_LENGTH;
	}

	/**
	 * 
	 * Return whether the string is double type
	 * 
	 * @param str String
	 * @return boolean
	 */
	public static boolean isDouble(String str) {
		if (StringUtils.isBlank(str)) {
			return false;
		}

		String reg = "^[-\\+]?\\d+(\\.\\d+)?$";
		return str.matches(reg);
	}

	/**
	 * 
	 * Return whether the string is positive double type
	 * 
	 * @param str String
	 * @return boolean
	 */
	public static boolean isPositiveDouble(String str) {
		if (StringUtils.isBlank(str)) {
			return false;
		}

		String reg = "^\\d+(\\.\\d+)?$";
		return str.matches(reg);
	}

	/**
	 * 
	 * Return whether the string is integer type
	 * 
	 * @param str String
	 * @return boolean
	 */
	public static boolean isInteger(String str) {
		if (StringUtils.isBlank(str)) {
			return false;
		}

		String reg = "^[-\\+]?\\d+$";
		return str.matches(reg);
	}

	/**
	 * 
	 * Return whether the string is validate ip address
	 * 
	 * @param str String
	 * @return boolean
	 */
	public static boolean isIP(String str) {
		if (StringUtils.isBlank(str)) {
			return false;
		}

		String reg = "^([\\d]{1,3})\\.([\\d]{1,3})\\.([\\d]{1,3})\\.([\\d]{1,3})$";

		if (!str.matches(reg)) {
			return false;
		}

		String[] ipArray = str.split("\\.");

		for (int i = 0; i < ipArray.length; i++) {
			if (Integer.parseInt(ipArray[i]) > 255) {
				return false;
			}
			if (ipArray[i].length() != 1 && ipArray[i].indexOf(0) == 0) {
				return false;
			}
		}
		if (Integer.parseInt(ipArray[0]) > 223) {
			return false;
		}

		return true;
	}

	/**
	 * Return whether the string is validate double or scientific notation
	 * 
	 * @param str String
	 * @return boolean
	 */
	public static boolean isSciDouble(String str) {
		if (StringUtils.isBlank(str)) {
			return false;
		}
		String reg = "^[-||+]?\\d+(\\.\\d+)?([e||E][-||+]?\\d+)?$";
		return str.matches(reg);
	}

	/**
	 * check port
	 * 
	 * @param str port string
	 * @return boolean
	 */
	public static boolean isValidPort(final String str) {
		boolean isValid;
		try {
			final int port = Integer.parseInt(str);
			isValid = port >= 1024 && port <= 65535;
		} catch (NumberFormatException e) {
			isValid = false;
		}
		return isValid;
	}
}
