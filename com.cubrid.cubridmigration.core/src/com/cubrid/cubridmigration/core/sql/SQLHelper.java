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
package com.cubrid.cubridmigration.core.sql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.dbobject.Table;

/**
 * SQLHelper Description
 * 
 * @author Kevin Cao
 */
public abstract class SQLHelper {
	public static final String SQLPARAM_PAGE_SIZE = "#pageSize#";
	public static final String SQLPARAM_PAGE_START = "#pageStartPosition#";
	public static final String SQLPARAM_PAGE_END = "#pageEndPosition#";
	//	public static final String SQLPARAM_TOTAL_EXPORTED = "#totalExported#";

	private static final String VIEW_PATTERN1 = "(?i)CREATE .*? (?i)VIEW (.*?)[\\s|\\t]((?i)AS){1}[\\s|\\t](.*)";
	private static final String VIEW_PATTERN2 = "(?i)CREATE (?i)VIEW (.*?)[\\s|\\t]((?i)AS){1}[\\s|\\t](.*)";

	protected static final String LIMIT_PATTEN_2 = "\\s*(?i)LIMIT\\s*\\d+\\s*(?i)OFFSET\\s*\\d+\\s*(\\D*)";
	protected static final String LIMIT_PATTEN_1 = "\\s*(?i)LIMIT\\s*(\\d+\\s*,)?\\s*\\d+\\s*+(\\D*)";

	/**
	 * return query spec start index
	 * 
	 * @param viewDDL String
	 * @param patternStr String
	 * @return Integer
	 */
	protected Integer getQuerySpecStartIndex(String viewDDL, String patternStr) {
		Pattern pattern = Pattern.compile(patternStr);
		Matcher matcher = pattern.matcher(viewDDL);
		boolean matchFound = matcher.find();

		if (matchFound) {
			return matcher.start(3);
		}
		return null;
	}

	/**
	 * return database object name
	 * 
	 * @param objectName String
	 * @return String
	 */
	public abstract String getQuotedObjName(String objectName);

	/**
	 * append "limit 0" or "rownum = 0" or "top 1" to SELECT statement
	 * 
	 * @param sql SELECT statement
	 * @return String
	 */
	public abstract String getTestSelectSQL(String sql);

	/**
	 * GET Query Specification SQL eg: create view ... as select ... , the
	 * select statement "select ..." will be extracted and returned.
	 * 
	 * @param viewDDL String
	 * @return String
	 */
	public String getViewQuerySpec(String viewDDL) {
		if (StringUtils.isBlank(viewDDL)) {
			return "";
		}
		String viewDDLTemp = viewDDL.replace("\n", " ");
		viewDDLTemp = viewDDLTemp.replace("\r", " ");

		Integer querySpecStartIndex = getQuerySpecStartIndex(viewDDLTemp, VIEW_PATTERN1);
		if (null != querySpecStartIndex) {
			return viewDDL.substring(querySpecStartIndex);
		}

		querySpecStartIndex = getQuerySpecStartIndex(viewDDLTemp, VIEW_PATTERN2);
		if (null != querySpecStartIndex) {
			return viewDDL.substring(querySpecStartIndex);
		}
		return "";
	}

	/**
	 * @param sql to be replaced
	 * @param pageSize page size
	 * @param exportedRecordCount the record count has been exported
	 * @return SQL replaced by page query parameters
	 */
	public String replacePageQueryParameters(String sql, long pageSize, long exportedRecordCount) {
		String result = replacePageQueryParameterToValue(sql, SQLHelper.SQLPARAM_PAGE_SIZE,
				String.valueOf(pageSize));
		long pageStart = Math.max(0, exportedRecordCount);
		result = replacePageQueryParameterToValue(result, SQLHelper.SQLPARAM_PAGE_START,
				String.valueOf(pageStart));
		long pageEnd = Math.max(0, pageStart + pageSize - 1);
		result = replacePageQueryParameterToValue(result, SQLHelper.SQLPARAM_PAGE_END,
				String.valueOf(pageEnd));
		return result;
	}

	/**
	 * Replace the parameter in the SQL by input parameter value
	 * 
	 * @param sql
	 * @param parameterName parameter name, format is #parameterName#
	 * @param parameterValue parameter value
	 * @return replaced SQL
	 */
	protected String replacePageQueryParameterToValue(String sql, String parameterName,
			String parameterValue) {
		boolean isInQuote = false;
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < sql.length(); i++) {
			char ch = sql.charAt(i);
			if (ch == '#') {
				if (isInQuote) {
					sb.append(ch);
					continue;
				}
				String param = sql.substring(i, i + parameterName.length());
				if (param.equals(parameterName)) {
					sb.append(parameterValue);
					i = i + parameterName.length() - 1;
				} else {
					sb.append(ch);
				}
			} else {
				sb.append(ch);
			}
			if (ch == '\'') {
				isInQuote = !isInQuote;
			}
		}
		return sb.toString();
	}
	
	public static SQLHelper getInstance(String ver) {
		return null;
	}

	public String getTableDDL(Table table) {
		return null;
	}
}
