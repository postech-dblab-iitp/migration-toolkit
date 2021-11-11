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
package com.cubrid.cubridmigration.core.engine.config;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * SourceColumnConfig Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-9-8 created by Kevin Cao
 */
public class SourceColumnConfig extends
		SourceConfig {
	private SourceTableConfig parent;

	private boolean needTrim = false;

	private final Map<String, String> valueReplace = new TreeMap<String, String>();

	//A class file's full name with method "Object convert(String tablename,String columnName,Object value)" 
	private String userDataHandler;

	/**
	 * Parent table configuration.
	 * 
	 * @return the parent
	 */
	public SourceTableConfig getParent() {
		return parent;
	}

	/**
	 * @param parent the parent to set
	 */
	public void setParent(SourceTableConfig parent) {
		this.parent = parent;
	}

	public boolean isNeedTrim() {
		return needTrim;
	}

	public void setNeedTrim(boolean needTrim) {
		this.needTrim = needTrim;
	}

	/**
	 * Get replace value
	 * 
	 * @param oldValue old value to be replaced
	 * @return new value value
	 */
	public String getReplaceValue(String oldValue) {
		final String result = valueReplace.get(oldValue);
		return result == null ? oldValue : result;
	}

	/**
	 * Set the column configuration's value replacement expression. The
	 * expression should be a special format string as follows:
	 * oldvalue1:newvalue;oldvalue2:newvalue2 ...
	 * 
	 * @param exp the replacement expression
	 */
	public void setReplaceExpression(String exp) {
		valueReplace.clear();
		if (StringUtils.isEmpty(exp)) {
			return;
		}
		String[] entrys = exp.split(";");
		for (String entry : entrys) {
			if (entry.indexOf(":") < 0) {
				continue;
			}
			String old = entry.substring(0, entry.indexOf(":"));
			String nv = entry.substring(entry.indexOf(":") + 1);
			valueReplace.put(old, nv);
		}
	}

	/**
	 * Retrieves the replacement expression
	 * 
	 * @return the replacement expression
	 */
	public String getReplaceExp() {
		StringBuffer sb = new StringBuffer();
		for (Entry<String, String> entry : valueReplace.entrySet()) {
			if (sb.length() > 0) {
				sb.append(";");
			}
			sb.append(entry.getKey()).append(":").append(entry.getValue());
		}
		return sb.toString();
	}

	public String getUserDataHandler() {
		return userDataHandler;
	}

	public void setUserDataHandler(String handlerPath) {
		this.userDataHandler = handlerPath;
	}

}
