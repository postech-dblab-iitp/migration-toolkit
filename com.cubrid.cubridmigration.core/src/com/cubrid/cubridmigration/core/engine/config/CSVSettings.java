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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

/**
 * CSVSettings Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-3-18 created by Kevin Cao
 */
public class CSVSettings implements
		Serializable,
		Cloneable {

	private static final long serialVersionUID = 6398832693801882983L;

	private char separateChar = ',';
	private char quoteChar = '\"';
	private char escapeChar = MigrationConfiguration.CSV_NO_CHAR;

	private final List<String> nullStrings = new ArrayList<String>(4);

	private String charset = "UTF-8";

	public CSVSettings() {
		nullStrings.add("\\N");
		nullStrings.add("NULL");
		nullStrings.add("(NULL)");
	}

	/**
	 * Clone object
	 * 
	 * @return CSVSettings
	 */
	public CSVSettings clone() {
		CSVSettings result = new CSVSettings();
		result.setCharset(getCharset());
		result.setEscapeChar(getEscapeChar());
		result.setNullStrings(getNullStrings());
		result.setQuoteChar(getQuoteChar());
		result.setSeparateChar(getSeparateChar());
		return result;
	}

	/**
	 * Copy object
	 * 
	 * @param result CSVSettings
	 */
	public void copyFrom(CSVSettings result) {
		setCharset(result.getCharset());
		setEscapeChar(result.getEscapeChar());
		setNullStrings(result.getNullStrings());
		setQuoteChar(result.getQuoteChar());
		setSeparateChar(result.getSeparateChar());
	}

	/**
	 * equals
	 * 
	 * @param obj Object
	 * @return true if equals
	 */
	public boolean equals(Object obj) {
		if (super.equals(obj)) {
			return true;
		}
		if (obj == null || !(obj instanceof CSVSettings)) {
			return false;
		}
		CSVSettings source = (CSVSettings) obj;
		if (this.separateChar != source.separateChar) {
			return false;
		}
		if (this.quoteChar != source.quoteChar) {
			return false;
		}
		if (this.escapeChar != source.escapeChar) {
			return false;
		}
		if (!StringUtils.equalsIgnoreCase(this.charset, source.charset)) {
			return false;
		}
		if (this.nullStrings.size() != source.nullStrings.size()) {
			return false;
		}
		for (String ss : nullStrings) {
			if (source.nullStrings.indexOf(ss) < 0) {
				return false;
			}
		}
		return true;
	}

	public String getCharset() {
		return charset;
	}

	public char getEscapeChar() {
		return escapeChar;
	}

	/**
	 * Return a copy of NULL strings
	 * 
	 * @return List<String>
	 */
	public List<String> getNullStrings() {
		return new ArrayList<String>(nullStrings);
	}

	public char getQuoteChar() {
		return quoteChar;
	}

	public char getSeparateChar() {
		return separateChar;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setEscapeChar(char escapeChar) {
		this.escapeChar = escapeChar;
	}

	/**
	 * Set NULL strings
	 * 
	 * @param ns List<String>
	 */
	public void setNullStrings(List<String> ns) {
		nullStrings.clear();
		if (CollectionUtils.isEmpty(ns)) {
			return;
		}
		for (String ss : ns) {
			if (nullStrings.indexOf(ss) < 0) {
				nullStrings.add(ss);
			}
		}
	}

	/**
	 * Set NULL strings
	 * 
	 * @param ns List<String>
	 */
	public void setNullStrings(String ns) {
		nullStrings.clear();
		if (StringUtils.isEmpty(ns)) {
			return;
		}
		String[] nss = ns.split(";");
		for (String ss : nss) {
			if (nullStrings.indexOf(ss) < 0) {
				nullStrings.add(ss);
			}
		}
	}

	public void setQuoteChar(char quoteChar) {
		this.quoteChar = quoteChar;
	}

	public void setSeparateChar(char separateChar) {
		this.separateChar = separateChar;
	}
}
