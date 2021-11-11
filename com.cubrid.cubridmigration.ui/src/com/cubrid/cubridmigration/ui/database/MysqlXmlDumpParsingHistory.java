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
package com.cubrid.cubridmigration.ui.database;

import com.cubrid.cubridmigration.core.common.PathUtils;

/**
 * XMLCatalogHistory
 * 
 * @author moulinwang
 */
public class MysqlXmlDumpParsingHistory {

	private String xmlFile;
	private long xmlFileLength;
	private String xmlFileCharset;
	private String jsonFileName;

	public MysqlXmlDumpParsingHistory(String xmlFile, String xmlFileCharset, long xmlFileLength,
			String jsonFileName) {
		super();
		this.xmlFile = xmlFile;
		this.xmlFileCharset = xmlFileCharset;
		this.xmlFileLength = xmlFileLength;
		this.jsonFileName = jsonFileName;
	}

	public String getXmlFile() {
		return xmlFile;
	}

	public void setXmlFile(String xmlFile) {
		this.xmlFile = xmlFile;
	}

	public String getXmlFileCharset() {
		return xmlFileCharset;
	}

	public void setXmlFileCharset(String xmlFileCharset) {
		this.xmlFileCharset = xmlFileCharset;
	}

	public long getXmlFileLength() {
		return xmlFileLength;
	}

	public void setXmlFileLength(long xmlFileLength) {
		this.xmlFileLength = xmlFileLength;
	}

	public String getJsonFileName() {
		return jsonFileName;
	}

	public void setJsonFileName(String jsonFileName) {
		this.jsonFileName = jsonFileName;
	}

	/**
	 * Retrieves the full name of json file
	 * 
	 * @return String
	 */
	public String getFullJsonFileName() {
		return PathUtils.getSchemaCacheDir() + jsonFileName;
	}
}
