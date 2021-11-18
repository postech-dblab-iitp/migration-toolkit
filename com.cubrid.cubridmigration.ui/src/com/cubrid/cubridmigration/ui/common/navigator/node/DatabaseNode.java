/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
 * Solution.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 */
package com.cubrid.cubridmigration.ui.common.navigator.node;

import com.cubrid.common.ui.navigator.DefaultCUBRIDNode;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Catalog;

/**
 * 
 * DatabaseNode
 * 
 * @author moulinwang
 * @version 1.0 - 2009-10-13
 */
public class DatabaseNode extends
		DefaultCUBRIDNode {
	Catalog catalog;
	ConnParameters connParameters;
	boolean isCatalogLoaded = false;
	boolean isXMLDatabase = false;

	/**
	 * The constructor
	 * 
	 * @param id
	 * @param label
	 */
	public DatabaseNode(String id, String label) {
		super(id, label, "icon/db/DB.png");
		setType(CubridNodeType.DATABASE);
		setContainer(true);
	}

	public Catalog getCatalog() {
		return catalog;
	}

	public void setCatalog(Catalog catalog) {
		this.catalog = catalog;
	}

	public ConnParameters getConnParameters() {
		return connParameters;
	}

	public void setConnParameters(ConnParameters connParameters) {
		this.connParameters = connParameters;
	}

	public boolean isCatalogLoaded() {
		return isCatalogLoaded;
	}

	public void setCatalogLoaded(boolean isCatalogLoaded) {
		this.isCatalogLoaded = isCatalogLoaded;
	}

	String xmlFile;

	String xmlFileCharset;

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

	public boolean isXMLDatabase() {
		return isXMLDatabase;
	}

	public void setXMLDatabase(boolean isXMLDatabase) {
		this.isXMLDatabase = isXMLDatabase;
	}
}
