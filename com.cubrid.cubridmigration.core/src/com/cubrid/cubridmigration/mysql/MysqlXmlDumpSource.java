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
package com.cubrid.cubridmigration.mysql;

import java.io.FileInputStream;
import java.io.Reader;
import java.io.Serializable;

import com.cubrid.cubridmigration.core.dbmetadata.IDBSource;
import com.cubrid.cubridmigration.core.io.IReaderEvent;
import com.cubrid.cubridmigration.core.io.RmInvalidXMLCharReader;

/**
 * MYSQLXMLDumpSource Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-2-20 created by Kevin Cao
 */
public class MysqlXmlDumpSource implements
		IDBSource,
		Serializable,
		Cloneable {

	private static final long serialVersionUID = 2459494843213118124L;

	private final String fileName;
	private final String charset;
	private IReaderEvent event;

	public MysqlXmlDumpSource(String file, String charset) {
		this(file, charset, null);
	}

	public MysqlXmlDumpSource(String file, String charset, IReaderEvent event) {
		this.fileName = file;
		this.charset = charset;
		this.event = event;
	}

	public String getCharset() {
		return charset;
	}

	public IReaderEvent getEvent() {
		return event;
	}

	public String getFileName() {
		return fileName;
	}

	/**
	 * Create XML reader
	 * 
	 * @return XML reader RmInvalidXMLCharReader
	 */
	public Reader createReader() {
		try {
			RmInvalidXMLCharReader reader = new RmInvalidXMLCharReader(
					new FileInputStream(fileName), charset);
			reader.setReaderEvent(event);
			return reader;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public void setEvent(IReaderEvent event) {
		this.event = event;
	}

	/**
	 * Clone
	 * 
	 * @return MYSQLXMLDumpSource
	 */
	public MysqlXmlDumpSource clone() {
		try {
			return (MysqlXmlDumpSource) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
}
