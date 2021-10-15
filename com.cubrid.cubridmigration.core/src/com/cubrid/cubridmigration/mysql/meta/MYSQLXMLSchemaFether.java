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
package com.cubrid.cubridmigration.mysql.meta;

import java.io.IOException;
import java.io.Reader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbmetadata.IBuildSchemaFilter;
import com.cubrid.cubridmigration.core.dbmetadata.IDBSchemaInfoFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IDBSource;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.mysql.MysqlXmlDumpSource;

/**
 * Build MySQL meta data from a XML file
 * 
 * @author caoyilin
 * @version 1.0 - 2010-9-15
 */
public class MYSQLXMLSchemaFether implements
		IDBSchemaInfoFetcher {

	private Runnable cancelRunnable;

	/**
	 * Parsing MySQL XML dump file and retrieves the file's catalog.
	 * 
	 * @param ds IDBSource
	 * @param filter IBuildSchemaFilter
	 * @return Catalog
	 */
	public Catalog fetchSchema(IDBSource ds, IBuildSchemaFilter filter) {
		if (cancelRunnable != null) {
			throw new RuntimeException("One fetching work is running");
		}
		MysqlXmlDumpSource mysqlXml = (MysqlXmlDumpSource) ds;
		final Reader reader = mysqlXml.createReader();

		cancelRunnable = new Runnable() {

			public void run() {
				try {
					reader.close();
				} catch (IOException e) {
					// DO nothing
				}
			}
		};
		try {
			try {
				SAXParserFactory sf = SAXParserFactory.newInstance();
				sf.setValidating(false);
				SAXParser sp = sf.newSAXParser();
				//		sp.setProperty(
				//				"http://apache.org/xml/features/continue-after-fatal-error",
				//				true);
				MySQLXMLSchemaParser structReader = new MySQLXMLSchemaParser();

				InputSource is = new InputSource(reader);
				sp.parse(is, structReader);
				final Catalog catalog = structReader.getCatalog();
				catalog.setCharset(mysqlXml.getCharset());
				return catalog;
			} finally {
				cancelRunnable = null;
				Closer.close(reader);
			}
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Cancel parsing progress
	 */
	public void cancel() {
		if (cancelRunnable != null) {
			cancelRunnable.run();
			cancelRunnable = null;
		}
	}
}
