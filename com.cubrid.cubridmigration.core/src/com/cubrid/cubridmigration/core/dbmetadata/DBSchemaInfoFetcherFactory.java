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
package com.cubrid.cubridmigration.core.dbmetadata;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Catalog;

/**
 * DBSchemaInfoFetcherFactory build data source's schema information.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-2-20 created by Kevin Cao
 */
public final class DBSchemaInfoFetcherFactory {

	private final static IDBSchemaInfoFetcher NA_FETCHER = new IDBSchemaInfoFetcher() {

		public Catalog fetchSchema(IDBSource ds, IBuildSchemaFilter filter) {
			return null;
		}

		public void cancel() {
			//Do nothing
		}

	};

	private DBSchemaInfoFetcherFactory() {
		//Do nothing
	}

	/**
	 * Get schema information from data source (database or database's dump
	 * file)
	 * 
	 * @param ds IDataSource
	 * @return catalog
	 */
	public static IDBSchemaInfoFetcher createFetcher(IDBSource ds) {
		if (ds instanceof ConnParameters) {
			return new JDBCDBSchemaFetcherFacade();
		}
		return NA_FETCHER;
	}
}
