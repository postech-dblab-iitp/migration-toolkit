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
package com.cubrid.cubridmigration.oracle.export.handler;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.export.handler.BlobTypeHandler;

/**
 * OracleBFileTypeHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-1-12 created by Kevin Cao
 */
public class OracleBFileTypeHandler extends
		BlobTypeHandler {
	private static final Logger LOG = LogUtil.getLogger(OracleBFileTypeHandler.class);

	/**
	 * Retrieves the value object of BFile column.
	 * 
	 * @param rs the result set
	 * @param column column description
	 * @return value of column
	 * @throws SQLException e
	 */
	public Object getJdbcObject(ResultSet rs, Column column) throws SQLException {
		try {
			ClassLoader dbClassLoader = rs.getClass().getClassLoader();
			Method method = Class.forName("oracle.jdbc.OracleResultSet", false,
					dbClassLoader).getMethod("getBFILE",
					new Class[]{String.class });
			Object bfile = method.invoke(rs, column.getName());
			if (bfile == null) {
				return null;
			}

			Class<?> bfileClass = Class.forName("oracle.sql.BFILE", false,
					dbClassLoader);
			Method openMethod = bfileClass.getMethod("open");
			openMethod.invoke(bfile);

			Method getBinaryStreamMethod = bfileClass.getMethod("getBinaryStream");
			return getBinaryObject((InputStream) getBinaryStreamMethod.invoke(bfile));
		} catch (Exception e) {
			LOG.error("", e);
			return null;
		}
	}

}
