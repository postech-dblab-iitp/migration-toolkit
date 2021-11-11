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
package com.cubrid.cubridmigration.cubrid.stmt.handler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;

/**
 * DefaultHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-9-1 created by Kevin Cao
 */
public class BitHandler extends
		DefaultHandler {

	/**
	 * get Byte[] from InputSteam
	 * 
	 * @param in InputStream
	 * @return byte[]
	 */
	private byte[] getBytesFromInputSteam(InputStream in) {
		ByteArrayOutputStream bos = null;
		try {
			byte[] buffer = new byte[1024];
			int read;
			bos = new ByteArrayOutputStream();
			while ((read = in.read(buffer)) > 0) {
				bos.write(buffer, 0, read);
			}
			return bos.toByteArray();
		} catch (IOException ex) {
			throw new NormalMigrationException(ex);
		} finally {
			Closer.close(bos);
			Closer.close(in);
		}
	}

	/**
	 * 
	 * @param stmt PreparedStatement
	 * @param idx parameter index
	 * @param columnValue ColumnValue
	 * @throws SQLException when SQL error.
	 */
	public void handle(PreparedStatement stmt, int idx, ColumnValue columnValue) throws SQLException {
		//Column column = columnValue.getColumn();
		//Integer dataTypeID = column.getJdbcIDOfDataType();
		Object value = columnValue.getValue();
		if ("".equals(value)) {
			stmt.setString(idx + 1, "");
			return;
		}
		//if (dataTypeID == DataTypeConstant.CUBRID_DT_BIT				|| dataTypeID == DataTypeConstant.CUBRID_DT_VARBIT) {
		//try {
		byte[] bytesvalues;
		if (value instanceof Blob) {
			Blob blob = (Blob) value;
			InputStream in = blob.getBinaryStream();
			bytesvalues = getBytesFromInputSteam(in);

		} else if (value instanceof InputStream) {
			InputStream in = (InputStream) value;
			bytesvalues = getBytesFromInputSteam(in);
		} else {
			bytesvalues = (byte[]) value;
		}
		if (bytesvalues.length == 0) {
			stmt.setString(idx + 1, "");
			return;
		}
		stmt.setBytes(idx + 1, bytesvalues);
		//		} catch (IOException ex) {
		//			throw new NormalMigrationException(ex);
		//		}
	}

}