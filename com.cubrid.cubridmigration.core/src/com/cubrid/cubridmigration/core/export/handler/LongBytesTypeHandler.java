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
package com.cubrid.cubridmigration.core.export.handler;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.export.IExportDataHandler;

/**
 * BytesTypeHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-1-12 created by Kevin Cao
 */
public class LongBytesTypeHandler implements
		IExportDataHandler {

	/**
	 * Retrieves the value object of LongBytes column.
	 * 
	 * @param rs the result set
	 * @param column column description
	 * @return null or byte[]
	 * @throws SQLException e
	 */
	public Object getJdbcObject(ResultSet rs, Column column) throws SQLException {
		return getBinaryObject(rs.getBinaryStream(column.getName()));
	}

	/**
	 * Convert Byte[] to byte[]
	 * 
	 * @param byteObjects Byte[]
	 * @return byte[]
	 */
	protected byte[] getBytesFromByteArray(Byte[] byteObjects) {
		byte[] data = null;
		if (byteObjects != null) {
			data = new byte[byteObjects.length];
			for (int i = 0; i < byteObjects.length; i++) {
				data[i] = byteObjects[i].byteValue();
			}
		}
		return data;
	}

	/**
	 * getBinaryObject
	 * 
	 * @param inputStream InputStream
	 * 
	 * @return null or Byte[]
	 * @throws SQLException e
	 */
	protected Object getBinaryObject(final InputStream inputStream) throws SQLException {
		try {
			if (inputStream == null) {
				return null;
			}

			List<Byte> list = new ArrayList<Byte>();
			byte[] buf = new byte[1024];
			int len = inputStream.read(buf);

			while (len != -1) {
				for (int i = 0; i < len; i++) {
					list.add(buf[i]);
				}
				len = inputStream.read(buf);
			}

			return getBytesFromByteArray(list.toArray(new Byte[list.size()]));
		} catch (IOException ex) {
			throw new SQLException(ex);
		} finally {
			Closer.close(inputStream);
		}
	}
}
