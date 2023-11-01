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
package com.cubrid.cubridmigration.tibero.export.handler;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.export.IExportDataHandler;

public class TiberoIntervalDSTypeHandler implements
		IExportDataHandler {
	private static final Logger LOG = LogUtil.getLogger(TiberoIntervalDSTypeHandler.class);

	/**
	 * Retrieves the value object of INTERVALDS column.
	 * 
	 * @param rs the result set
	 * @param column column description
	 * @return value of column
	 * @throws SQLException e
	 */
	public Object getJdbcObject(ResultSet rs, Column column) throws SQLException {
		try {
			Method method = rs.getClass().getMethod("getINTERVALDS",
					new Class[]{String.class });
			Object intervalDS = method.invoke(rs, column.getName());
			return intervalDS == null ? null : intervalDS.toString();
			//			Method method2 = intervalDS.getClass().getMethod("toBytes");
			//			byte[] result = (byte[]) method2.invoke(intervalDS);
			//			if (result == null || result.length == 0 || result.length != 11) {
			//				return intervalDS.toString();
			//			}
			//			StringBuffer sb = new StringBuffer();
			//			sb.append(
			//					new BigInteger(new byte[]{(byte) (result[0] + 128),
			//							result[1], result[2], result[3] })).append(" ");
			//			if (Math.abs(result[4] - 60) < 10) {
			//				sb.append("0");
			//			}
			//			sb.append(Math.abs(result[4] - 60)).append(':');
			//
			//			if (Math.abs(result[5] - 60) < 10) {
			//				sb.append("0");
			//			}
			//			sb.append(Math.abs(result[5] - 60)).append(':');
			//
			//			if (Math.abs(result[6] - 60) < 10) {
			//				sb.append("0");
			//			}
			//			sb.append(Math.abs(result[6] - 60)).append('.').append(
			//					new BigInteger(new byte[]{(byte) (result[7] + 128),
			//							result[8], result[9], result[10] })).append("  ").append(
			//					intervalDS.toString());
			//			return sb.toString();
		} catch (Exception e) {
			LOG.error("", e);
			return null;
		}
	}
}
