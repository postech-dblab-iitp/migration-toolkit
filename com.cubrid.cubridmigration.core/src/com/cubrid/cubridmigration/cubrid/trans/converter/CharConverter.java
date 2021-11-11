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
package com.cubrid.cubridmigration.cubrid.trans.converter;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

import com.cubrid.cubridmigration.core.common.CharsetUtils;
import com.cubrid.cubridmigration.core.common.CommonUtils;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.trans.AbstractDataConverter;

/**
 * Char,Varchar,NChar,NVarchar converter
 * 
 * @author Kevin Wang 2011-9-15
 */
public class CharConverter extends
		AbstractDataConverter {

	//	/**
	//	 * return String of a Reader
	//	 * 
	//	 * @param reader Reader
	//	 * @return String
	//	 * @throws IOException e
	//	 */
	//	private String getStringFromReader(Reader reader) throws IOException {
	//		try {
	//			StringBuffer buf = new StringBuffer();
	//			char[] ch = new char[1024];
	//			int length = reader.read(ch);
	//
	//			while (length != -1) {
	//				String str = new String(ch, 0, length);
	//				length = reader.read(ch);
	//				buf.append(str);
	//			}
	//
	//			return buf.toString();
	//		} finally {
	//			Closer.close(reader);
	//		}
	//	}

	/**
	 * to CUBRID string type
	 * 
	 * @param obj Object
	 * @param config Migration configuration
	 * @return Object of string
	 * @throws Exception when error
	 */
	protected Object toCUBRIDString(Object obj, MigrationConfiguration config) throws Exception {
		if (obj instanceof String) {
			return obj;
		}
		//		if (obj instanceof Clob) {
		//			Clob clob = (Clob) obj;
		//			Reader reader = clob.getCharacterStream();
		//			return getStringFromReader(reader);
		//
		//		}
		//		if (obj instanceof InputStream) {
		//			InputStream in = (InputStream) obj;
		//			Reader reader = new InputStreamReader(in, config.getSourceCharset());
		//			return getStringFromReader(reader);
		//		}

		if (obj instanceof Boolean) {
			return booleanToString((Boolean) obj);
		}

		if (obj instanceof char[]) {
			return new String((char[]) obj);
		}

		if (obj instanceof byte[]) {
			try {
				return new String((byte[]) obj, config.getSourceCharset());
			} catch (UnsupportedEncodingException ex) {
				return new String(
						(byte[]) obj,
						CharsetUtils.turnOracleCharset2Normal(config.getSourceCharset()));
			}
		}
		if (obj instanceof BigDecimal) {
			return CommonUtils.formatCUBRIDNumber((BigDecimal) obj);
		}
		return obj.toString();
	}

	/**
	 * convert boolean To String
	 * 
	 * @param flag Boolean
	 * @return String
	 */
	protected String booleanToString(Boolean flag) {
		if (flag.booleanValue()) {
			return "y";
		} else {
			return "n";
		}
	}

	/**
	 * @param obj Object
	 * @param dti DataTypeInstance
	 * @param config MigrationConfiguration
	 * @return value Object
	 */
	public Object convert(Object obj, DataTypeInstance dti,
			MigrationConfiguration config) {
		try {
			return toCUBRIDString(obj, config);
		} catch (Exception e) {
			throw new RuntimeException("", e);
		}
	}
}
