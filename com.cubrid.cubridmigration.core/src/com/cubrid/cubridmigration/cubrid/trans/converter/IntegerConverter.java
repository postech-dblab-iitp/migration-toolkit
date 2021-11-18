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

import java.math.BigDecimal;

import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.trans.AbstractDataConverter;

/**
 * 
 * IntegerConverter Description
 * 
 * @author Kevin.Wang
 * @version 1.0 - 2011-11-29 created by Kevin.Wang
 */
public class IntegerConverter extends
		AbstractDataConverter {

	/**
	 * @param obj Object
	 * @param dti DataTypeInstance
	 * @param config MigrationConfiguration
	 * @return value Object
	 */
	public Object convert(Object obj, DataTypeInstance dti,
			MigrationConfiguration config) {
		if (obj instanceof Integer) {
			return obj;
		}
		if (obj instanceof Double) {
			double dbvalue = (Double) obj;
			return ((int) dbvalue);
		} else if (obj instanceof Float) {
			float dbvalue = (Float) obj;
			return ((int) dbvalue);
		} else if (obj instanceof BigDecimal) {
			BigDecimal dbvalue = (BigDecimal) obj;
			return dbvalue.intValue();
		}
		try {
			return Integer.valueOf(obj.toString());
		} catch (NumberFormatException ex) {
			throw new RuntimeException("ERROR: could not convert:" + obj
					+ " to CUBRID type Integer", ex);
		}
	}
}
