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
import java.math.BigInteger;
import java.math.RoundingMode;

import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.trans.AbstractDataConverter;

/**
 * 
 * NumericConverter Description
 * 
 * @author Kevin.Wang
 * @version 1.0 - 2011-11-29 created by Kevin.Wang
 */
public class NumericConverter extends
		AbstractDataConverter {

	/**
	 * If it is NUMERIC type and there is a decimal point,
	 * it returns the rounded value if the total number of digits exceeds 38.
	 * 
	 * @param obj Object
	 * @return value Object
	 */
	private Object getCUBRIDDataSet(Object obj) {
		Object value = obj;
		int maxSize = DataTypeConstant.NUMERIC_MAX_PRECISIE_SIZE ;
		String strValue = obj.toString() ;
		int strValueLength = strValue.length();
		
		if (strValue.charAt(0) == '0') {
			if (strValueLength == 1) {
				return value;
			}
			if (strValue.charAt(1) == '.') {
				if (strValueLength > maxSize + 2) {
					value = new BigDecimal(strValue).setScale(maxSize, RoundingMode.HALF_UP);
				}
			}
		} else {
			int index = strValue.indexOf(".");
			if ((index >= 0 && index < maxSize) && strValueLength > maxSize) {
				value = new BigDecimal(strValue).setScale(maxSize - index, RoundingMode.HALF_UP);
			}
		}

		return value;
	}

	/**
	 * @param obj Object
	 * @param dti DataTypeInstance
	 * @param config MigrationConfiguration
	 * @return value Object
	 */
	public Object convert(Object obj, DataTypeInstance dti,
			MigrationConfiguration config) {
		Object value = null;

		if (obj instanceof BigInteger) {
			return obj;
		}
		if (obj instanceof BigDecimal) {
			return getCUBRIDDataSet(obj) ;
		}
		int scale = dti.getScale();
		if (scale == 0) {
			try {
				value = new BigInteger(obj.toString());
			} catch (NumberFormatException ex) {
				throw new RuntimeException("ERROR: could not convert:" + obj
						+ " to CUBRID type Numeric", ex);
			}
		} else if (scale > 0) {
			try {
				value = getCUBRIDDataSet (new BigDecimal(obj.toString()));
			} catch (NumberFormatException ex) {
				throw new RuntimeException("ERROR: could not convert:" + obj
						+ " to CUBRID type Numeric", ex);
			}
		}

		return value;
	}
}
