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
package com.cubrid.cubridmigration.mysql.trans;

import java.util.Locale;

import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;

/**
 * MySQLDataTypeMapping
 * 
 * @author Kevin.Wang
 * @version 1.0 - 2011-11-23 created by Kevin.Wang
 */
public class MySQLDataTypeMappingHelper extends
		AbstractDataTypeMappingHelper {

	public MySQLDataTypeMappingHelper() {
		super("MySQL2CUBRID",
				"/com/cubrid/cubridmigration/mysql/trans/MySQL2CUBRID.xml");
	}

	/**
	 * get the config map key
	 * 
	 * @param datatype String
	 * @param precision String
	 * @param scale String
	 * @return key String
	 */
	public String getMapKey(String datatype, String precision, String scale) {
		if ("bit".equals(datatype) && "1".equals(precision)) {
			return datatype + precision;
		} else if ("bit".equals(datatype) && !"1".equals(precision)) {
			return datatype.toLowerCase(Locale.ENGLISH);
		}

		return datatype.toLowerCase(Locale.ENGLISH);
	}

}
