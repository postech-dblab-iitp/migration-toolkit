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
package com.cubrid.cubridmigration.graph.trans;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.tibero.TiberoDataTypeHelper;

/**
 * CubridDatatypeMapping
 * 
 * @author Kevin.Wang
 * @version 1.0 - 2011-11-23 created by Kevin.Wang
 */
public class Neo4jToTiberoDataTypeMappingHelper extends
		AbstractDataTypeMappingHelper {
	private static final Logger LOG = LogUtil.getLogger(AbstractDataTypeMappingHelper.class);
	
	/**
	 * @param databaseTypeID
	 * @param key
	 * @param dataTypeMappingFileName
	 */
	public Neo4jToTiberoDataTypeMappingHelper() {
		super("Neo4j2Tibero",
				"/com/cubrid/cubridmigration/graph/trans/Neo4j2Tibero.xml");
	}

	/**
	 * get the mapkey
	 * 
	 * @param datatype String
	 * @param precision String
	 * @param scale String
	 * @return key String
	 */
	public String getMapKey(String datatype, String precision, String scale) {
		String dataTypeUpper = datatype;
//		if ("NUMBER".equals(dataTypeUpper)) {
//			return getNumberMapKey(dataTypeUpper, precision);
//		} else if (dataTypeUpper.matches("INTERVAL DAY\\(\\d*\\) TO SECOND\\(\\d*\\)")) {
//			return "INTERVAL DAY TO SECOND";
//		} else if (dataTypeUpper.matches("INTERVAL YEAR\\(\\d*\\) TO MONTH")) {
//			return "INTERVAL YEAR TO MONTH";
//		} else {
//			return OracleDataTypeHelper.getOracleDataTypeKey(dataTypeUpper);
//		}
		return TiberoDataTypeHelper.getTiberoDataTypeKey(dataTypeUpper);
	}
}
