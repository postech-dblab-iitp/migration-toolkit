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
package com.cubrid.cubridmigration.graph;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;

public final class GraphDataTypeHelper extends
		DBDataTypeHelper {
	private static final Logger LOG = LogUtil.getLogger(GraphDataTypeHelper.class);

	private static final String NOT_SUPPORT = "not support";
	private static final Map<String, String> CUBRID_GRAPH_DATATYPE = new HashMap<String, String>();

	private static final GraphDataTypeHelper HELPER = new GraphDataTypeHelper();
	//init all cubrid datatype
	static {
		initCUBRIDGRAPHDataTypes();
	}

	/**
	 * Singleton
	 * 
	 * @param version of oracle database
	 * @return DataTypeHelper
	 */
	public static GraphDataTypeHelper getInstance(String version) {
		return HELPER;
	}

	//if necessary, change to DataTypeSymbol
	private static void initCUBRIDGRAPHDataTypes() {
		//small int
		CUBRID_GRAPH_DATATYPE.put("short", "integer");
		//int
		CUBRID_GRAPH_DATATYPE.put("int", "integer");
		//bigint
		CUBRID_GRAPH_DATATYPE.put("bigint", "integer");
		//numeric
		CUBRID_GRAPH_DATATYPE.put("numeric", "integer");
		//float
		CUBRID_GRAPH_DATATYPE.put("float", "float");
		//double
		CUBRID_GRAPH_DATATYPE.put("double", "double");
		//monetary
		CUBRID_GRAPH_DATATYPE.put("monetary", "string");
		//char
		CUBRID_GRAPH_DATATYPE.put("char", "string");
		//varchar
		CUBRID_GRAPH_DATATYPE.put("varchar", "string");
		//time
		CUBRID_GRAPH_DATATYPE.put("time", "string");
		//date
		CUBRID_GRAPH_DATATYPE.put("date", "date");
		//timestamp
		CUBRID_GRAPH_DATATYPE.put("timestamp", "string");
		//datetime
		CUBRID_GRAPH_DATATYPE.put("datetime", "datetime");
		//bit
		CUBRID_GRAPH_DATATYPE.put("bit", NOT_SUPPORT);
		//varbit
		CUBRID_GRAPH_DATATYPE.put("varbit", NOT_SUPPORT);
		//set
		CUBRID_GRAPH_DATATYPE.put("set",NOT_SUPPORT);
		//multiset
		CUBRID_GRAPH_DATATYPE.put("multiset", NOT_SUPPORT);
		//sequence
		CUBRID_GRAPH_DATATYPE.put("sequence", NOT_SUPPORT);
		//glo
		CUBRID_GRAPH_DATATYPE.put("glo", NOT_SUPPORT);
		//object
		CUBRID_GRAPH_DATATYPE.put("object", NOT_SUPPORT);
		//clob
		CUBRID_GRAPH_DATATYPE.put("clob", NOT_SUPPORT);
		//blob
		CUBRID_GRAPH_DATATYPE.put("blob", NOT_SUPPORT);
		//enum
		CUBRID_GRAPH_DATATYPE.put("enum", NOT_SUPPORT);
	}
	
	public String getGraphDataType(String type) {
		return CUBRID_GRAPH_DATATYPE.get(type);
	}
	
	public boolean SupportDataType(String type) {
		if (CUBRID_GRAPH_DATATYPE.get(type).equals(NOT_SUPPORT)){
			return false;
		}
		return true;
	}

	public DatabaseType getDBType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getJdbcDataTypeID(Catalog catalog, String dataType,
			Integer precision, Integer scale) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getShownDataType(Column column) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isBinary(String dataType) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCollection(String dataType) {
		// TODO Auto-generated method stub
		return false;
	}
}
