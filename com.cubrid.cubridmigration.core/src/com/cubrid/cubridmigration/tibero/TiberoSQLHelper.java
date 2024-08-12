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
package com.cubrid.cubridmigration.tibero;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.common.DBUtils;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.sql.SQLHelper;



public class TiberoSQLHelper extends
		SQLHelper {
	
	private static final String NEWLINE = "\n";
	private static final String END_LINE_CHAR = ";";
	
	private static final TiberoSQLHelper INS = new TiberoSQLHelper();

	/**
	 * Singleton factory.
	 * 
	 * @param version Tibero server version
	 * @return MSSQLDDLUtil
	 */
	public static TiberoSQLHelper getInstance(String version) {
		return INS;
	}

	private TiberoSQLHelper() {
		//Hide the constructor for singleton
	}
	
	public String getTableDDL(Table table) {
			StringBuffer bf = new StringBuffer();
			bf.append("CREATE TABLE ");
			String tableName = table.getName();
			
			if (StringUtils.isEmpty(tableName)) {
				bf.append("<class_name>");
			} else {
				bf.append(getQuotedObjName(tableName));
			}
			
			// instance attribute
			List<Column> nlist = table.getColumns();
			bf.append("(").append(NEWLINE);
			for (int i = 0; i < nlist.size(); i++) {
				Column instanceAttr = nlist.get(i);
			
				if (i > 0) {
					bf.append(",").append(NEWLINE);
				}
			
				bf.append(getColumnDDL(instanceAttr, table.getPk()));
			
			}
			bf.append(NEWLINE).append(")");
			if (table.isReuseOID()) {
				bf.append(" REUSE_OID");
			}
			if (DBUtils.supportedCubridPartition(table.getPartitionInfo())) {
				bf.append(NEWLINE).append(table.getPartitionInfo().getDDL());
			}
			
			bf.append(NEWLINE).append(END_LINE_CHAR);
			
			return bf.toString();
		}
	
	private Object getColumnDDL(Column column, PK pk) {
		StringBuffer bf = new StringBuffer();
		bf.append(getQuotedObjName(column.getName()));
		bf.append(" ").append(column.getShownDataType());
		
		if (column.getShownDataType().equals("varchar")) {
			bf.append("(" + column.getPrecision() + ")");
			
			return bf.toString();
		}
		
		if (column.getPrecision() > 0) { 
			bf.append("(" + column.getPrecision() + ", " + column.getScale() + ")");
		} else {
			bf.append("");
		}

		return bf.toString();
	}

	/**
	 * append "rownum = 0" to SELECT statement
	 * 
	 * @param sql SELECT statement
	 * @return String
	 */
	public String getTestSelectSQL(String sql) {
		return "SELECT * FROM ( " + sql + " ) WHERE 1<>1";
	}

	/**
	 * return database object name
	 * 
	 * @param objectName String
	 * @return String
	 */
	public String getQuotedObjName(String objectName) {
		return new StringBuffer("\"").append(objectName).append("\"").toString();
	}
}
