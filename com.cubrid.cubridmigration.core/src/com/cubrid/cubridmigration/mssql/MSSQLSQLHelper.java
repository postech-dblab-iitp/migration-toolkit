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
package com.cubrid.cubridmigration.mssql;

import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDSQLHelper;

/**
 * 
 * MSSQL server SQL helper
 * 
 * @author Kevin Cao
 * @version 1.0 - 2010-11-25
 */
public class MSSQLSQLHelper extends
		SQLHelper {

	//	private static final String[] UPDATE_RULE = new String[]{"CASCADE",
	//			"RESTRICT", "NO ACTION", "SET NULL" };
	//	private static final String[] DELETE_RULE = new String[]{"CASCADE",
	//			"RESTRICT", "NO ACTION", "SET NULL" };

	//private String newLine = "\n";
	//private String endLineChar = ";";

	private static final MSSQLSQLHelper INS = new MSSQLSQLHelper();

	/**
	 * Singleton factory.
	 * 
	 * @param version MSSQL server version
	 * @return MSSQLDDLUtil
	 */
	public static MSSQLSQLHelper getInstance(String version) {
		return INS;
	}

	private MSSQLSQLHelper() {
		//Hide the constructor for singleton
	}

	//	public String getEndLineChar() {
	//		return endLineChar;
	//	}

	//private final List<String> notNullChangedColumn = new ArrayList<String>();

	//	/**
	//	 * return DDL of adding index, unique, reverse index or reverse unique
	//	 * 
	//	 * @param tableName String
	//	 * @param index Index
	//	 * @return String
	//	 */
	//	private String addIndex(String tableName, Index index) {
	//		StringBuffer bf = new StringBuffer();
	//		bf.append("CREATE");
	//		if (index.isUnique()) {
	//			bf.append(" UNIQUE");
	//		}
	//		bf.append(" INDEX");
	//		String idxName = index.getName();
	//		if (StringUtils.isNotBlank(idxName)) {
	//			bf.append(" ").append(getQuotedObjName(idxName));
	//		}
	//		bf.append(" ON ").append(getQuotedObjName(tableName));
	//
	//		List<String> list = new ArrayList<String>();
	//		List<String> rules = index.getColumnOrderRulesString();
	//		List<String> columns = index.getColumnNames();
	//
	//		for (int i = 0; i < columns.size(); i++) {
	//			String columnName = columns.get(i);
	//			String rule = rules.get(i);
	//
	//			if ("A".equals(rule) || "NULL".equals(rule)) {
	//				list.add(getQuotedObjName(columnName));
	//			} else {
	//				list.add(getQuotedObjName(columnName) + " DESC");
	//			}
	//		}
	//		bf.append("(");
	//		int count = 0;
	//		for (String str : list) {
	//			if (0 != count) {
	//				bf.append(",");
	//			}
	//			bf.append(str);
	//			count++;
	//		}
	//		bf.append(")");
	//		return bf.toString();
	//	}

	//	/**
	//	 * DDL of adding FK
	//	 * 
	//	 * @param tableName String
	//	 * @param fk FK
	//	 * @return String
	//	 */
	//	private String addFK(String tableName, FK fk) {
	//		StringBuffer bf = new StringBuffer();
	//		bf.append("ALTER TABLE ");
	//		bf.append(getQuotedObjName(tableName));
	//		bf.append(" ADD ");
	//		bf.append(getFKDDL(fk));
	//		return bf.toString();
	//	}

	//	/**
	//	 * DDL of adding PK
	//	 * 
	//	 * @param tableName String
	//	 * @param pkName String
	//	 * @param pkColumns List<String>
	//	 * @return String
	//	 */
	//	private String addPK(String tableName, String pkName, List<String> pkColumns) {
	//		StringBuffer bf = new StringBuffer();
	//
	//		bf.append("ALTER TABLE ").append(getQuotedObjName(tableName)).append(
	//				" ADD");
	//		int count = 0;
	//		if (StringUtils.isNotBlank(pkName)) {
	//			bf.append(" CONSTRAINT ").append(getQuotedObjName(pkName));
	//		}
	//		bf.append(" PRIMARY KEY(");
	//		for (String column : pkColumns) {
	//			if (count > 0) {
	//				bf.append(",");
	//			}
	//			bf.append(getQuotedObjName(column));
	//			count++;
	//		}
	//		bf.append(")");
	//		return bf.toString();
	//	}

	//	/**
	//	 * get a table's fk ddls
	//	 * 
	//	 * @param table Table
	//	 * @return String
	//	 */
	//	public String getTableFKDDL(Table table) {
	//		StringBuffer bf = new StringBuffer();
	//		String tableName = table.getName();
	//
	//		// add FKs to target tables
	//		List<FK> fks = table.getFks();
	//
	//		if (fks != null && !fks.isEmpty()) {
	//			for (FK fk : fks) {
	//				bf.append(addFK(tableName, fk));
	//				bf.append(endLineChar).append(newLine);
	//			}
	//		}
	//
	//		return bf.toString();
	//	}

	//	/**
	//	 * get a table's pk ddl
	//	 * 
	//	 * @param table Table
	//	 * @return String
	//	 */
	//	public String getTablePKAndIndexDDL(Table table) {
	//		StringBuffer bf = new StringBuffer();
	//		String tableName = table.getName();
	//		PK pk = table.getPk();
	//		List<String> pkColumns = pk == null ? new ArrayList<String>()
	//				: pk.getPkColumns();
	//
	//		if (!pkColumns.isEmpty()) {
	//			bf.append(addPK(tableName, pk.getName(), pkColumns));
	//			bf.append(endLineChar).append(newLine);
	//		}
	//
	//		// constraint
	//		List<Index> constaintList = table.getIndexes();
	//
	//		if (!constaintList.isEmpty()) {
	//			for (int i = 0; i < constaintList.size(); i++) {
	//				Index constraint = constaintList.get(i);
	//				String indexDDL = addIndex(tableName, constraint);
	//				if (!"".equals(indexDDL)) {
	//					bf.append(indexDDL);
	//					bf.append(endLineChar).append(newLine);
	//				}
	//			}
	//		}
	//		return bf.toString();
	//	}

	/**
	 * return database object name
	 * 
	 * @param objectName String
	 * @return String
	 */
	public String getQuotedObjName(String objectName) {
		return new StringBuffer("[").append(objectName).append("]").toString();
	}

	//	/**
	//	 * return DDL of a table
	//	 * 
	//	 * @param table Table
	//	 * @return String
	//	 */
	//	public String getTableDDL(Table table) {
	//		StringBuffer bf = new StringBuffer();
	//		bf.append("CREATE TABLE ");
	//		String tableName = table.getName();
	//
	//		if (null == tableName || tableName.equals("")) {
	//			bf.append("<class_name>");
	//		} else {
	//			bf.append(getQuotedObjName(tableName));
	//		}
	//
	//		boolean attrBegin = false;
	//		int count = 0;
	//
	//		// instance attribute
	//
	//		count = 0;
	//		attrBegin = false;
	//		List<Column> nlist = table.getColumns();
	//
	//		if (!nlist.isEmpty()) {
	//			for (int i = 0; i < nlist.size(); i++) {
	//				Column instanceAttr = nlist.get(i);
	//
	//				if (count == 0) {
	//					if (!attrBegin) {
	//						bf.append("(").append(newLine);
	//						attrBegin = true;
	//					}
	//				} else {
	//					bf.append(",").append(newLine);
	//				}
	//
	//				bf.append(getColumnDDL(instanceAttr, table.getPk()));
	//				count++;
	//
	//			}
	//		}
	//
	//		if (count > 0) {
	//			bf.append(newLine).append(");");
	//		}
	//
	//		return bf.toString();
	//	}

	//	/**
	//	 * DDL of FK in creating and altering a schema
	//	 * 
	//	 * @param fk FK
	//	 * @return String
	//	 */
	//	private String getFKDDL(FK fk) {
	//		StringBuffer bf = new StringBuffer();
	//
	//		bf.append("FOREIGN KEY");
	//
	//		List<String> list = fk.getColumnNames();
	//		bf.append(" (");
	//
	//		for (int i = 0; i < list.size(); i++) {
	//			if (i != 0) {
	//				bf.append(",");
	//			}
	//
	//			bf.append(getQuotedObjName(list.get(i)));
	//		}
	//
	//		bf.append(")");
	//
	//		String refTable = fk.getReferencedTableName();
	//		bf.append(" REFERENCES ").append(getQuotedObjName(refTable));
	//
	//		bf.append("(");
	//
	//		List<String> pklist = fk.getCol2RefMapping();
	//
	//		for (int i = 0; i < pklist.size(); i++) {
	//			if (i != 0) {
	//				bf.append(",");
	//			}
	//
	//			bf.append(getQuotedObjName(pklist.get(i)));
	//		}
	//		bf.append(")");
	//		bf.append(" ON DELETE ").append(DELETE_RULE[fk.getDeleteRule()]);
	//		bf.append(" ON UPDATE ").append(UPDATE_RULE[fk.getUpdateRule()]);
	//		return bf.toString();
	//	}

	//	/**
	//	 * DDL of a column in creating a table
	//	 * 
	//	 * @param column Column
	//	 * @param pk PK
	//	 * @return String
	//	 */
	//	private String getColumnDDL(Column column, PK pk) {
	//		StringBuffer bf = new StringBuffer();
	//		bf.append(getQuotedObjName(column.getName()));
	//		bf.append(" ").append(column.getShownDataType());
	//		String defaultv = column.getDefaultValue();
	//
	//		if (defaultv == null) {
	//			boolean autoInc = column.isAutoIncrement();
	//			if (autoInc) {
	//				bf.append(" IDENTITY");
	//
	//				if (column.getAutoIncMaxVal() > 0) {
	//					bf.append("(").append(column.getAutoIncMaxVal()).append(",").append(
	//							column.getAutoIncIncrVal()).append(")");
	//				}
	//
	//			}
	//		} else {
	//
	//			if (defaultv != null) {
	//				bf.append(" DEFAULT ").append(defaultv);
	//			}
	//
	//		}
	//		if (!column.isNullable()) {
	//			if (pk != null && !pk.getPkColumns().contains(column.getName())
	//					|| pk == null) { //NOPMD
	//				bf.append(" NOT NULL");
	//			}
	//		}
	//		return bf.toString();
	//	}

	//	public List<String> getNotNullChangedColumn() {
	//		return notNullChangedColumn;
	//	}

	/**
	 * Retrieves the MSSQL tables' partition DDL. TODO: now it uses the CUBRID's
	 * DDL schema.
	 * 
	 * @param table Table
	 * @return String
	 */
	public String getTablePartitonDDL(Table table) {
		return CUBRIDSQLHelper.getInstance(null).getTablePartitonDDL(table);
	}

	/**
	 * return test SELECT statement to verify where condition
	 * 
	 * @param sql SELECT statement
	 * @return String
	 */
	public String getTestSelectSQL(String sql) {
		String result = replacePageQueryParameters(sql, 1L, 0L);
		//		result = replacePageSizeParameterToValue(result, SQLPARAM_PAGE_END, "1");
		//		result = replacePageSizeParameterToValue(result, SQLPARAM_TOTAL_EXPORTED, "0");
		if (sql.equals(result)) {
			result = "SELECT TOP 1 * FROM (" + sql + ") tartbl";
		}
		return result;
	}
}
