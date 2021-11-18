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
package com.cubrid.cubridmigration.cubrid;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.DBUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.PartitionTable;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.sql.SQLHelper;

/**
 * 
 * CUBRIDSQLHelper
 * 
 * @author moulinwang Kevin Cao
 * @version 1.0 - 2009-9-18
 */
public class CUBRIDSQLHelper extends
		SQLHelper {

	private static final Logger LOG = LogUtil.getLogger(CUBRIDSQLHelper.class);

	private static final String[] UPDATE_RULE = new String[] {"CASCADE", "RESTRICT", "SET NULL",
			"NO ACTION"};
	private static final String[] DELETE_RULE = new String[] {"CASCADE", "RESTRICT", "SET NULL",
			"NO ACTION"};
	private static final String NEWLINE = "\n";
	private static final String HINT = "/*+ NO_STATS */";
	private static final String END_LINE_CHAR = ";";

	private final static CUBRIDSQLHelper HELPER = new CUBRIDSQLHelper();

	/**
	 * Get CUBRID DDL utils
	 * 
	 * @param version CUBRID version string
	 * @return UTIL
	 */
	public static CUBRIDSQLHelper getInstance(String version) {
		return HELPER;
	}

	private CUBRIDSQLHelper() {
		//Hide constructor.
	}

	/**
	 * DDL of a column in creating a table
	 * 
	 * @param column Column
	 * @param pk PK
	 * @return String
	 */
	private String getColumnDDL(Column column, PK pk) {
		StringBuffer bf = new StringBuffer();
		bf.append(getQuotedObjName(column.getName()));
		bf.append(" ").append(column.getShownDataType());

		boolean autoInc = column.isAutoIncrement();
		String value = column.getDefaultValue();
		if (autoInc) {
			bf.append(" AUTO_INCREMENT");

			final Long autoIncSeedVal = column.getAutoIncSeedVal();
			Long autoIncIncVal = column.getAutoIncIncrVal();
			autoIncIncVal = autoIncIncVal == null ? 1 : autoIncIncVal;
			if (autoIncSeedVal > 0) {
				bf.append(" (");
				bf.append(autoIncSeedVal);
				bf.append(",");
				bf.append(autoIncIncVal);
				bf.append(")");
			}
		} else if (column.isShared()) {
			String defaultv = CUBRIDFormator.format(column.getDataTypeInstance(),
					column.getSharedValue()).getFormatResult();
			bf.append(" SHARED ").append(defaultv);
		} else if ((column.getDataType().equals("datetime") || column.getDataType().equals(
				"timestamp"))
				&& ("CURRENT_TIMESTAMP".equalsIgnoreCase(value)
						|| "SYS_TIMESTAMP".equalsIgnoreCase(value)
						|| "SYSTIMESTAMP".equalsIgnoreCase(value)
						|| "SYS_DATETIME".equalsIgnoreCase(value)
						|| "SYSDATETIME".equalsIgnoreCase(value)
						|| "CURRENT_DATETIME".equalsIgnoreCase(value)
						|| "CURRENT_DATETIME()".equalsIgnoreCase(value) || "NOW()".equalsIgnoreCase(value))) {
			bf.append(" DEFAULT ").append(value);
		} else {
			String defaultv = column.getDefaultValue();
			if (!column.isDefaultIsExpression()) {
				defaultv = CUBRIDFormator.format(column.getDataTypeInstance(),
						column.getDefaultValue()).getFormatResult();
			}
			if (defaultv != null) {
				bf.append(" DEFAULT ").append(defaultv);
			}
		}
		//add for bug484
		final boolean isNotPKColumn = pk == null || !pk.getPkColumns().contains(column.getName());
		if (!column.isNullable() && isNotPKColumn) {
			bf.append(" NOT NULL");
		}
		if (column.isShared() || !column.isUnique() || !isNotPKColumn) {
			return bf.toString();
		}
		if (column.getTableOrView() instanceof Table) {
			boolean flag = false;
			for (Index idx : ((Table) column.getTableOrView()).getIndexes()) {
				if (!idx.isUnique()) {
					continue;
				}
				if (idx.getColumnNames().indexOf(column.getName()) >= 0) {
					flag = true;
				}
			}
			if (!flag) {
				bf.append(" UNIQUE ");
			}
		}
		return bf.toString();
	}

	/**
	 * return db qualifier
	 * 
	 * @param objectName String
	 * @return String
	 */
	public String getDBQualifier(String objectName) {
		//If it is an expression
		if (objectName.indexOf('(') >= 0) {
			return objectName;
		}
		return getQuotedObjName(objectName);
	}

	/**
	 * DDL of FK in creating and altering a schema
	 * 
	 * @param fk FK
	 * @return String
	 */
	private String getFKDDL(FK fk) {
		StringBuffer bf = new StringBuffer();

		bf.append(getQuotedObjName(fk.getName()));
		bf.append(" FOREIGN KEY");

		bf.append(" (");
		List<String> columnNames = fk.getColumnNames();
		for (int i = 0; i < columnNames.size(); i++) {
			if (i != 0) {
				bf.append(",");
			}

			bf.append(getQuotedObjName(columnNames.get(i)));
		}
		bf.append(")");

		String refTable = fk.getReferencedTableName();
		bf.append(" REFERENCES ").append(getQuotedObjName(refTable));

		bf.append("(");

		List<String> pklist = fk.getCol2RefMapping();

		for (int i = 0; i < pklist.size(); i++) {
			if (i != 0) {
				bf.append(",");
			}

			bf.append(getQuotedObjName(pklist.get(i)));
		}

		bf.append(")");
		bf.append(" ON DELETE ").append(DELETE_RULE[fk.getDeleteRule()]);
		bf.append(" ON UPDATE ").append(UPDATE_RULE[fk.getUpdateRule()]);
		return bf.toString();
	}

	/**
	 * DDL of adding FK
	 * 
	 * @param tableName String
	 * @param fk FK
	 * @return String
	 */
	public String getFKDDL(String tableName, FK fk) {
		StringBuffer bf = new StringBuffer();
		bf.append("ALTER " + HINT + " TABLE ");
		bf.append(getQuotedObjName(tableName));
		bf.append(" ADD CONSTRAINT ");
		bf.append(getFKDDL(fk));
		return bf.toString();
	}

	/**
	 * return DDL of adding index, unique, reverse index or reverse unique
	 * 
	 * @param tableName String
	 * @param index Index
	 * @param prefix index name prefix
	 * @return String
	 */
	public String getIndexDDL(String tableName, Index index, String prefix) {
		String defaultName = index.getName();
		StringBuffer bf = new StringBuffer();
		bf.append("CREATE " + HINT + " ");

		if (index.isReverse()) {
			bf.append(" REVERSE ");
		}
		if (index.isUnique()) {
			bf.append(" UNIQUE ");
		}
		bf.append("INDEX");
		if (defaultName != null) {
			bf.append(" ").append(getDBQualifier((prefix == null ? "" : prefix) + index.getName()));
		}

		bf.append(" ON ").append(getQuotedObjName(tableName));

		List<String> list = new ArrayList<String>();

		//if (type == Index.TYPE_NORMAL || type == Index.TYPE_UNIQUE) {
		List<Boolean> rules = index.getColumnOrderRules();
		List<String> columns = index.getColumnNames();

		for (int i = 0; i < columns.size(); i++) {
			String columnName = columns.get(i);
			Boolean rule = rules.get(i);

			if (rule) {
				list.add(getDBQualifier(columnName));
			} else {
				list.add(getDBQualifier(columnName) + " DESC");
			}
		}

		bf.append("(");
		int count = 0;

		for (String str : list) {
			if (0 != count) {
				bf.append(",");
			}

			bf.append(str);
			count++;
		}

		bf.append(")");

		return bf.toString();
	}

	/**
	 * DDL of adding PK
	 * 
	 * @param tableName String
	 * @param pkName String
	 * @param pkColumns List<String>
	 * @return String
	 */
	public String getPKDDL(String tableName, String pkName, List<String> pkColumns) {
		StringBuffer bf = new StringBuffer();

		bf.append("ALTER " + HINT + " TABLE ").append(getQuotedObjName(tableName)).append(" ADD");
		if (StringUtils.isNotBlank(pkName)) {
			bf.append(" CONSTRAINT ").append(getQuotedObjName(pkName));
		}
		bf.append(" PRIMARY KEY(");
		int count = 0;

		for (String column : pkColumns) {
			if (count > 0) {
				bf.append(",");
			}

			bf.append(getQuotedObjName(column));
			count++;
		}
		bf.append(")");
		return bf.toString();
	}

	/**
	 * get serial ddl
	 * 
	 * @param sequence Sequence
	 * @return String boolean isNoCache; boolean isNoMinValue; boolean
	 *         isNoMaxValue;
	 */
	public String getSequenceDDL(Sequence sequence) {
		if (sequence == null) {
			return "";
		}
		StringBuffer buf = new StringBuffer(256);
		buf.append("CREATE SERIAL ").append(getQuotedObjName(sequence.getName()));

		buf.append(" START WITH ").append(String.valueOf(sequence.getCurrentValue()));

		buf.append(" INCREMENT BY ").append(String.valueOf(sequence.getIncrementBy()));

		if (sequence.isNoMinValue()) {
			buf.append(" NOMINVALUE ");
		} else {
			buf.append(" MINVALUE ").append(String.valueOf(sequence.getMinValue()));
		}

		if (sequence.isNoMaxValue()) {
			buf.append(" NOMAXVALUE ");
		} else if (sequence.getMaxValue() != null) {
			buf.append(" MAXVALUE  ").append(sequence.getMaxValue().toString());
		}

		if (sequence.isCycleFlag()) {
			buf.append(" CYCLE");
		} else {
			buf.append(" NOCYCLE");
		}

		if (sequence.getCacheSize() == 0) {
			buf.append(" NOCACHE");
		} else {
			buf.append(" CACHE ").append(String.valueOf(sequence.getCacheSize()));
		}

		return buf.toString();
	}

	/**
	 * return DDL of a table
	 * 
	 * @param table Table
	 * @return String
	 */
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
	//		for (FK fk : fks) {
	//			bf.append(addFK(tableName, fk));
	//			bf.append(END_LINE_CHAR).append(NEWLINE);
	//		}
	//
	//		return bf.toString();
	//	}

	//	public List<String> getNotNullChangedColumn() {
	//		return notNullChangedColumn;
	//	}

	/**
	 * getTablePartitonDDL
	 * 
	 * @param table Table
	 * @return String
	 */
	public String getTablePartitonDDL(Table table) {
		PartitionInfo partInfo = table.getPartitionInfo();
		if (partInfo == null) {
			return null;
		}
		try {
			String partMethod = partInfo.getPartitionMethod();

			if (PartitionInfo.PARTITION_METHOD_KEY.equals(partMethod)
					|| PartitionInfo.PARTITION_METHOD_LINEARKEY.equals(partMethod)) {
				return partInfo.getDDL();
			}
			StringBuffer buf = new StringBuffer("PARTITION BY ");
			//Expression
			StringBuffer exp = new StringBuffer();
			exp.append("(");
			if (StringUtils.isEmpty(partInfo.getPartitionFunc())) {
				exp.append("\"").append(partInfo.getPartitionColumns().get(0).getName()).append(
						"\"");
			} else {
				exp.append(DBUtils.getCubridPartitionExp(partInfo));
			}
			exp.append(")");

			List<PartitionTable> partitions = partInfo.getPartitions();
			if (PartitionInfo.PARTITION_METHOD_RANGE.equals(partMethod)) {
				buf.append(PartitionInfo.PARTITION_METHOD_RANGE).append(" ");
				buf.append(exp).append("(").append(NEWLINE);
				for (int i = 0; i < partitions.size(); i++) {
					PartitionTable partTable = partitions.get(i);
					buf.append("PARTITION \"");
					buf.append(partTable.getPartitionName());
					buf.append("\" VALUES LESS THAN ");

					String partitionDesc = partTable.getPartitionDesc();

					if ("MAXVALUE".equalsIgnoreCase(partitionDesc)) {
						buf.append(partitionDesc);
					} else {
						buf.append("(");
						buf.append(partitionDesc);
						buf.append(")");
					}

					if (i != partitions.size() - 1) {
						buf.append(",");
						buf.append(NEWLINE);
					}
				}
				buf.append(NEWLINE).append(")");
			} else if (PartitionInfo.PARTITION_METHOD_LIST.equals(partMethod)) {
				buf.append(PartitionInfo.PARTITION_METHOD_LIST).append(" ");
				buf.append(exp).append(" ").append("( ").append(NEWLINE);
				for (int i = 0; i < partitions.size(); i++) {
					PartitionTable partitionTable = partitions.get(i);

					buf.append("PARTITION ");
					buf.append(partitionTable.getPartitionName());

					buf.append(" VALUES IN (");

					String partitionDesc = partitionTable.getPartitionDesc();

					if (partInfo.getPartitionFunc() == null) {
						String[] splits = partitionDesc.split(",");

						for (int j = 0; j < splits.length; j++) {
							if (j != 0) {
								buf.append(",");
							}
							buf.append(splits[j]);
						}

					} else {
						buf.append(partitionDesc);
					}

					buf.append(")");

					if (i != partitions.size() - 1) {
						buf.append(",");
						buf.append(NEWLINE);
					}

				}
				buf.append(NEWLINE).append(" ) ");
			} else if (PartitionInfo.PARTITION_METHOD_HASH.equals(partMethod)
					|| PartitionInfo.PARTITION_METHOD_LINEARHASH.equals(partMethod)) {
				buf.append(PartitionInfo.PARTITION_METHOD_HASH).append(" ");
				buf.append(exp).append(" ").append(NEWLINE);
				buf.append("PARTITIONS ").append(partitions.size());
			}
			return buf.toString();
		} catch (Exception e) {
			LOG.error("", e);
		}
		return "";
	}

	/***
	 * getDDL
	 * 
	 * @param view View
	 * @return String
	 */
	public String getViewDDL(View view) {
		if (view == null) {
			return "";
		}
		StringBuffer sb = new StringBuffer();
		sb.append("CREATE OR REPLACE VIEW ");
		String viewName = view.getName();

		if (viewName != null) {
			sb.append(getQuotedObjName(viewName));
		}
		//Column definitions are not necessarily.
		//		sb.append("(");
		//		List<Column> list = view.getColumns();
		//
		//		for (Iterator<Column> iterator = list.iterator(); iterator.hasNext();) {
		//			Column column = (Column) iterator.next();
		//
		//			String type = column.getShownDataType();
		//			sb.append(NEWLINE).append(" ").append(
		//					getDBQualifier(column.getName())).append(" ").append(type);
		//			String defaultv = column.getDefaultValue();
		//
		//			if (defaultv != null) {
		//				defaultv = CUBRIDFormator.formatValue(column.getDataType(),
		//						column.getSubDataType(), column.getScale(), defaultv).getFormatResult();
		//
		//				if (defaultv != null) {
		//					sb.append(" DEFAULT ").append(defaultv);
		//				}
		//			}
		//
		//			sb.append(",");
		//		}
		//
		//		if (!list.isEmpty() && sb.length() > 0) {
		//			sb.deleteCharAt(sb.length() - 1);
		//		}
		//
		//		sb.append(")").append(NEWLINE);
		sb.append("    AS ");
		List<String> queryListData = new ArrayList<String>();
		queryListData.add(view.getQuerySpec());

		for (int i = 0; i < queryListData.size(); i++) {
			String map = queryListData.get(i);
			sb.append(NEWLINE).append(map);

			if (i != queryListData.size() - 1) {
				sb.append(NEWLINE).append(" UNION ALL ");
			}
		}

		sb.append(END_LINE_CHAR);
		return sb.toString();
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

	/**
	 * append "rownum = 0" to SELECT statement
	 * 
	 * @param sql SELECT statement
	 * @return String
	 */
	public String getTestSelectSQL(String sql) {
		String cleanSql = sql.toUpperCase().trim();
		String realSQL = sql;
		Pattern groupbyPattern = Pattern.compile("GROUP\\s+BY", Pattern.MULTILINE
				| Pattern.CASE_INSENSITIVE);
		Pattern orderbyPattern = Pattern.compile("ORDER\\s+BY", Pattern.MULTILINE
				| Pattern.CASE_INSENSITIVE);
		realSQL = realSQL.replaceAll(LIMIT_PATTEN_1, "");
		realSQL = realSQL.replaceAll(LIMIT_PATTEN_2, "");
		Matcher groupbyMatcher = groupbyPattern.matcher(cleanSql);
		Matcher orderbyMatcher = orderbyPattern.matcher(cleanSql);
		StringBuilder buf = new StringBuilder(realSQL);
		if (groupbyMatcher.find()) {
			if (cleanSql.indexOf("HAVING") == -1) {
				buf.append(" HAVING ");
			} else {
				buf.append(" AND ");
			}
			buf.append(" GROUPBY_NUM() = 1 ");
		} else if (orderbyMatcher.find()) {
			buf.append(" FOR ORDERBY_NUM() = 1 ");
		} else if (cleanSql.indexOf("WHERE") != -1) {
			buf.append(" AND ROWNUM = 1");
		} else {
			buf.append(" WHERE ROWNUM = 1");
		}
		return buf.toString();
	}
}
