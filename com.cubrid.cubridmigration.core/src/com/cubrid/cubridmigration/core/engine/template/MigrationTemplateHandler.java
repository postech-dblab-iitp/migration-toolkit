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
package com.cubrid.cubridmigration.core.engine.template;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.PartitionTable;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.CSVSettings;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.mysql.trans.MySQL2CUBRIDMigParas;

/**
 * MigrationTemplateHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-9-13 created by Kevin Cao
 */
public final class MigrationTemplateHandler extends
		DefaultHandler {

	private final MigrationConfiguration config = new MigrationConfiguration();
	private boolean isSourceNode;
	private boolean isGraphTarget;
	
	private Vertex targetVertex;
	private Edge targetEdge;

	private SourceTableConfig srcTableCfg;
	private Table targetTable;
	private View targetView;
	private StringBuffer sqlStatement;
	private StringBuffer schemaCache;
	private Catalog srcCatalog;
	private Catalog srcSQLCatalog;

	private SourceCSVConfig srcCSV;

	private final CUBRIDDataTypeHelper dtHelper = CUBRIDDataTypeHelper.getInstance(null);

	public static final List<String> FK_OPERATION = new ArrayList<String>();

	static {
		FK_OPERATION.add("CASCADE");
		FK_OPERATION.add("RESTRICT");
		FK_OPERATION.add("SET NULL");
		FK_OPERATION.add("NO ACTION");
	}

	MigrationTemplateHandler() {
		//Do nothing
	}

	/**
	 * Receive notification of character data inside an element.
	 * 
	 * <p>
	 * By default, do nothing. Application writers may override this method to
	 * take specific actions for each chunk of character data (such as adding
	 * the data to a node or buffer, or printing it to a file).
	 * </p>
	 * 
	 * @param ch The characters.
	 * @param start The start position in the character array.
	 * @param length The number of characters to use from the character array.
	 * @exception org.xml.sax.SAXException Any SAX exception, possibly wrapping
	 *            another exception.
	 * @see org.xml.sax.ContentHandler#characters
	 */
	public void characters(char[] ch, int start, int length) throws SAXException {
		if (schemaCache != null) {
			schemaCache.append(ch, start, length);
			return;
		}
		if (sqlStatement == null) {
			return;
		}
		sqlStatement.append(ch, start, length);
	}

	/**
	 * Receive notification of the end of an element.
	 * 
	 * <p>
	 * By default, do nothing. Application writers may override this method in a
	 * subclass to take specific actions at the end of each element (such as
	 * finalising a tree node or writing output to a file).
	 * </p>
	 * 
	 * @param uri The Namespace URI, or the empty string if the element has no
	 *        Namespace URI or if Namespace processing is not being performed.
	 * @param localName The local name (without prefix), or the empty string if
	 *        Namespace processing is not being performed.
	 * @param qName The qualified name (with prefix), or the empty string if
	 *        qualified names are not available.
	 * @exception org.xml.sax.SAXException Any SAX exception, possibly wrapping
	 *            another exception.
	 * @see org.xml.sax.ContentHandler#endElement
	 */
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if (TemplateTags.TAG_SCHEMA.equals(qName)) {
			srcCatalog = Catalog.loadXML(schemaCache.toString());
			schemaCache = null;
		} else if (TemplateTags.TAG_SQL_SCHEMA.equals(qName)) {
			srcSQLCatalog = Catalog.loadXML(schemaCache.toString());
			schemaCache = null;
		} else if (TemplateTags.TAG_TABLE.equals(qName)) {
			srcTableCfg = null;
			targetTable = null;
		} else if (TemplateTags.TAG_SQLTABLE.equals(qName)) {
			srcTableCfg = null;
		} else if (TemplateTags.TAG_STATEMENT.equals(qName)) {
			((SourceSQLTableConfig) srcTableCfg).setSql(sqlStatement.toString().trim());
			sqlStatement = null;
		} else if (TemplateTags.TAG_VIEW.equals(qName)) {
			targetView = null;
		} else if (TemplateTags.TAG_VIEWQUERYSQL.equals(qName)) {
			targetView.setQuerySpec(sqlStatement.toString().trim());
			sqlStatement = null;
		} else if (TemplateTags.TAG_CREATEVIEWSQL.equals(qName)) {
			targetView.setDDL(sqlStatement.toString().trim());
			sqlStatement = null;
		} else if (TemplateTags.TAG_PARTITION_DDL.equals(qName)) {
			targetTable.getPartitionInfo().setDDL(sqlStatement.toString());
			sqlStatement = null;
		} else if (TemplateTags.TAG_CSV.equals(qName)) {
			config.addCSVFile(srcCSV);
		} else if (TemplateTags.TAG_MIGRATION.equals(qName)) {
			if (srcSQLCatalog != null && CollectionUtils.isNotEmpty(srcSQLCatalog.getSchemas())) {
				Schema sqlSchema = srcSQLCatalog.getSchemas().get(0);
				for (Table tt : sqlSchema.getTables()) {
					config.addExpSQLTableSchema(tt);
				}
			}
			if (srcCatalog != null) {
				ConnParameters sourceConParams = config.getSourceConParams();
				//Set copy of the source connection parameters
				srcCatalog.setConnectionParameters(sourceConParams == null ? null
						: sourceConParams.clone());
				config.setSrcCatalog(srcCatalog, false);
				config.setOfflineSrcCatalog(srcCatalog);
			}
		}
	}

	/**
	 * getBoolean of string
	 * 
	 * @param value String yes/no
	 * @param def default value
	 * @return boolean
	 */
	private boolean getBoolean(String value, boolean def) {
		if (value == null) {
			return def;
		}
		return TemplateTags.VALUE_YES.equals(value);
	}

	/**
	 * getFKOptIndex
	 * 
	 * @param opt String
	 * @return int
	 */
	private int getFKOptIndex(String opt) {
		int result = FK_OPERATION.indexOf(opt);
		return result < 0 ? 1 : result;
	}

	/**
	 * Get parsing result
	 * 
	 * @return MigrationConfiguration
	 */
	public MigrationConfiguration getResult() {
		return config;
	}

	/**
	 * Transform string to string list
	 * 
	 * @param strings with format "xxx,xxx,xx,xxx"
	 * @return List<String>
	 */
	private List<String> getStringList(String strings) {
		List<String> result = new ArrayList<String>();
		if (StringUtils.isEmpty(strings)) {
			return result;
		}
		String[] stringArray = strings.split(",");
		for (String str : stringArray) {
			result.add(str);
		}
		return result;
	}

	//	/**
	//	 * @param attributes of node
	//	 */
	//	private void parseTargetCMServer(Attributes attributes) {
	//		CMServerConfig cmServer = new CMServerConfig();
	//		cmServer.setHost(attributes.getValue(TemplateTags.ATTR_HOST));
	//		cmServer.setPort(Integer.parseInt(attributes.getValue(TemplateTags.ATTR_PORT)));
	//		cmServer.setUser(attributes.getValue(TemplateTags.ATTR_USER));
	//		cmServer.setPassword(attributes.getValue(TemplateTags.ATTR_PASSWORD));
	//		config.setCmServer(cmServer);
	//	}

	/**
	 * @param attributes of node
	 */
	private void parseTargetColumn(Attributes attributes) {
		Column column = new Column();
		column.setName(attributes.getValue(TemplateTags.ATTR_NAME));
		column.setNullable(getBoolean(attributes.getValue(TemplateTags.ATTR_NULL), true));
		column.setAutoIncrement(getBoolean(attributes.getValue(TemplateTags.ATTR_AUTO_INCREMENT),
				false));
		column.setUnique(getBoolean(attributes.getValue(TemplateTags.ATTR_UNIQUE), false));
		column.setShared(getBoolean(attributes.getValue(TemplateTags.ATTR_SHARED), false));
		if (column.isShared()) {
			column.setSharedValue(attributes.getValue(TemplateTags.ATTR_SHARED_VALUE));
		}
		if (column.isAutoIncrement()) {
			column.setAutoIncIncrVal(Integer.parseInt(attributes.getValue(TemplateTags.ATTR_INCREMENT)));
			column.setAutoIncSeedVal(Integer.parseInt(attributes.getValue(TemplateTags.ATTR_START)));
		}
		column.setDefaultValue(attributes.getValue(TemplateTags.ATTR_DEFAULT));
		column.setDefaultIsExpression(getBoolean(
				attributes.getValue(TemplateTags.ATTR_DEFAULT_EXPRESSION), false));
		final String type = attributes.getValue(TemplateTags.ATTR_TYPE);
		if (StringUtils.isEmpty(type)) {
			System.out.println(targetTable.getName() + ":" + column.getName());

		}
		dtHelper.setColumnDataType(type, column);
		targetTable.addColumn(column);
		//column.setUnique(unique);
	}

	/**
	 * 
	 * @param attributes of node
	 */
	private void parseTargetJDBC(Attributes attributes) {
		DatabaseType dbType = DatabaseType.CUBRID;
		
		if (isGraphTarget) {
			dbType = DatabaseType.GRAPH;
		}
		
		ConnParameters cp = ConnParameters.getConParam(null,
				attributes.getValue(TemplateTags.ATTR_HOST),
				Integer.parseInt(attributes.getValue(TemplateTags.ATTR_PORT)),
				attributes.getValue(TemplateTags.ATTR_NAME), 
				dbType,
				attributes.getValue(TemplateTags.ATTR_CHARSET),
				attributes.getValue(TemplateTags.ATTR_USER),
				attributes.getValue(TemplateTags.ATTR_PASSWORD),
				attributes.getValue(TemplateTags.ATTR_DRIVER),
				attributes.getValue(TemplateTags.ATTR_SCHEMA));
		cp.setUserJDBCURL(attributes.getValue(TemplateTags.ATTR_USER_JDBC_URL));
		cp.setTimeZone(attributes.getValue(TemplateTags.ATTR_TIMEZONE));

		config.setTargetConParams(cp);
		config.setCreateConstrainsBeforeData(getBoolean(
				attributes.getValue(TemplateTags.ATTR_CREATE_CONSTRAINT_NOW), false));
		config.setWriteErrorRecords(getBoolean(
				attributes.getValue(TemplateTags.ATTR_WRITE_ERROR_RECORDS), false));
	}

	/**
	 * @param attributes of node
	 */
	private void parseTargetFK(Attributes attributes) {
		FK fk = new FK(targetTable);
		targetTable.addFK(fk);
		fk.setName(attributes.getValue(TemplateTags.ATTR_NAME));
		fk.setDeleteRule(getFKOptIndex(attributes.getValue(TemplateTags.ATTR_ON_DELETE)));
		fk.setUpdateRule(getFKOptIndex(attributes.getValue(TemplateTags.ATTR_ON_UPDATE)));
		fk.setReferencedTableName(attributes.getValue(TemplateTags.ATTR_REF_TABLE));
		final List<String> cols = getStringList(attributes.getValue(TemplateTags.ATTR_FIELDS));
		final List<String> refCols = getStringList(attributes.getValue(TemplateTags.ATTR_REF_FIELDS));
		if (CollectionUtils.isNotEmpty(cols) && CollectionUtils.isNotEmpty(refCols)
				&& cols.size() == refCols.size()) {
			for (int i = 0; i < cols.size(); i++) {
				fk.addRefColumnName(cols.get(i), refCols.get(i));
			}
		} else {
			targetTable.removeFK(fk.getName());
		}
		//fk.setOnCacheObject(attributes.getValue(TemplateTags.ATTR_ON_CACHE_OBJECT));
	}

	/**
	 * @param attributes of node
	 */
	private void parseTargetHashPartition(Attributes attributes) {
		PartitionTable pt = new PartitionTable();
		pt.setPartitionName(attributes.getValue(TemplateTags.ATTR_NAME));
		targetTable.getPartitionInfo().addPartition(pt);
	}

	/**
	 * @param attributes of node
	 */
	private void parseTargetIndex(Attributes attributes) {
		Index index = new Index(targetTable);
		index.setName(attributes.getValue(TemplateTags.ATTR_NAME));

		List<String> rules = getStringList(attributes.getValue(TemplateTags.ATTR_ORDER_RULE));
		List<String> columns = getStringList(attributes.getValue(TemplateTags.ATTR_FIELDS));
		if (CollectionUtils.isEmpty(rules) || CollectionUtils.isEmpty(columns)
				|| rules.size() != columns.size()) {
			return;
		}
		for (int i = 0; i < columns.size(); i++) {
			index.addColumn(columns.get(i), rules.get(i).startsWith("A"));
		}

		//index.setMigrationPrefix(attributes.getValue(TemplateTags.ATTR_PRE_FIX));
		index.setUnique(getBoolean(attributes.getValue(TemplateTags.ATTR_UNIQUE), false));
		index.setReverse(getBoolean(attributes.getValue(TemplateTags.ATTR_REVERSE), false));

		// If a XE template has a unique index, it need to check whether reverse unique or normal unique.
		//		if (index.isReverse()) {
		//			if (index.isUnique()) {
		//				index.setIndexType(Index.TYPE_REVERSE_UNIQUE);
		//			} else {
		//				index.setIndexType(Index.TYPE_REVERSE_NORMAL);
		//			}
		//		} else if (index.isUnique()) {
		//			index.setIndexType(Index.TYPE_UNIQUE);
		//		} else {
		//			index.setIndexType(Index.TYPE_NORMAL);
		//		}
		targetTable.addIndex(index);
	}

	/**
	 * 
	 * @param attributes of node
	 */
	private void parseTargetPartition(Attributes attributes) {
		PartitionInfo partition = new PartitionInfo();
		targetTable.setPartitionInfo(partition);
		partition.setPartitionExp(attributes.getValue(TemplateTags.ATTR_EXPRESSION));
		partition.setPartitionMethod(attributes.getValue(TemplateTags.ATTR_TYPE));

	}

	/**
	 * @param attributes of node
	 */
	private void parseTargetPK(Attributes attributes) {
		PK pk = new PK(targetTable);
		targetTable.setPk(pk);
		pk.setPkColumns(getStringList(attributes.getValue(TemplateTags.ATTR_FIELDS)));

	}

	/**
	 * 
	 * @param attributes of node
	 */
	private void parseTargetRangePartition(Attributes attributes) {
		PartitionTable pt = new PartitionTable();
		pt.setPartitionName(attributes.getValue(TemplateTags.ATTR_NAME));
		pt.setPartitionDesc(attributes.getValue(TemplateTags.ATTR_VALUE));
		targetTable.getPartitionInfo().addPartition(pt);
	}

	/**
	 * @param attributes of node
	 */
	private void parseTargetSequence(Attributes attributes) {
		Sequence seq = new Sequence();
		seq.setName(attributes.getValue(TemplateTags.ATTR_NAME));
		seq.setIncrementBy(new BigInteger(attributes.getValue(TemplateTags.ATTR_INCREMENT)));
		seq.setCurrentValue(new BigInteger(attributes.getValue(TemplateTags.ATTR_START)));
		seq.setCycleFlag(getBoolean(attributes.getValue(TemplateTags.ATTR_CYCLE), false));
		seq.setNoCache(!getBoolean(attributes.getValue(TemplateTags.ATTR_CACHE), true));
		if (!seq.isNoCache()) {
			final String cs = attributes.getValue(TemplateTags.ATTR_CACHE_SIZE);
			seq.setCacheSize(cs == null ? 2 : Integer.parseInt(cs));
		}
		seq.setNoMaxValue(getBoolean(attributes.getValue(TemplateTags.ATTR_NO_MAX), true));
		if (!seq.isNoMaxValue()) {
			seq.setMaxValue(new BigInteger(attributes.getValue(TemplateTags.ATTR_MAX)));
		}
		seq.setNoMinValue(getBoolean(attributes.getValue(TemplateTags.ATTR_NO_MIN), true));
		if (!seq.isNoMinValue()) {
			seq.setMinValue(new BigInteger(attributes.getValue(TemplateTags.ATTR_MIN)));
		}
		config.addTargetSerialSchema(seq);
	}

	/**
	 * @param attributes of node
	 */
	private void parseTargetTable(Attributes attributes) {
		targetTable = new Table();
		targetTable.setName(attributes.getValue(TemplateTags.ATTR_NAME));
		targetTable.setReuseOID(getBoolean(attributes.getValue(TemplateTags.ATTR_REUSE_OID), false));
		config.addTargetTableSchema(targetTable);
	}

	/**
	 * parse Target View
	 * 
	 * @param attributes Attributes
	 */
	private void parseTargetView(Attributes attributes) {
		targetView = new View();
		config.addTargetViewSchema(targetView);
		targetView.setName(attributes.getValue(TemplateTags.ATTR_NAME));
	}

	/**
	 * parse Target View Column
	 * 
	 * @param attributes Attributes
	 */
	private void parseTargetViewColumn(Attributes attributes) {
		Column column = new Column();
		targetView.addColumn(column);
		column.setTableOrView(targetView);
		column.setName(attributes.getValue(TemplateTags.ATTR_NAME));
		dtHelper.setColumnDataType(attributes.getValue(TemplateTags.ATTR_TYPE), column);
	}

	/**
	 * Receive notification of the start of an element.
	 * 
	 * <p>
	 * By default, do nothing. Application writers may override this method in a
	 * subclass to take specific actions at the start of each element (such as
	 * allocating a new tree node or writing output to a file).
	 * </p>
	 * 
	 * @param uri The Namespace URI, or the empty string if the element has no
	 *        Namespace URI or if Namespace processing is not being performed.
	 * @param localName The local name (without prefix), or the empty string if
	 *        Namespace processing is not being performed.
	 * @param qName The qualified name (with prefix), or the empty string if
	 *        qualified names are not available.
	 * @param attributes The attributes attached to the element. If there are no
	 *        attributes, it shall be an empty Attributes object.
	 * @exception org.xml.sax.SAXException Any SAX exception, possibly wrapping
	 *            another exception.
	 * @see org.xml.sax.ContentHandler#startElement
	 */
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		if (TemplateTags.TAG_MIGRATION.equals(qName)) {
			config.setName(attributes.getValue(TemplateTags.ATTR_NAME));
		} else if (TemplateTags.TAG_SOURCE.equals(qName)) {
			isSourceNode = true;
			config.setSourceType(attributes.getValue(TemplateTags.ATTR_DB_TYPE));
		} else if (TemplateTags.TAG_TARGET.equals(qName)) {
			
			isGraphTarget = (attributes.getValue(TemplateTags.ATTR_DB_TYPE)).equals("graph");
			
			isSourceNode = false;
			config.setTargetDBVersion(attributes.getValue(TemplateTags.ATTR_VERSION));
			String type = attributes.getValue(TemplateTags.ATTR_TYPE);
			config.setDestType(MigrationConfiguration.DEST_GRAPH);
			if (TemplateTags.VALUE_ONLINE.equalsIgnoreCase(type) && !(isGraphTarget)) {
				config.setDestType(MigrationConfiguration.DEST_ONLINE);
			} else if (TemplateTags.VALUE_DIR.equalsIgnoreCase(type)) {
				config.setDestType(MigrationConfiguration.DEST_DB_UNLOAD);
			}
//			
//			if (TemplateTags.VALUE_DIR.equalsIgnoreCase(type)) {
//				config.setDestType(MigrationConfiguration.DEST_DB_UNLOAD);
//			}
//			
			//			else if (TemplateTags.VALUE_OFFLINE.equalsIgnoreCase(type)) {
			//				config.setDestType(MigrationConfiguration.DEST_OFFLINE);
			//			} 
		} else if (TemplateTags.TAG_PARAMS.equals(qName)) {
			config.setExportThreadCount(Integer.parseInt(attributes.getValue(TemplateTags.ATTR_EXPORT_THREAD)));
			String attrImportThread = attributes.getValue(TemplateTags.ATTR_IMPORT_THREAD);
			attrImportThread = attrImportThread == null ? ("" + config.getExportThreadCount())
					: attrImportThread;
			config.setImportThreadCount(Integer.parseInt(attrImportThread));
			config.setCommitCount(Integer.parseInt(attributes.getValue(TemplateTags.ATTR_COMMIT_COUNT)));
			final String fetchCount = attributes.getValue(TemplateTags.ATTR_PAGE_FETCH_COUNT);
			config.setPageFetchCount(fetchCount == null ? 1000 : Integer.parseInt(fetchCount));
			config.setImplicitEstimate(getBoolean(
					attributes.getValue(TemplateTags.ATTR_IMPLICIT_ESTIMATE_PROGRESS), false));
			config.setUpdateStatistics(getBoolean(
					attributes.getValue(TemplateTags.ATTR_UPDATE_STATISTICS), true));
			String s1 = attributes.getValue(MySQL2CUBRIDMigParas.UNPARSED_TIME);
			if (s1 != null) {
				config.putOtherParam(MySQL2CUBRIDMigParas.UNPARSED_TIME, s1);
			}
			String s2 = attributes.getValue(MySQL2CUBRIDMigParas.UNPARSED_DATE);
			if (s2 != null) {
				config.putOtherParam(MySQL2CUBRIDMigParas.UNPARSED_DATE, s2);
			}
			String s3 = attributes.getValue(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP);
			if (s3 != null) {
				config.putOtherParam(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP, s3);
			}
			String s4 = attributes.getValue(MySQL2CUBRIDMigParas.REPLAXE_CHAR0);
			if (s4 != null) {
				config.putOtherParam(MySQL2CUBRIDMigParas.REPLAXE_CHAR0, s4);
			}

		} else if (isSourceNode) {
			startSourceElement(qName, attributes);
		} else {
			startTargetElement(qName, attributes);
		}
	}

	/**
	 * Start parse source element
	 * 
	 * @param qName String
	 * @param attributes Attributes
	 * @throws SAXException when errors
	 */
	private void startSourceElement(String qName, Attributes attributes) throws SAXException {
		if (TemplateTags.TAG_JDBC.equals(qName)) {
			ConnParameters scp = ConnParameters.getConParam(null,
					attributes.getValue(TemplateTags.ATTR_HOST),
					Integer.parseInt(attributes.getValue(TemplateTags.ATTR_PORT)),
					attributes.getValue(TemplateTags.ATTR_NAME), config.getSourceDBType(),
					attributes.getValue(TemplateTags.ATTR_CHARSET),
					attributes.getValue(TemplateTags.ATTR_USER),
					attributes.getValue(TemplateTags.ATTR_PASSWORD),
					attributes.getValue(TemplateTags.ATTR_DRIVER),
					attributes.getValue(TemplateTags.ATTR_SCHEMA));
			scp.setUserJDBCURL(attributes.getValue(TemplateTags.ATTR_USER_JDBC_URL));
			scp.setTimeZone(attributes.getValue(TemplateTags.ATTR_TIMEZONE));
			config.setSourceConParams(scp);
		} else if (TemplateTags.TAG_SCHEMA.equals(qName)) {
			schemaCache = new StringBuffer();
		} else if (TemplateTags.TAG_SQL_SCHEMA.equals(qName)) {
			schemaCache = new StringBuffer();
		} else if (TemplateTags.TAG_FILE.equals(qName)) {
			config.setSourceFileName(attributes.getValue(TemplateTags.ATTR_LOCATION));
			config.setSourceFileEncoding(attributes.getValue(TemplateTags.ATTR_CHARSET));
			config.setSourceFileTimeZone(attributes.getValue(TemplateTags.ATTR_TIMEZONE));
			config.setSourceFileVersion(attributes.getValue(TemplateTags.ATTR_VERSION));
		} else if (TemplateTags.TAG_TABLE.equals(qName)) {
			srcTableCfg = new SourceEntryTableConfig();
			srcTableCfg.setCreateNewTable(getBoolean(attributes.getValue(TemplateTags.ATTR_CREATE),
					true));
			srcTableCfg.setMigrateData(getBoolean(
					attributes.getValue(TemplateTags.ATTR_MIGRATE_DATA), true));
			srcTableCfg.setName(attributes.getValue(TemplateTags.ATTR_NAME));
			srcTableCfg.setReplace(getBoolean(attributes.getValue(TemplateTags.ATTR_REPLACE), false));
			srcTableCfg.setTarget(attributes.getValue(TemplateTags.ATTR_TARGET));
			srcTableCfg.setSqlBefore(attributes.getValue(TemplateTags.ATTR_BEFORE_SQL));
			srcTableCfg.setSqlAfter(attributes.getValue(TemplateTags.ATTR_AFTER_SQL));
			srcTableCfg.setOwner(attributes.getValue(TemplateTags.ATTR_OWNER));

			SourceEntryTableConfig setc = ((SourceEntryTableConfig) srcTableCfg);
			setc.setCreatePartition(getBoolean(attributes.getValue(TemplateTags.ATTR_PARTITION),
					false));
			setc.setCreatePK(getBoolean(attributes.getValue(TemplateTags.ATTR_PK), true));
			setc.setCondition(attributes.getValue(TemplateTags.ATTR_CONDITION));
			setc.setEnableExpOpt(getBoolean(attributes.getValue(TemplateTags.ATTR_EXP_OPT_COL),
					true));
			setc.setStartFromTargetMax(getBoolean(
					attributes.getValue(TemplateTags.ATTR_START_TAR_MAX), false));
			config.addExpEntryTableCfg(setc);

		} else if (TemplateTags.TAG_COLUMN.equals(qName)) {
			String colName = attributes.getValue(TemplateTags.ATTR_NAME);
			srcTableCfg.addColumnConfig(colName, attributes.getValue(TemplateTags.ATTR_TARGET),
					true);
			SourceColumnConfig scc = srcTableCfg.getColumnConfig(colName);
			scc.setNeedTrim(getBoolean(attributes.getValue(TemplateTags.ATTR_TRIM), false));
			scc.setReplaceExpression(attributes.getValue(TemplateTags.ATTR_REPLACE_EXPRESSION));
			scc.setUserDataHandler(attributes.getValue(TemplateTags.ATTR_USER_DATA_HANDLER));
		} else if (TemplateTags.TAG_FK.equals(qName)) {
			((SourceEntryTableConfig) srcTableCfg).addFKConfig(
					attributes.getValue(TemplateTags.ATTR_NAME),
					attributes.getValue(TemplateTags.ATTR_TARGET), true);
		} else if (TemplateTags.TAG_INDEX.equals(qName)) {
			((SourceEntryTableConfig) srcTableCfg).addIndexConfig(
					attributes.getValue(TemplateTags.ATTR_NAME),
					attributes.getValue(TemplateTags.ATTR_TARGET), true);
		} else if (TemplateTags.TAG_SQLTABLE.equals(qName)) {
			srcTableCfg = new SourceSQLTableConfig();
			srcTableCfg.setName(attributes.getValue(TemplateTags.ATTR_NAME));
			srcTableCfg.setCreateNewTable(getBoolean(attributes.getValue(TemplateTags.ATTR_CREATE),
					true));
			srcTableCfg.setMigrateData(getBoolean(
					attributes.getValue(TemplateTags.ATTR_MIGRATE_DATA), true));
			srcTableCfg.setReplace(getBoolean(attributes.getValue(TemplateTags.ATTR_REPLACE), false));
			srcTableCfg.setTarget(attributes.getValue(TemplateTags.ATTR_TARGET));
			config.addExpSQLTableCfg((SourceSQLTableConfig) srcTableCfg);
		} else if (TemplateTags.TAG_STATEMENT.equals(qName)) {
			sqlStatement = new StringBuffer();
		} else if (TemplateTags.TAG_SEQUENCE.equals(qName)) {
			SourceSequenceConfig ssc = config.addExpSerialCfg(
					attributes.getValue(TemplateTags.ATTR_OWNER),
					attributes.getValue(TemplateTags.ATTR_NAME),
					attributes.getValue(TemplateTags.ATTR_TARGET));
			ssc.setAutoSynchronizeStartValue(getBoolean(
					attributes.getValue(TemplateTags.ATTR_AUTO_SYNCHRONIZE_START_VALUE), true));
		} else if (TemplateTags.TAG_VIEW.equals(qName)) {
			config.addExpViewCfg(attributes.getValue(TemplateTags.ATTR_OWNER),
					attributes.getValue(TemplateTags.ATTR_NAME),
					attributes.getValue(TemplateTags.ATTR_TARGET));
		} else if (TemplateTags.TAG_TRIGGER.equals(qName)) {
			config.addExpTriggerCfg(attributes.getValue(TemplateTags.ATTR_NAME));
		} else if (TemplateTags.TAG_FUNCTION.equals(qName)) {
			config.addExpFunctionCfg(attributes.getValue(TemplateTags.ATTR_NAME));
		} else if (TemplateTags.TAG_PROCEDURE.equals(qName)) {
			config.addExpProcedureCfg(attributes.getValue(TemplateTags.ATTR_NAME));
		} else if (TemplateTags.TAG_SQL.equals(qName)) {
			config.setSourceFileEncoding(attributes.getValue(TemplateTags.ATTR_CHARSET));
		} else if (TemplateTags.TAG_SQL_FILE.equals(qName)) {
			config.addSQLFile(attributes.getValue(TemplateTags.ATTR_LOCATION));
		} else if (TemplateTags.TAG_CSVS.equals(qName)) {
			String cs = attributes.getValue(TemplateTags.ATTR_CSV_SEPARATE);
			final CSVSettings csvSettings = config.getCsvSettings();
			if (cs == null) {
				csvSettings.setSeparateChar(csvSettings.getSeparateChar());
			} else if (cs.length() > 0) {
				csvSettings.setSeparateChar(cs.charAt(0));
			} else {
				csvSettings.setSeparateChar(MigrationConfiguration.CSV_NO_CHAR);
			}

			cs = attributes.getValue(TemplateTags.ATTR_CSV_QUOTE);
			if (cs == null) {
				csvSettings.setQuoteChar(csvSettings.getQuoteChar());
			} else if (cs.length() > 0) {
				csvSettings.setQuoteChar(cs.charAt(0));
			} else {
				csvSettings.setQuoteChar(MigrationConfiguration.CSV_NO_CHAR);
			}

			cs = attributes.getValue(TemplateTags.ATTR_CSV_ESCAPE);
			if (cs == null) {
				csvSettings.setEscapeChar(csvSettings.getEscapeChar());
			} else if (cs.length() > 0) {
				csvSettings.setEscapeChar(cs.charAt(0));
			} else {
				csvSettings.setEscapeChar(MigrationConfiguration.CSV_NO_CHAR);
			}

			cs = attributes.getValue(TemplateTags.ATTR_CSV_NULL_VALUE);
			if (cs != null) {
				csvSettings.setNullStrings(cs);
			}
			cs = attributes.getValue(TemplateTags.ATTR_CHARSET);
			if (cs != null) {
				csvSettings.setCharset(cs);
			}
		} else if (TemplateTags.TAG_CSV.equals(qName)) {
			srcCSV = new SourceCSVConfig();
			srcCSV.setCreate(getBoolean(attributes.getValue(TemplateTags.ATTR_CREATE), false));
			srcCSV.setReplace(getBoolean(attributes.getValue(TemplateTags.ATTR_REPLACE), false));
			srcCSV.setImportFirstRow(getBoolean(
					attributes.getValue(TemplateTags.ATTR_IMPORT_FIRST_ROW), false));
			srcCSV.setName(attributes.getValue(TemplateTags.ATTR_NAME));
			srcCSV.setTarget(attributes.getValue(TemplateTags.ATTR_TARGET));
		} else if (TemplateTags.TAG_CSV_COLUMN.equals(qName)) {
			SourceCSVColumnConfig sccc = new SourceCSVColumnConfig();
			sccc.setCreate(getBoolean(attributes.getValue(TemplateTags.ATTR_CREATE), true));
			sccc.setReplace(false);
			sccc.setName(attributes.getValue(TemplateTags.ATTR_NAME));
			sccc.setTarget(attributes.getValue(TemplateTags.ATTR_TARGET));
			srcCSV.addColumn(sccc);
		}
	}
	
	private void parseVertexInfo(Attributes attr) {
		Vertex vertex = new Vertex();
		
		vertex.setVertexLabel(attr.getValue(TemplateTags.ATTR_LABEL));
		vertex.setVertexType(Integer.parseInt(attr.getValue("vertex_type")));
		vertex.setTableName(attr.getValue("table_name"));
		vertex.setOwner(attr.getValue("table_owner"));
		vertex.putVertexProperties(attr.getValue(TemplateTags.ATTR_NAME), attr.getValue(TemplateTags.ATTR_TYPE));
		
		targetVertex = vertex;
		
		config.getGraphDictionary().addMigratedVertexList(vertex);
	}
	
	private void parseEdgeInfo(Attributes attr) {
		Edge edge = new Edge();
		
		edge.setEdgeLabel(attr.getValue(TemplateTags.ATTR_LABEL));
		edge.setEdgeType(Integer.parseInt(attr.getValue("edge_type")));
		edge.putVertexProperties(attr.getValue(TemplateTags.ATTR_NAME), attr.getValue(TemplateTags.ATTR_TYPE));
		
		edge.setStartVertexName(attr.getValue(TemplateTags.ATTR_START_VERTEX));
		edge.setEndVertexName(attr.getValue(TemplateTags.ATTR_END_VERTEX));
		
		targetEdge = edge;
		
		config.getGraphDictionary().addMigratedEdgeList(edge);
	}
	
	private void parsePropertyInfo(Attributes attr) {
		Column col = new Column();
		col.setName(attr.getValue("name"));
		col.setDataType(attr.getValue("type"));
		col.setSupportGraphDataType(Boolean.valueOf(attr.getValue("datatype_support")));
		
		targetVertex.addColumn(col);
	}
	
	private void parseEdgePropertyInfo(Attributes attr) {
		Column col = new Column();
		col.setName(attr.getValue("name"));
		col.setDataType(attr.getValue("type"));
		col.setGraphDataType(attr.getValue("graph_type"));
		
		String referCol = attr.getValue("reference_to");
		
		if (referCol != null && !referCol.isEmpty()) {
			targetEdge.addFKCol2Ref(attr.getValue("name"), attr.getValue("reference_to"));			
		}
		
		
		targetEdge.addColumn(col);
	}
	
	private void parseEdgeFKInfo(Attributes attr) {
		String colName = attr.getValue("column_name");
		String refColName = attr.getValue("ref_column_name");
		
		targetEdge.addFKCol2Ref(colName, refColName);
	}

	/**
	 * Start parse the target element
	 * 
	 * @param qName String
	 * @param attr Attributes
	 * @throws SAXException when errors
	 */
	private void startTargetElement(String qName, Attributes attr) throws SAXException {
		if (isGraphTarget) {
			if (TemplateTags.TAG_VERTEX.equals(qName)) {
				parseVertexInfo(attr);				
			} else if (TemplateTags.TAG_EDGE.equals(qName)) {
				parseEdgeInfo(attr);
			} else if (TemplateTags.TAG_JDBC.equals(qName)) {
				parseTargetJDBC(attr);
			} else if (TemplateTags.TAG_PROPERTY.equals(qName)) {
				parsePropertyInfo(attr);
			} else if ("edge_property".equals(qName)) {
				parseEdgePropertyInfo(attr);
			} else if ("fk".equals(qName)) {
				parseEdgeFKInfo(attr);
			} else if (TemplateTags.TAG_FILE_REPOSITORY.equals(qName)) {
				config.setFileRepositroyPath(attr.getValue(TemplateTags.ATTR_DIR));
				config.setTargetSchemaFileName(attr.getValue(TemplateTags.ATTR_SCHEMA));
				config.setTargetDataFileName(attr.getValue(TemplateTags.ATTR_DATA));
				config.setTargetIndexFileName(attr.getValue(TemplateTags.ATTR_INDEX));
				config.setTargetFileTimeZone(attr.getValue(TemplateTags.ATTR_TIMEZONE));
				config.setOneTableOneFile(getBoolean(attr.getValue(TemplateTags.ATTR_ONETABLEONEFILE),
						false));
				final String fileMaxSize = attr.getValue(TemplateTags.ATTR_FILE_MAX_SIZE);
				config.setMaxCountPerFile(fileMaxSize == null ? 0 : Integer.parseInt(fileMaxSize));
				config.setTargetFilePrefix(attr.getValue(TemplateTags.ATTR_OUTPUT_FILE_PREFIX));
				try {
					config.setDestType(Integer.parseInt(attr.getValue(TemplateTags.ATTR_DATA_FILE_FORMAT)));
				} catch (Exception ex) {
					config.setDestType(MigrationConfiguration.DEST_DB_UNLOAD);
				}
				config.setTargetCharSet(attr.getValue(TemplateTags.ATTR_CHARSET));
				if (config.targetIsCSV()) {
					String value = attr.getValue(TemplateTags.ATTR_CSV_SEPARATE);
					config.getCsvSettings().setSeparateChar(
							StringUtils.isEmpty(value) ? ',' : value.charAt(0));
					value = attr.getValue(TemplateTags.ATTR_CSV_QUOTE);
					config.getCsvSettings().setQuoteChar(
							StringUtils.isEmpty(value) ? MigrationConfiguration.CSV_NO_CHAR
									: value.charAt(0));
					value = attr.getValue(TemplateTags.ATTR_CSV_ESCAPE);
					config.getCsvSettings().setEscapeChar(
							StringUtils.isEmpty(value) ? MigrationConfiguration.CSV_NO_CHAR
									: value.charAt(0));
				}
				config.setTargetLOBRootPath(attr.getValue(TemplateTags.ATTR_LOB_ROOT_DIR));
			}
		} else {
			if (TemplateTags.TAG_TABLE.equals(qName)) {
				parseTargetTable(attr);
			} else if (TemplateTags.TAG_COLUMN.equals(qName)) {
				parseTargetColumn(attr);
			} else if (TemplateTags.TAG_FK.equals(qName)) {
				parseTargetFK(attr);
			} else if (TemplateTags.TAG_INDEX.equals(qName)) {
				parseTargetIndex(attr);
			} else if (TemplateTags.TAG_SEQUENCE.equals(qName)) {
				parseTargetSequence(attr);
			} else if (TemplateTags.TAG_VIEW.equals(qName)) {
				parseTargetView(attr);
			} else if (TemplateTags.TAG_VIEWCOLUMN.equals(qName)) {
				parseTargetViewColumn(attr);
			} else if (TemplateTags.TAG_VIEWQUERYSQL.equals(qName)) {
				sqlStatement = new StringBuffer();
			} else if (TemplateTags.TAG_CREATEVIEWSQL.equals(qName)) {
				sqlStatement = new StringBuffer();
			} else if (TemplateTags.TAG_PK.equals(qName)) {
				parseTargetPK(attr);
			} else if (TemplateTags.TAG_PARTITIONS.equals(qName)) {
				parseTargetPartition(attr);
			} else if (TemplateTags.TAG_RANGE.equals(qName)) {
				parseTargetRangePartition(attr);
			} else if (TemplateTags.TAG_HASH.equals(qName)) {
				parseTargetHashPartition(attr);
			} else if (TemplateTags.TAG_LIST.equals(qName)) {
				parseTargetRangePartition(attr);
			} else if (TemplateTags.TAG_JDBC.equals(qName)) {
				parseTargetJDBC(attr);
			} else if (TemplateTags.TAG_FILE_REPOSITORY.equals(qName)) {
				config.setFileRepositroyPath(attr.getValue(TemplateTags.ATTR_DIR));
				config.setTargetSchemaFileName(attr.getValue(TemplateTags.ATTR_SCHEMA));
				config.setTargetDataFileName(attr.getValue(TemplateTags.ATTR_DATA));
				config.setTargetIndexFileName(attr.getValue(TemplateTags.ATTR_INDEX));
				config.setTargetFileTimeZone(attr.getValue(TemplateTags.ATTR_TIMEZONE));
				config.setOneTableOneFile(getBoolean(attr.getValue(TemplateTags.ATTR_ONETABLEONEFILE),
						false));
				final String fileMaxSize = attr.getValue(TemplateTags.ATTR_FILE_MAX_SIZE);
				config.setMaxCountPerFile(fileMaxSize == null ? 0 : Integer.parseInt(fileMaxSize));
				config.setTargetFilePrefix(attr.getValue(TemplateTags.ATTR_OUTPUT_FILE_PREFIX));
				try {
					config.setDestType(Integer.parseInt(attr.getValue(TemplateTags.ATTR_DATA_FILE_FORMAT)));
				} catch (Exception ex) {
					config.setDestType(MigrationConfiguration.DEST_DB_UNLOAD);
				}
				config.setTargetCharSet(attr.getValue(TemplateTags.ATTR_CHARSET));
				if (config.targetIsCSV()) {
					String value = attr.getValue(TemplateTags.ATTR_CSV_SEPARATE);
					config.getCsvSettings().setSeparateChar(
							StringUtils.isEmpty(value) ? ',' : value.charAt(0));
					value = attr.getValue(TemplateTags.ATTR_CSV_QUOTE);
					config.getCsvSettings().setQuoteChar(
							StringUtils.isEmpty(value) ? MigrationConfiguration.CSV_NO_CHAR
									: value.charAt(0));
					value = attr.getValue(TemplateTags.ATTR_CSV_ESCAPE);
					config.getCsvSettings().setEscapeChar(
							StringUtils.isEmpty(value) ? MigrationConfiguration.CSV_NO_CHAR
									: value.charAt(0));
				}
				config.setTargetLOBRootPath(attr.getValue(TemplateTags.ATTR_LOB_ROOT_DIR));
			} else if (TemplateTags.TAG_PARTITION_DDL.equals(qName)) {
				sqlStatement = new StringBuffer();
			}
		}
		
		

		//		else {
		//			parseCMServer(qName, attr);
		//		}
	}

	//	/**
	//	 * Parse CM server phase.
	//	 * 
	//	 * @param qName tag name
	//	 * @param attributes of tags
	//	 */
	//	private void parseCMServer(String qName, Attributes attributes) {
	//		if (TemplateTags.TAG_CMSERVER.equals(qName)) {
	//			parseTargetCMServer(attributes);
	//		} else if (TemplateTags.TAG_EXISTDB.equals(qName)) {
	//			BaseDatabaseConfig bdc = new BaseDatabaseConfig();
	//			bdc.setName(attributes.getValue(TemplateTags.ATTR_NAME));
	//			bdc.setUser(attributes.getValue(TemplateTags.ATTR_USER));
	//			bdc.setPassword(attributes.getValue(TemplateTags.ATTR_PASSWORD));
	//			bdc.setCharset(attributes.getValue(TemplateTags.ATTR_CHARSET));
	//			bdc.setTimezone(attributes.getValue(TemplateTags.ATTR_TIMEZONE));
	//			config.setOfflineTargetDBInfo(bdc);
	//		} else if (TemplateTags.TAG_LOADDBSETTING.equals(qName)) {
	//			config.setLoadOnly(getBoolean(
	//					attributes.getValue(TemplateTags.ATTR_LOAD_ONLY), true));
	//			config.setNoOid(getBoolean(
	//					attributes.getValue(TemplateTags.ATTR_NO_OID), true));
	//			config.setNoLogging(getBoolean(
	//					attributes.getValue(TemplateTags.ATTR_NO_LOGGING), true));
	//			config.setNoStatics(getBoolean(
	//					attributes.getValue(TemplateTags.ATTR_NO_STATISTICS), true));
	//			config.setOptimizedb(getBoolean(
	//					attributes.getValue(TemplateTags.ATTR_OPTIMIZE_DB), true));
	//		}
	//	}
}
