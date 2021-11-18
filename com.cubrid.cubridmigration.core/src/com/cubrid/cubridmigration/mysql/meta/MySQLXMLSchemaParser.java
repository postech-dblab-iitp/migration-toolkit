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
package com.cubrid.cubridmigration.mysql.meta;

import java.sql.DatabaseMetaData;
import java.util.Locale;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.cubrid.cubridmigration.core.dbmetadata.AbstractJDBCSchemaFetcher;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObjectFactory;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.mysql.MySQLDataTypeHelper;

/**
 * get database meta data from Mysql xml file
 * 
 * @author moulinwang
 * 
 */
public class MySQLXMLSchemaParser extends
		DefaultHandler {
	private Catalog catalog;

	private Schema schema;
	private Table table;
	private boolean continueParseMeta;
	private int tableRowCount = 0;
	private final MySQLDataTypeHelper dtHelper = MySQLDataTypeHelper.getInstance(null);

	private final DBObjectFactory factory = new DBObjectFactory();

	public Catalog getCatalog() {
		return catalog;
	}

	/**
	 * start document
	 * 
	 * @throws SAXException e
	 */
	public void startDocument() throws SAXException {
		catalog = factory.createCatalog();
		catalog.setDatabaseType(DatabaseType.MYSQL);
		schema = factory.createSchema();
		schema.setCatalog(catalog);
		super.startDocument();
	}

	/**
	 * deal with when element ended
	 * 
	 * @param uri String
	 * @param localName String
	 * @param qName String
	 * @throws SAXException e
	 */
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if ("table_data".equals(qName)) {
			table.setTableRowCount(tableRowCount);
			tableRowCount = 0;
		} else if ("row".equals(qName)) {
			tableRowCount++;
		}
	}

	/**
	 * deal with when element started
	 * 
	 * @param uri String
	 * @param localName String
	 * @param qName String
	 * @param attributes Attributes
	 * @throws SAXException e
	 */
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		if ("mysqldump".equals(qName)) {
			catalog.setDatabaseType(DatabaseType.MYSQL);
		} else if ("database".equals(qName)) {
			String databaseName = attributes.getValue("name");
			catalog.setName(databaseName);
			schema.setName(databaseName);
			catalog.addSchema(schema);
		} else if ("table_structure".equals(qName)) {
			String tableName = attributes.getValue("name");
			table = factory.createTable();
			table.setName(tableName);
			schema.addTable(table);
			table.setSchema(schema);
			continueParseMeta = true;
		} else if ("table_data".equals(qName)) {
			continueParseMeta = false;
			return;
		} else if ("options".equals(qName)) {
			String rows = attributes.getValue("Rows");
			if (null != rows) {
				table.setTableRowCount(Long.parseLong(rows));
			}

			String comment = attributes.getValue("Comment");

			if ("VIEW".equals(comment)) {
				View view = factory.createView();
				view.setSchema(schema);

				view.setName(table.getName());
				view.setColumns(table.getColumns());
				//				schema.addView(view); 
				schema.getTables().remove(table);
			}

		} else if ("field".equals(qName)) {
			if (!continueParseMeta) {
				return;
			}
			Column column = factory.createColumn();
			column.setTableOrView(table);
			table.addColumn(column);
			String columnName = attributes.getValue("Field");
			column.setName(columnName);
			String columnDataType = attributes.getValue("Type");

			column.setShownDataType(columnDataType);
			column.setDataType(dtHelper.parseMainType(columnDataType));
			column.setCharLength(dtHelper.parsePrecision(columnDataType));
			column.setPrecision(dtHelper.parsePrecision(columnDataType));
			column.setScale(dtHelper.parseScale(columnDataType));

			// make sure precision is greater than scale
			if (column.getScale() != null && column.getPrecision() < column.getScale()) {
				column.setPrecision(16);

				if (column.getPrecision() < column.getScale()) {
					column.setPrecision(column.getScale() + 1);
				}
			}

			String nullableString = attributes.getValue("Null");
			column.setNullable(AbstractJDBCSchemaFetcher.isYes(nullableString));

			// prevent VARCHAR(0) columns
			if (column.getDataType().equalsIgnoreCase("VARCHAR") && column.getByteLength() == 0) {
				column.setByteLength(255);
			}

			String colDefaultString = attributes.getValue("Default");
			column.setDefaultValue(colDefaultString);

			String colExtratString = attributes.getValue("Extra");
			if ("auto_increment".equals(colExtratString)) {
				column.setAutoIncrement(true);
			}
		} else if ("key".equals(qName)) {
			String tableName = attributes.getValue("Table");
			Table table = schema.getTableByName(tableName);
			String keyName = attributes.getValue("Key_name");

			if ("PRIMARY".equals(keyName)) {
				PK primaryKey = table.getPk();

				if (primaryKey == null) {
					primaryKey = factory.createPK(table);
					table.setPk(primaryKey);
					primaryKey.setName(keyName);
				}

				String colName = attributes.getValue("Column_name");
				primaryKey.addColumn(colName);
			} else {
				Index index = table.getIndexByName(keyName);

				if (index == null) {
					index = factory.createIndex(table);
					index.setName(keyName);
					table.addIndex(index);

					String indexType = attributes.getValue("Index_type");
					String notUniqueInt = attributes.getValue("Non_unique");

					if ("0".equals(notUniqueInt) && "BTREE".equalsIgnoreCase(indexType)) {
						index.setIndexType(DatabaseMetaData.tableIndexClustered);
						index.setUnique(true);
					} else if ("BTREE".equalsIgnoreCase(indexType)) {
						index.setIndexType(DatabaseMetaData.tableIndexClustered);
					} else {
						index.setIndexType(DatabaseMetaData.tableIndexOther);
					}
				}

				String colName = attributes.getValue("Column_name");
				String order = attributes.getValue("Collation");
				order = order == null ? "A" : order.toUpperCase(Locale.US);
				index.addColumn(colName, order.startsWith("A"));
			}
		}

		super.startElement(uri, localName, qName, attributes);
	}

	/**
	 * @param e1 SAXParseException
	 * @throws SAXException e
	 */
	public void fatalError(SAXParseException e1) throws SAXException {
		if (-1 == e1.getMessage().indexOf("An invalid XML character (Unicode:")) {
			super.fatalError(e1);
		}
	}

}
