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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.helpers.DefaultHandler;

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.TextFileUtils;
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
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceFKConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceIndexConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceViewConfig;
import com.cubrid.cubridmigration.core.engine.exception.ErrorMigrationTemplateException;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.mysql.trans.MySQL2CUBRIDMigParas;

/**
 * MigrationTemplateParser Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-9-8 created by Kevin Cao
 */
public final class MigrationTemplateParser {

	private static final String DEFAULT_MIGRATION_SCRIPT_NAME = "migration_script";
	private static final String VERSION = "9.3.0";
	private static final String INDENT_AMOUNT = "{http://xml.apache.org/xslt}indent-amount";
	private static final String UTF_8 = "utf-8";

	private MigrationTemplateParser() {
		//Do nothing here
	}

	/**
	 * Parse configuration file
	 * 
	 * @param file file input stream
	 * @param handler parse handler
	 */
	protected static void parse(InputStream file, DefaultHandler handler) {

		try {
			SAXParserFactory sf = SAXParserFactory.newInstance();
			sf.setValidating(false);
			SAXParser sp;
			sp = sf.newSAXParser();
			InputSource is = new InputSource(new InputStreamReader(file, UTF_8));
			sp.parse(is, handler);
		} catch (Exception e) {
			throw new ErrorMigrationTemplateException(e);
		}

	}

	/**
	 * Parse configuration file
	 * 
	 * @param fileName file location
	 * @return MigrationConfiguration
	 */
	public static MigrationConfiguration parse(String fileName) {

		try {
			final FileInputStream fis = new FileInputStream(fileName);
			try {
				final MigrationConfiguration config = parse(fis);
				autoNameConfiguration(fileName, config);
				return config;
			} finally {
				fis.close();
			}
		} catch (FileNotFoundException e) {
			throw new ErrorMigrationTemplateException(e);
		} catch (IOException ex) {
			throw new ErrorMigrationTemplateException(ex);
		}
	}

	/**
	 * //Auto update migration configuration name
	 * 
	 * @param fileName the script's file name
	 * @param config parsed configuration
	 */
	protected static void autoNameConfiguration(String fileName, final MigrationConfiguration config) {
		if (StringUtils.isBlank(config.getName())) {
			String cfgName = PathUtils.getFileNameWithoutExtendName(new File(fileName).getName());
			if (StringUtils.isBlank(cfgName)) {
				cfgName = DEFAULT_MIGRATION_SCRIPT_NAME;
			}
			config.setName(cfgName);
		}
	}

	/**
	 * Parse configuration file
	 * 
	 * @param configInputStream input stream
	 * @return MigrationConfiguration
	 */
	protected static MigrationConfiguration parse(InputStream configInputStream) {
		MigrationTemplateHandler reader = new MigrationTemplateHandler();
		parse(configInputStream, reader);
		MigrationConfiguration config = reader.getResult();
		if (!config.hasObjects2Export()) {
			throw new ErrorMigrationTemplateException("Invalid Configuration file.");
		}
		return config;
	}

	/**
	 * Transfer boolean to string
	 * 
	 * @param value of boolean
	 * @return yes or no
	 */
	private static String getBooleanString(boolean value) {
		return value ? TemplateTags.VALUE_YES : TemplateTags.VALUE_NO;
	}

	/**
	 * Save document to file
	 * 
	 * @param fileName to be save.
	 * @param document to be save.
	 * @throws Exception when errors
	 */
	private static void saveDoc2File(String fileName, Document document) throws Exception {
		DOMSource ds = new DOMSource(document);
		TransformerFactory tFactory = TransformerFactory.newInstance();
		//tFactory.setAttribute(TemplateTags.ATTR_indent-number, 2);
		Transformer transformer = tFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.METHOD, "xml");
		transformer.setOutputProperty(OutputKeys.ENCODING, UTF_8);
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty(INDENT_AMOUNT, "4");
		File file = new File(fileName);
		PathUtils.deleteFile(file);
		PathUtils.createFile(file);
		StreamResult result = new StreamResult(file);
		transformer.transform(ds, result);
	}

	/**
	 * Save configuration to a xml file
	 * 
	 * @param config to be save
	 * @param fileName target file
	 * @param saveSchema If saving source schema into migration script
	 */
	public static void save(MigrationConfiguration config, String fileName, boolean saveSchema) {
		try {
			Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
			//root
			Element root = document.createElement(TemplateTags.TAG_MIGRATION);
			root.setAttribute(TemplateTags.ATTR_VERSION, VERSION);
			root.setAttribute(TemplateTags.ATTR_NAME, config.getName());
			document.appendChild(root);
			//Source
			createSourceNode(config, document, root, saveSchema);
			//save target
			createTargetNode(config, document, root);
			//Parameters
			Element param = createElement(document, root, TemplateTags.TAG_PARAMS);
			param.setAttribute(TemplateTags.ATTR_EXPORT_THREAD,
					String.valueOf(config.getExportThreadCount()));
			param.setAttribute(TemplateTags.ATTR_IMPORT_THREAD,
					String.valueOf(config.getImportThreadCount()));
			param.setAttribute(TemplateTags.ATTR_COMMIT_COUNT,
					String.valueOf(config.getCommitCount()));
			param.setAttribute(TemplateTags.ATTR_PAGE_FETCH_COUNT,
					String.valueOf(config.getPageFetchCount()));
			param.setAttribute(TemplateTags.ATTR_IMPLICIT_ESTIMATE_PROGRESS,
					getBooleanString(config.isImplicitEstimate()));
			param.setAttribute(TemplateTags.ATTR_UPDATE_STATISTICS,
					getBooleanString(config.isUpdateStatistics()));
			if (config.hasOtherParam()) {
				String s1 = config.getOtherParam(MySQL2CUBRIDMigParas.UNPARSED_TIME);
				param.setAttribute(MySQL2CUBRIDMigParas.UNPARSED_TIME, s1 == null ? "" : s1);
				String s2 = config.getOtherParam(MySQL2CUBRIDMigParas.UNPARSED_DATE);
				param.setAttribute(MySQL2CUBRIDMigParas.UNPARSED_DATE, s2 == null ? "" : s2);
				String s3 = config.getOtherParam(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP);
				param.setAttribute(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP, s3 == null ? "" : s3);
				String s4 = config.getOtherParam(MySQL2CUBRIDMigParas.REPLAXE_CHAR0);
				param.setAttribute(MySQL2CUBRIDMigParas.REPLAXE_CHAR0, s4 == null ? "" : s4);
			}
			//Save XML content to file.
			saveDoc2File(fileName, document);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * 
	 * Create a target node
	 * 
	 * @param config to be save
	 * @param document of configuration
	 * @param root of the document.
	 */
	private static void createTargetNode(MigrationConfiguration config, Document document,
			Element root) {
		Element target = createElement(document, root, TemplateTags.TAG_TARGET);
		target.setAttribute(TemplateTags.ATTR_VERSION, config.getTargetDBVersion());
		if (config.targetIsGraph()) {
			target.setAttribute(TemplateTags.ATTR_TYPE, TemplateTags.VALUE_GRAPH);
			target.setAttribute(TemplateTags.ATTR_DB_TYPE, "graph");
		} else if (config.targetIsOnline()) {
			target.setAttribute(TemplateTags.ATTR_TYPE, TemplateTags.VALUE_ONLINE);
			target.setAttribute(TemplateTags.ATTR_DB_TYPE, "cubrid");
		} else if (config.targetIsFile()) {
			target.setAttribute(TemplateTags.ATTR_TYPE, TemplateTags.VALUE_DIR);
			target.setAttribute(TemplateTags.ATTR_DB_TYPE, "graph");
		} else {
			target.setAttribute(TemplateTags.ATTR_TYPE, TemplateTags.VALUE_OFFLINE);
			target.setAttribute(TemplateTags.ATTR_DB_TYPE, "cubrid");
		}
		
		if (config.targetIsGraph() || config.targetIsDBDump()) {
			createGraphTargetConInfoTag(config, document, target);
			createGraphTargetVertexTag(config, document, target);
			createGraphTargetEdgeTag(config, document, target);
		} else {
		createTargetConInfoNode(config, document, target);
		createTargetTableNodes(config, document, target);
		createTargetSequenceNodes(config, document, target);
		createTargetViewNodes(config, document, target);
		}
	}
	
	private static void createGraphTargetConInfoTag(MigrationConfiguration config, Document doc, Element target) {
		
		if (config.targetIsFile()) {
			
			Element dir = createElement(doc, target, TemplateTags.TAG_FILE_REPOSITORY);
			dir.setAttribute(TemplateTags.ATTR_DIR, config.getFileRepositroyPath());
			dir.setAttribute(TemplateTags.ATTR_SCHEMA, config.getTargetSchemaFileName());
			dir.setAttribute(TemplateTags.ATTR_DATA, config.getTargetDataFileName());
			dir.setAttribute(TemplateTags.ATTR_INDEX, config.getTargetIndexFileName());
			dir.setAttribute(TemplateTags.ATTR_TIMEZONE, config.getTargetFileTimeZone());
			dir.setAttribute(TemplateTags.ATTR_CHARSET, config.getTargetCharSet());

			dir.setAttribute(TemplateTags.ATTR_ONETABLEONEFILE,
					getBooleanString(config.isOneTableOneFile()));

			dir.setAttribute(TemplateTags.ATTR_DATA_FILE_FORMAT,
					String.valueOf(config.getDestType()));
			dir.setAttribute(TemplateTags.ATTR_OUTPUT_FILE_PREFIX, config.getTargetFilePrefix());
			dir.setAttribute(TemplateTags.ATTR_FILE_MAX_SIZE,
					String.valueOf(config.getMaxCountPerFile()));
			if (config.targetIsCSV()) {
				dir.setAttribute(
						TemplateTags.ATTR_CSV_SEPARATE,
						config.getCsvSettings().getSeparateChar() == MigrationConfiguration.CSV_NO_CHAR ? ""
								: String.valueOf(config.getCsvSettings().getSeparateChar()));
				dir.setAttribute(
						TemplateTags.ATTR_CSV_QUOTE,
						config.getCsvSettings().getQuoteChar() == MigrationConfiguration.CSV_NO_CHAR ? ""
								: String.valueOf(config.getCsvSettings().getQuoteChar()));
				dir.setAttribute(
						TemplateTags.ATTR_CSV_ESCAPE,
						config.getCsvSettings().getEscapeChar() == MigrationConfiguration.CSV_NO_CHAR ? ""
								: String.valueOf(config.getCsvSettings().getEscapeChar()));
			}
			if (config.targetIsDBDump()) {
				dir.setAttribute(TemplateTags.ATTR_LOB_ROOT_DIR, config.getTargetLOBRootPath());
			}
			
		} else {
			
			Element jdbc = createElement(doc, target, TemplateTags.TAG_JDBC);
			ConnParameters tcp = config.getTargetConParams();
			jdbc.setAttribute(TemplateTags.ATTR_HOST, tcp.getHost());
			jdbc.setAttribute(TemplateTags.ATTR_PORT, String.valueOf(tcp.getPort()));
			jdbc.setAttribute(TemplateTags.ATTR_DRIVER, tcp.getDriverFileName());
			jdbc.setAttribute(TemplateTags.ATTR_NAME, tcp.getDbName());
			jdbc.setAttribute(TemplateTags.ATTR_USER, tcp.getConUser());
			jdbc.setAttribute(TemplateTags.ATTR_PASSWORD, tcp.getConPassword());
			jdbc.setAttribute(TemplateTags.ATTR_CHARSET, tcp.getCharset());
			jdbc.setAttribute(TemplateTags.ATTR_TIMEZONE, tcp.getTimeZone());
			jdbc.setAttribute(TemplateTags.ATTR_CREATE_CONSTRAINT_NOW,
					getBooleanString(config.isCreateConstrainsBeforeData()));
			jdbc.setAttribute(TemplateTags.ATTR_WRITE_ERROR_RECORDS,
					getBooleanString(config.isWriteErrorRecords()));
			jdbc.setAttribute(TemplateTags.ATTR_USER_JDBC_URL, tcp.getUserJDBCURL());
			
			}
		
		return;
	}
	
	private static void createGraphTargetVertexTag(MigrationConfiguration config, Document doc, Element target) {
		GraphDictionary gdbDict = config.getGraphDictionary();
		
		Element vertexes = createElement(doc, target, TemplateTags.TAG_VERTEXES);
		
		for (Vertex vertex : gdbDict.getMigratedVertexList()) {
			
			Element vertexElement = createElement(doc, vertexes, TemplateTags.TAG_VERTEX);
			vertexElement.setAttribute(TemplateTags.ATTR_LABEL, vertex.getVertexLabel());
			vertexElement.setAttribute("vertex_type", "" + vertex.getVertexType());
			vertexElement.setAttribute("table_name", vertex.getTableName());
			vertexElement.setAttribute("table_owner", vertex.getOwner());
			
			HashMap<String, String> properties = (HashMap) vertex.getVertexProperties();
			List<Column> columnList = vertex.getColumnList();
			
			Element columnsElement = createElement(doc, vertexElement, "properties"); 
			
			for (Column col : columnList) {
				String colName = col.getName();
				String colType = col.getDataType();
				
				Element property = createElement(doc, columnsElement, TemplateTags.TAG_PROPERTY);
				
				property.setAttribute(TemplateTags.ATTR_NAME, colName);
				property.setAttribute(TemplateTags.ATTR_TYPE, colType);
				property.setAttribute("graph_type", col.getGraphDataType());
				property.setAttribute("datatype_support", String.valueOf(col.getSupportGraphDataType()));
			}
			
//			if (properties.isEmpty() || properties != null) {
//				
//				Iterator<String> propertyIter = properties.keySet().iterator();
//				
//				while (propertyIter.hasNext()) {
//					
//					Element property = createElement(doc, vertexElement, TemplateTags.TAG_PROPERTY);
//					String key = propertyIter.next();
//					
//					String typeValue = properties.get(key);
//					
//					property.setAttribute(TemplateTags.ATTR_NAME, key);
//					property.setAttribute(TemplateTags.ATTR_TYPE, typeValue);
//				}
//			}
		}
	}
	
	private static void createGraphTargetEdgeTag(MigrationConfiguration config, Document doc, Element target) {
		GraphDictionary gdbDict = config.getGraphDictionary();
		
		Element edges = createElement(doc, target, TemplateTags.TAG_EDGES);
		
		for (Edge edge : gdbDict.getMigratedEdgeList()) {
			Element edgeElement = createElement(doc, edges, TemplateTags.TAG_EDGE);
			
			edgeElement.setAttribute(TemplateTags.ATTR_LABEL, edge.getEdgeLabel());
			edgeElement.setAttribute("edge_type", "" + edge.getEdgeType());
			
			edgeElement.setAttribute(TemplateTags.ATTR_START_VERTEX, edge.getStartVertexName());
			edgeElement.setAttribute(TemplateTags.ATTR_END_VERTEX, edge.getEndVertexName());
			
			ArrayList<Column> columnList = (ArrayList<Column>) edge.getColumnList();
			HashMap<String, String> properties = (HashMap) edge.getEdgeProperties();
			TreeMap<String, String> fkRefers = (TreeMap<String, String>) edge.getfkCol2RefMapping();
			
			if (!properties.isEmpty() || properties != null) {
				Element propertiesElement = createElement(doc, edges, "properties");
				
				Iterator<String> propertyIter = properties.keySet().iterator();
				
				while (propertyIter.hasNext()) {
					
					Element property = createElement(doc, propertiesElement, "edge_property");
					String key = propertyIter.next();
					
					String typeValue = properties.get(key);
					
					property.setAttribute(TemplateTags.ATTR_NAME, key);
					property.setAttribute(TemplateTags.ATTR_TYPE, properties.get(key));
					for (Column col : columnList) {
						if (col.getName().equals(key)) {
							property.setAttribute("graph_type", col.getGraphDataType());
						}
					}
				}
			}
			
			if (!fkRefers.isEmpty() || fkRefers != null) {
				Element fkRefersElement = createElement(doc, edges, "fks");
				
				Iterator<String> referIter = fkRefers.keySet().iterator();
				
				while (referIter.hasNext()) {
					Element fk = createElement(doc, fkRefersElement, "fk");
					
					String colName = referIter.next();
					String refColName = fkRefers.get(colName);
					
					fk.setAttribute("column_name", colName);
					fk.setAttribute("ref_column_name", refColName);
				}
			}
		}
	}

	/**
	 * Create Target View Nodes
	 * 
	 * @param config MigrationConfiguration
	 * @param document document
	 * @param target Element
	 */
	private static void createTargetViewNodes(MigrationConfiguration config, Document document,
			Element target) {
		//Views
		Element views = createElement(document, target, TemplateTags.TAG_VIEWS);
		List<View> targetViews = config.getTargetViewSchema();
		for (View tt : targetViews) {
			Element view = createElement(document, views, TemplateTags.TAG_VIEW);
			view.setAttribute(TemplateTags.ATTR_NAME, tt.getName());
			Element viewQuerySQL = createElement(document, view, TemplateTags.TAG_VIEWQUERYSQL);
			viewQuerySQL.setTextContent(tt.getQuerySpec());
			Element createViewSQL = createElement(document, view, TemplateTags.TAG_CREATEVIEWSQL);
			createViewSQL.setTextContent(tt.getDDL());

			Element columns = createElement(document, view, TemplateTags.TAG_VIEWCOLUMNS);
			List<Column> cols = tt.getColumns();
			for (Column col : cols) {
				Element colNode = createElement(document, columns, TemplateTags.TAG_VIEWCOLUMN);
				colNode.setAttribute(TemplateTags.ATTR_NAME, col.getName());
				colNode.setAttribute(TemplateTags.ATTR_TYPE, col.getShownDataType());
				colNode.setAttribute(TemplateTags.ATTR_BASE_TYPE, col.getDataType());
				if (col.getSubDataType() != null) {
					colNode.setAttribute(TemplateTags.ATTR_SUB_TYPE, col.getSubDataType());
				}
				if (col.getDefaultValue() != null) {
					colNode.setAttribute(TemplateTags.ATTR_DEFAULT, col.getDefaultValue());
				}
			}
		}
	}

	/**
	 * createTargetSequenceNodes
	 * 
	 * @param config MigrationConfiguration
	 * @param document Document
	 * @param target Element
	 */
	private static void createTargetSequenceNodes(MigrationConfiguration config, Document document,
			Element target) {
		//sequences
		List<Sequence> targetSerials = config.getTargetSerialSchema();
		if (targetSerials.isEmpty()) {
			return;
		}
		Element sequences = createElement(document, target, TemplateTags.TAG_SEQUENCES);
		for (Sequence sc : targetSerials) {
			Element sequence = createElement(document, sequences, TemplateTags.TAG_SEQUENCE);
			sequence.setAttribute(TemplateTags.ATTR_NAME, sc.getName());
			sequence.setAttribute(TemplateTags.ATTR_START, String.valueOf(sc.getCurrentValue()));
			sequence.setAttribute(TemplateTags.ATTR_NO_MAX, getBooleanString(sc.isNoMaxValue()));
			if (sc.isNoMaxValue()) {
				sequence.setAttribute(TemplateTags.ATTR_MAX, "0");
			} else {
				sequence.setAttribute(TemplateTags.ATTR_MAX, String.valueOf(sc.getMaxValue()));
			}

			sequence.setAttribute(TemplateTags.ATTR_NO_MIN, getBooleanString(sc.isNoMinValue()));
			if (sc.isNoMinValue()) {
				sequence.setAttribute(TemplateTags.ATTR_MIN, "0");
			} else {
				sequence.setAttribute(TemplateTags.ATTR_MIN, String.valueOf(sc.getMinValue()));
			}
			sequence.setAttribute(TemplateTags.ATTR_CYCLE, getBooleanString(sc.isCycleFlag()));
			sequence.setAttribute(TemplateTags.ATTR_INCREMENT, String.valueOf(sc.getIncrementBy()));
			sequence.setAttribute(TemplateTags.ATTR_CACHE, getBooleanString(!sc.isNoCache()));
			if (sc.isNoCache()) {
				sequence.setAttribute(TemplateTags.ATTR_CACHE_SIZE, "0");
			} else {
				sequence.setAttribute(TemplateTags.ATTR_CACHE_SIZE,
						String.valueOf(sc.getCacheSize()));
			}
		}
	}

	/**
	 * createTargetTableNodes
	 * 
	 * @param config MigrationConfiguration
	 * @param document Document
	 * @param target Element
	 */
	private static void createTargetTableNodes(MigrationConfiguration config, Document document,
			Element target) {
		//tables
		Element tables = createElement(document, target, TemplateTags.TAG_TABLES);
		List<Table> targetTables = config.getTargetTableSchema();
		for (Table targetTable : targetTables) {
			Element table = createElement(document, tables, TemplateTags.TAG_TABLE);
			table.setAttribute(TemplateTags.ATTR_NAME, targetTable.getName());
			table.setAttribute(TemplateTags.ATTR_REUSE_OID,
					getBooleanString(targetTable.isReuseOID()));

			Element columns = createElement(document, table, TemplateTags.TAG_COLUMNS);
			List<Column> cols = targetTable.getColumns();
			for (Column col : cols) {
				Element colNode = createElement(document, columns, TemplateTags.TAG_COLUMN);
				colNode.setAttribute(TemplateTags.ATTR_NAME, col.getName());
				colNode.setAttribute(TemplateTags.ATTR_TYPE, col.getShownDataType());
				colNode.setAttribute(TemplateTags.ATTR_BASE_TYPE, col.getDataType());
				colNode.setAttribute(TemplateTags.ATTR_NULL, getBooleanString(col.isNullable()));
				colNode.setAttribute(TemplateTags.ATTR_AUTO_INCREMENT,
						getBooleanString(col.isAutoIncrement()));

				colNode.setAttribute(TemplateTags.ATTR_UNIQUE, getBooleanString(col.isUnique()));
				colNode.setAttribute(TemplateTags.ATTR_SHARED, getBooleanString(col.isShared()));
				if (col.isShared()) {
					colNode.setAttribute(TemplateTags.ATTR_SHARED_VALUE, col.getSharedValue());
				}
				if (col.isAutoIncrement()) {
					colNode.setAttribute(TemplateTags.ATTR_INCREMENT,
							String.valueOf(col.getAutoIncIncrVal()));
					colNode.setAttribute(TemplateTags.ATTR_START,
							String.valueOf(col.getAutoIncSeedVal()));
				} else {
					colNode.setAttribute(TemplateTags.ATTR_INCREMENT, "1");
					colNode.setAttribute(TemplateTags.ATTR_START, "0");
				}
				if (col.getSubDataType() != null) {
					colNode.setAttribute(TemplateTags.ATTR_SUB_TYPE, col.getSubDataType());
				}
				//colNode.setAttribute(TemplateTags.ATTR_cycle, col.getc.getName());
				if (col.getDefaultValue() != null) {
					colNode.setAttribute(TemplateTags.ATTR_DEFAULT, col.getDefaultValue());
					colNode.setAttribute(TemplateTags.ATTR_DEFAULT_EXPRESSION,
							getBooleanString(col.isDefaultIsExpression()));
				}
			}

			List<FK> tableFKs = targetTable.getFks();
			List<Index> tableIndexes = targetTable.getIndexes();
			PK tablePK = targetTable.getPk();
			if (!(tablePK == null && tableFKs.isEmpty() && tableIndexes.isEmpty())) {
				Element constraints = createElement(document, table, TemplateTags.TAG_CONSTRAINTS);
				//PK
				if (tablePK != null && CollectionUtils.isNotEmpty(tablePK.getPkColumns())) {
					Element pkNode = createElement(document, constraints, TemplateTags.TAG_PK);
					pkNode.setAttribute(TemplateTags.ATTR_FIELDS,
							list2String(tablePK.getPkColumns()));
				}
				//FK
				if (!tableFKs.isEmpty()) {
					for (FK fk : tableFKs) {
						Element fkNode = createElement(document, constraints, TemplateTags.TAG_FK);
						fkNode.setAttribute(TemplateTags.ATTR_NAME, fk.getName());
						fkNode.setAttribute(TemplateTags.ATTR_ON_UPDATE,
								MigrationTemplateHandler.FK_OPERATION.get(fk.getUpdateRule()));
						fkNode.setAttribute(TemplateTags.ATTR_ON_DELETE,
								MigrationTemplateHandler.FK_OPERATION.get(fk.getDeleteRule()));
						//						fkNode.setAttribute(TemplateTags.ATTR_ON_CACHE_OBJECT,
						//								fk.getOnCacheObject());
						fkNode.setAttribute(TemplateTags.ATTR_FIELDS,
								list2String(fk.getColumnNames()));
						fkNode.setAttribute(TemplateTags.ATTR_REF_TABLE,
								fk.getReferencedTableName());
						fkNode.setAttribute(TemplateTags.ATTR_REF_FIELDS,
								list2String(fk.getCol2RefMapping()));
					}
				}
				//Indexes
				if (!tableIndexes.isEmpty()) {
					for (Index index : tableIndexes) {
						Element indexNode = createElement(document, constraints,
								TemplateTags.TAG_INDEX);
						indexNode.setAttribute(TemplateTags.ATTR_NAME, index.getName());
						indexNode.setAttribute(TemplateTags.ATTR_REVERSE,
								getBooleanString(index.isReverse()));
						indexNode.setAttribute(TemplateTags.ATTR_UNIQUE,
								getBooleanString(index.isUnique()));
						indexNode.setAttribute(TemplateTags.ATTR_FIELDS,
								list2String(index.getColumnNames()));
						indexNode.setAttribute(TemplateTags.ATTR_ORDER_RULE,
								list2String(index.getColumnOrderRulesString()));
						//						indexNode.setAttribute(TemplateTags.ATTR_PRE_FIX,
						//								index.getMigrationPrefix());
					}
				}
			}
			//Partition
			PartitionInfo pi = targetTable.getPartitionInfo();
			if (pi == null) {
				continue;
			}
			Element partitions = createElement(document, table, TemplateTags.TAG_PARTITIONS);
			partitions.setAttribute(TemplateTags.ATTR_TYPE, pi.getPartitionMethod());
			partitions.setAttribute(TemplateTags.ATTR_EXPRESSION, pi.getPartitionExp());
			for (PartitionTable pt : pi.getPartitions()) {
				Element pNode = createElement(document, partitions,
						pi.getPartitionMethod().toLowerCase());
				pNode.setAttribute(TemplateTags.ATTR_NAME, pt.getPartitionName());
				if (TemplateTags.VALUE_HASH.equals(pi.getPartitionMethod())) {
					continue;
				}
				pNode.setAttribute(TemplateTags.ATTR_VALUE, pt.getPartitionDesc());
			}
			Element pDDLNode = createElement(document, partitions, TemplateTags.TAG_PARTITION_DDL);
			pDDLNode.setTextContent(pi.getDDL());
		}
	}

	/**
	 * createTargetConInfoNode
	 * 
	 * @param config MigrationConfiguration
	 * @param document Document
	 * @param target Element
	 */
	private static void createTargetConInfoNode(MigrationConfiguration config, Document document,
			Element target) {
		//JDBC information
		if (config.targetIsOnline()) {
			Element jdbc = createElement(document, target, TemplateTags.TAG_JDBC);
			ConnParameters tcp = config.getTargetConParams();
			jdbc.setAttribute(TemplateTags.ATTR_HOST, tcp.getHost());
			jdbc.setAttribute(TemplateTags.ATTR_PORT, String.valueOf(tcp.getPort()));
			jdbc.setAttribute(TemplateTags.ATTR_DRIVER, tcp.getDriverFileName());
			jdbc.setAttribute(TemplateTags.ATTR_NAME, tcp.getDbName());
			jdbc.setAttribute(TemplateTags.ATTR_USER, tcp.getConUser());
			jdbc.setAttribute(TemplateTags.ATTR_PASSWORD, tcp.getConPassword());
			jdbc.setAttribute(TemplateTags.ATTR_CHARSET, tcp.getCharset());
			jdbc.setAttribute(TemplateTags.ATTR_TIMEZONE, tcp.getTimeZone());
			jdbc.setAttribute(TemplateTags.ATTR_CREATE_CONSTRAINT_NOW,
					getBooleanString(config.isCreateConstrainsBeforeData()));
			jdbc.setAttribute(TemplateTags.ATTR_WRITE_ERROR_RECORDS,
					getBooleanString(config.isWriteErrorRecords()));
			jdbc.setAttribute(TemplateTags.ATTR_USER_JDBC_URL, tcp.getUserJDBCURL());
			//jdbc.setAttribute(TemplateTags.ATTR_SCHEMA, tcp.getSchema());
			return;
		}
		if (config.targetIsFile()) {
			Element dir = createElement(document, target, TemplateTags.TAG_FILE_REPOSITORY);
			dir.setAttribute(TemplateTags.ATTR_DIR, config.getFileRepositroyPath());
			dir.setAttribute(TemplateTags.ATTR_SCHEMA, config.getTargetSchemaFileName());
			dir.setAttribute(TemplateTags.ATTR_DATA, config.getTargetDataFileName());
			dir.setAttribute(TemplateTags.ATTR_INDEX, config.getTargetIndexFileName());
			dir.setAttribute(TemplateTags.ATTR_TIMEZONE, config.getTargetFileTimeZone());
			dir.setAttribute(TemplateTags.ATTR_CHARSET, config.getTargetCharSet());

			dir.setAttribute(TemplateTags.ATTR_ONETABLEONEFILE,
					getBooleanString(config.isOneTableOneFile()));

			dir.setAttribute(TemplateTags.ATTR_DATA_FILE_FORMAT,
					String.valueOf(config.getDestType()));
			dir.setAttribute(TemplateTags.ATTR_OUTPUT_FILE_PREFIX, config.getTargetFilePrefix());
			dir.setAttribute(TemplateTags.ATTR_FILE_MAX_SIZE,
					String.valueOf(config.getMaxCountPerFile()));
			if (config.targetIsCSV()) {
				dir.setAttribute(
						TemplateTags.ATTR_CSV_SEPARATE,
						config.getCsvSettings().getSeparateChar() == MigrationConfiguration.CSV_NO_CHAR ? ""
								: String.valueOf(config.getCsvSettings().getSeparateChar()));
				dir.setAttribute(
						TemplateTags.ATTR_CSV_QUOTE,
						config.getCsvSettings().getQuoteChar() == MigrationConfiguration.CSV_NO_CHAR ? ""
								: String.valueOf(config.getCsvSettings().getQuoteChar()));
				dir.setAttribute(
						TemplateTags.ATTR_CSV_ESCAPE,
						config.getCsvSettings().getEscapeChar() == MigrationConfiguration.CSV_NO_CHAR ? ""
								: String.valueOf(config.getCsvSettings().getEscapeChar()));
			}
			if (config.targetIsDBDump()) {
				dir.setAttribute(TemplateTags.ATTR_LOB_ROOT_DIR, config.getTargetLOBRootPath());
			}
			return;
		}
	}

	/**
	 * String list to a string
	 * 
	 * @param list of strings
	 * @return a string with ',' splided.
	 */
	private static String list2String(List<String> list) {
		StringBuffer sb = new StringBuffer();
		for (String ss : list) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			sb.append(ss);
		}
		return sb.toString();
	}

	/**
	 * Create a element of document
	 * 
	 * @param doc document
	 * @param parent element
	 * @param node element name to be created
	 * @return new element
	 */
	private static Element createElement(Document doc, Element parent, String node) {
		Element result = doc.createElement(node);
		parent.appendChild(result);
		return result;
	}

	/**
	 * Create a source node
	 * 
	 * @param config to be save
	 * @param document of configuration
	 * @param root of the document.
	 * @param saveSchema if save schema information into migration script
	 */
	private static void createSourceNode(MigrationConfiguration config, Document document,
			Element root, boolean saveSchema) {
		//source
		Element source = createElement(document, root, TemplateTags.TAG_SOURCE);
		source.setAttribute(TemplateTags.ATTR_DB_TYPE, config.getSourceTypeName());
		source.setAttribute(TemplateTags.ATTR_ONLINE, getBooleanString(config.sourceIsOnline()));
		//connection
		if (config.sourceIsOnline()) {
			Element jdbc = createElement(document, source, TemplateTags.TAG_JDBC);
			ConnParameters scp = config.getSourceConParams();
			if (scp == null) {
				throw new IllegalArgumentException("Source connection parameters can't be NULL.");
			}
			jdbc.setAttribute(TemplateTags.ATTR_HOST, scp.getHost());
			jdbc.setAttribute(TemplateTags.ATTR_PORT, String.valueOf(scp.getPort()));
			jdbc.setAttribute(TemplateTags.ATTR_DRIVER, scp.getDriverFileName());
			jdbc.setAttribute(TemplateTags.ATTR_NAME, scp.getDbName());
			jdbc.setAttribute(TemplateTags.ATTR_USER, scp.getConUser());
			jdbc.setAttribute(TemplateTags.ATTR_PASSWORD, scp.getConPassword());
			jdbc.setAttribute(TemplateTags.ATTR_CHARSET, scp.getCharset());
			jdbc.setAttribute(TemplateTags.ATTR_TIMEZONE, scp.getTimeZone());
			jdbc.setAttribute(TemplateTags.ATTR_USER_JDBC_URL, scp.getUserJDBCURL());
			//jdbc.setAttribute(TemplateTags.ATTR_SCHEMA, scp.getSchema());
			//jdbc.setAttribute(TemplateTags.ATTR_VERSION,sourceConParams.getDriverVersion());
			if (saveSchema) {
				Element elmSchema = createElement(document, jdbc, TemplateTags.TAG_SCHEMA);
				try {
					File tempFile = new File(PathUtils.getBaseTempDir() + UUID.randomUUID());
					Catalog srcCatalog = config.getSrcCatalog();
					srcCatalog.saveXML(tempFile);
					String sss = TextFileUtils.readText(tempFile.getCanonicalPath(), "utf-8",
							Integer.MAX_VALUE);
					PathUtils.deleteFile(tempFile);
					elmSchema.setTextContent(sss);
				} catch (Exception e) {
					jdbc.removeChild(elmSchema);
				}
				//Save SQL table's schema information
				List<Table> srcSQLTables = config.getSrcSQLSchema2Exp();
				if (CollectionUtils.isNotEmpty(srcSQLTables)) {
					Element elmSQLSchema = createElement(document, jdbc,
							TemplateTags.TAG_SQL_SCHEMA);
					try {
						File tempFile = new File(PathUtils.getBaseTempDir() + UUID.randomUUID());
						Catalog sqlCatalog = new Catalog();
						sqlCatalog.setName("sql_catalog");
						Schema sqlSchema = new Schema();
						sqlCatalog.addSchema(sqlSchema);
						sqlSchema.setName("sql_schema");
						sqlSchema.setTables(srcSQLTables);
						sqlCatalog.saveXML(tempFile);
						String sss = TextFileUtils.readText(tempFile.getCanonicalPath(), "utf-8",
								Integer.MAX_VALUE);
						PathUtils.deleteFile(tempFile);
						elmSQLSchema.setTextContent(sss);
					} catch (Exception e) {
						jdbc.removeChild(elmSQLSchema);
					}
				}
			}
		} else if (config.sourceIsSQL()) {
			Element sourceFile = createElement(document, source, TemplateTags.TAG_SQL);
			sourceFile.setAttribute(TemplateTags.ATTR_CHARSET, config.getSourceFileEncoding());
			List<String> files = config.getSqlFiles();
			for (String file : files) {
				Element sqlFile = createElement(document, sourceFile, TemplateTags.TAG_SQL_FILE);
				sqlFile.setAttribute(TemplateTags.ATTR_LOCATION, file);
			}
			return;
		} else if (config.sourceIsXMLDump()) {
			Element sourceFile = createElement(document, source, TemplateTags.TAG_FILE);
			sourceFile.setAttribute(TemplateTags.ATTR_LOCATION, config.getSourceFileName());
			sourceFile.setAttribute(TemplateTags.ATTR_CHARSET, config.getSourceFileEncoding());
			sourceFile.setAttribute(TemplateTags.ATTR_TIMEZONE, config.getSourceFileTimeZone());
			sourceFile.setAttribute(TemplateTags.ATTR_VERSION, config.getSourceFileVersion());
		} else if (config.sourceIsCSV()) {
			//csv
			Element tables = createElement(document, source, TemplateTags.TAG_CSVS);
			tables.setAttribute(
					TemplateTags.ATTR_CSV_SEPARATE,
					config.getCsvSettings().getSeparateChar() == MigrationConfiguration.CSV_NO_CHAR ? ""
							: String.valueOf(config.getCsvSettings().getSeparateChar()));
			tables.setAttribute(
					TemplateTags.ATTR_CSV_QUOTE,
					config.getCsvSettings().getQuoteChar() == MigrationConfiguration.CSV_NO_CHAR ? ""
							: String.valueOf(config.getCsvSettings().getQuoteChar()));
			tables.setAttribute(
					TemplateTags.ATTR_CSV_ESCAPE,
					config.getCsvSettings().getEscapeChar() == MigrationConfiguration.CSV_NO_CHAR ? ""
							: String.valueOf(config.getCsvSettings().getEscapeChar()));
			StringBuffer sb = new StringBuffer();
			for (String ns : config.getCsvSettings().getNullStrings()) {
				if (sb.length() > 0) {
					sb.append(";");
				}
				sb.append(ns);
			}
			if (sb.length() > 0) {
				tables.setAttribute(TemplateTags.ATTR_CSV_NULL_VALUE, sb.toString());
			}
			tables.setAttribute(TemplateTags.ATTR_CHARSET,
					String.valueOf(config.getCsvSettings().getCharset()));

			List<SourceCSVConfig> csvFiles = config.getCSVConfigs();
			for (SourceCSVConfig scc : csvFiles) {
				Element tbe = createElement(document, tables, TemplateTags.TAG_CSV);
				tbe.setAttribute(TemplateTags.ATTR_NAME, scc.getName());
				tbe.setAttribute(TemplateTags.ATTR_CREATE, getBooleanString(scc.isCreate()));
				tbe.setAttribute(TemplateTags.ATTR_REPLACE, getBooleanString(scc.isReplace()));
				tbe.setAttribute(TemplateTags.ATTR_IMPORT_FIRST_ROW,
						getBooleanString(scc.isImportFirstRow()));
				tbe.setAttribute(TemplateTags.ATTR_TARGET, scc.getTarget());

				Element columns = createElement(document, tbe, TemplateTags.TAG_CSV_COLUMNS);
				List<SourceCSVColumnConfig> columnConfigList = scc.getColumnConfigs();
				for (SourceCSVColumnConfig sccc : columnConfigList) {
					Element col = createElement(document, columns, TemplateTags.TAG_CSV_COLUMN);
					col.setAttribute(TemplateTags.ATTR_NAME, sccc.getName());
					col.setAttribute(TemplateTags.ATTR_TARGET, sccc.getTarget());
					col.setAttribute(TemplateTags.ATTR_CREATE, getBooleanString(sccc.isCreate()));
				}
			}
			return;
		}
		//tables
		Element tables = createElement(document, source, TemplateTags.TAG_TABLES);
		List<SourceEntryTableConfig> exportEntryTables = config.getExpEntryTableCfg();
		for (SourceEntryTableConfig setc : exportEntryTables) {
			Element tbe = createElement(document, tables, TemplateTags.TAG_TABLE);
			tbe.setAttribute(TemplateTags.ATTR_IS_SELECTED, setc.isSelected().toString());
			tbe.setAttribute(TemplateTags.ATTR_NAME, setc.getName());
			tbe.setAttribute(TemplateTags.ATTR_CREATE, getBooleanString(setc.isCreateNewTable()));
			tbe.setAttribute(TemplateTags.ATTR_REPLACE, getBooleanString(setc.isReplace()));
			tbe.setAttribute(TemplateTags.ATTR_MIGRATE_DATA, getBooleanString(setc.isMigrateData()));
			tbe.setAttribute(TemplateTags.ATTR_PK, getBooleanString(setc.isCreatePK()));
			tbe.setAttribute(TemplateTags.ATTR_PARTITION,
					getBooleanString(setc.isCreatePartition()));
			tbe.setAttribute(TemplateTags.ATTR_TARGET, setc.getTarget());
			tbe.setAttribute(TemplateTags.ATTR_CONDITION, setc.getCondition());
			tbe.setAttribute(TemplateTags.ATTR_BEFORE_SQL, setc.getSqlBefore());
			tbe.setAttribute(TemplateTags.ATTR_AFTER_SQL, setc.getSqlAfter());
			tbe.setAttribute(TemplateTags.ATTR_OWNER, setc.getOwner());
			if (setc.isEnableExpOpt()) {
				tbe.setAttribute(TemplateTags.ATTR_EXP_OPT_COL,
						getBooleanString(setc.isEnableExpOpt()));
				tbe.setAttribute(TemplateTags.ATTR_START_TAR_MAX,
						getBooleanString(setc.isStartFromTargetMax()));
			}

			Element columns = createElement(document, tbe, TemplateTags.TAG_COLUMNS);
			List<SourceColumnConfig> columnConfigList = setc.getColumnConfigList();
			for (SourceColumnConfig scc : columnConfigList) {
				Element col = createElement(document, columns, TemplateTags.TAG_COLUMN);
				col.setAttribute(TemplateTags.ATTR_NAME, scc.getName());
				col.setAttribute(TemplateTags.ATTR_TARGET, scc.getTarget());
				col.setAttribute(TemplateTags.ATTR_TRIM, getBooleanString(scc.isNeedTrim()));
				col.setAttribute(TemplateTags.ATTR_REPLACE_EXPRESSION, scc.getReplaceExp());
				col.setAttribute(TemplateTags.ATTR_USER_DATA_HANDLER, scc.getUserDataHandler());
			}

			List<SourceIndexConfig> indexConfigList = setc.getIndexConfigList();
			List<SourceFKConfig> fkConfigList = setc.getFKConfigList();
			if (indexConfigList.isEmpty() && fkConfigList.isEmpty()) {
				continue;
			}

			Element constraints = createElement(document, tbe, TemplateTags.TAG_CONSTRAINTS);
			for (SourceFKConfig fkc : fkConfigList) {
				Element fk = createElement(document, constraints, TemplateTags.TAG_FK);
				fk.setAttribute(TemplateTags.ATTR_NAME, fkc.getName());
				fk.setAttribute(TemplateTags.ATTR_TARGET, fkc.getTarget());
			}

			for (SourceIndexConfig sic : indexConfigList) {
				Element index = createElement(document, constraints, TemplateTags.TAG_INDEX);
				index.setAttribute(TemplateTags.ATTR_NAME, sic.getName());
				index.setAttribute(TemplateTags.ATTR_TARGET, sic.getTarget());
			}
		}
		//source SQL-tables
		List<SourceSQLTableConfig> exportSQLTables = config.getExpSQLCfg();
		if (!exportSQLTables.isEmpty()) {
			Element sqltables = createElement(document, source, TemplateTags.TAG_SQLTABLES);
			for (SourceSQLTableConfig setc : exportSQLTables) {
				Element tbe = createElement(document, sqltables, TemplateTags.TAG_SQLTABLE);
				tbe.setAttribute(TemplateTags.ATTR_NAME, setc.getName());
				tbe.setAttribute(TemplateTags.ATTR_CREATE,
						getBooleanString(setc.isCreateNewTable()));
				tbe.setAttribute(TemplateTags.ATTR_REPLACE, getBooleanString(setc.isReplace()));
				tbe.setAttribute(TemplateTags.ATTR_MIGRATE_DATA,
						getBooleanString(setc.isMigrateData()));
				tbe.setAttribute(TemplateTags.ATTR_TARGET, setc.getTarget());

				Element statement = createElement(document, tbe, TemplateTags.TAG_STATEMENT);
				statement.setTextContent(setc.getSql());
				Element columns = createElement(document, tbe, TemplateTags.TAG_COLUMNS);
				List<SourceColumnConfig> columnConfigList = setc.getColumnConfigList();
				for (SourceColumnConfig scc : columnConfigList) {
					Element col = createElement(document, columns, TemplateTags.TAG_COLUMN);
					col.setAttribute(TemplateTags.ATTR_NAME, scc.getName());
					col.setAttribute(TemplateTags.ATTR_TARGET, scc.getTarget());
					col.setAttribute(TemplateTags.ATTR_TRIM, getBooleanString(scc.isNeedTrim()));
					col.setAttribute(TemplateTags.ATTR_REPLACE_EXPRESSION, scc.getReplaceExp());
					col.setAttribute(TemplateTags.ATTR_USER_DATA_HANDLER, scc.getUserDataHandler());
				}
			}
		}
		//source sequences
		List<SourceSequenceConfig> exportSerials = config.getExpSerialCfg();
		if (!exportSerials.isEmpty()) {
			Element sequences = createElement(document, source, TemplateTags.TAG_SEQUENCES);
			for (SourceSequenceConfig sc : exportSerials) {
				Element sequence = createElement(document, sequences, TemplateTags.TAG_SEQUENCE);
				sequence.setAttribute(TemplateTags.ATTR_OWNER, sc.getOwner());
				sequence.setAttribute(TemplateTags.ATTR_NAME, sc.getName());
				sequence.setAttribute(TemplateTags.ATTR_TARGET, sc.getTarget());
				sequence.setAttribute(TemplateTags.ATTR_AUTO_SYNCHRONIZE_START_VALUE,
						getBooleanString(sc.isAutoSynchronizeStartValue()));
			}
		}
		//source views
		List<SourceViewConfig> exportViews = config.getExpViewCfg();
		if (!exportViews.isEmpty()) {
			Element views = createElement(document, source, TemplateTags.TAG_VIEWS);
			for (SourceViewConfig sc : exportViews) {
				Element vwNode = createElement(document, views, TemplateTags.TAG_VIEW);
				vwNode.setAttribute(TemplateTags.ATTR_OWNER, sc.getOwner());
				vwNode.setAttribute(TemplateTags.ATTR_NAME, sc.getName());
				vwNode.setAttribute(TemplateTags.ATTR_TARGET, sc.getTarget());
			}
		}
		//source triggers
		List<String> exportTriggers = config.getExpTriggerCfg();
		if (!exportTriggers.isEmpty()) {
			Element triggers = createElement(document, source, TemplateTags.TAG_TRIGGERS);
			for (String sc : exportTriggers) {
				Element trigger = createElement(document, triggers, TemplateTags.TAG_TRIGGER);
				trigger.setAttribute(TemplateTags.ATTR_NAME, sc);
			}
		}
		//source functions
		List<String> exportFunctions = config.getExpFunctionCfg();
		if (!exportFunctions.isEmpty()) {
			Element functions = createElement(document, source, TemplateTags.TAG_FUNCTIONS);
			for (String sc : exportFunctions) {
				Element fun = createElement(document, functions, TemplateTags.TAG_FUNCTION);
				fun.setAttribute(TemplateTags.ATTR_NAME, sc);
			}
		}
		//source procedures
		List<String> exportProcedures = config.getExpProcedureCfg();
		if (!exportProcedures.isEmpty()) {
			Element procedures = createElement(document, source, TemplateTags.TAG_PROCEDURES);
			for (String sc : exportProcedures) {
				Element pro = createElement(document, procedures, TemplateTags.TAG_PROCEDURE);
				pro.setAttribute(TemplateTags.ATTR_NAME, sc);
			}
		}
	}
}
