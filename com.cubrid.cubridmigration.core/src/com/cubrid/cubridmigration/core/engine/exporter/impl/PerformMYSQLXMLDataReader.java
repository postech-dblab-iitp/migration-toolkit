///*
// * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search Solution. 
// *
// * Redistribution and use in source and binary forms, with or without modification, 
// * are permitted provided that the following conditions are met: 
// *
// * - Redistributions of source code must retain the above copyright notice, 
// *   this list of conditions and the following disclaimer. 
// *
// * - Redistributions in binary form must reproduce the above copyright notice, 
// *   this list of conditions and the following disclaimer in the documentation 
// *   and/or other materials provided with the distribution. 
// *
// * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
// *   may be used to endorse or promote products derived from this software without 
// *   specific prior written permission. 
// *
// * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
// * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
// * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
// * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
// * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
// * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
// * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
// * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
// * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
// * OF SUCH DAMAGE. 
// *
// */
//package com.cubrid.cubridmigration.core.engine.exporter.impl;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.commons.collections.CollectionUtils;
//import org.xml.sax.Attributes;
//import org.xml.sax.SAXException;
//import org.xml.sax.helpers.DefaultHandler;
//
//import com.cubrid.cubridmigration.core.engine.MigrationStatusManager;
//import com.cubrid.cubridmigration.core.engine.RecordExportedListener;
//import com.cubrid.cubridmigration.core.engine.ThreadUtils;
//import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
//import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
//import com.cubrid.cubridmigration.core.engine.executors.IRunnableExecutor;
//
///**
// * get column value from MYSQL XML file
// * 
// * @author Kevin Cao
// */
//public class PerformMYSQLXMLDataReader extends
//		DefaultHandler {
//
//	private static final String ATTR_NAME = "name";
//	private static final String ATTR_XSI_NIL = "xsi:nil";
//	//private static final String ATTR_XSI_TYPE = "xsi:type";
//	private static final String VALUE_TRUE = "true";
//	//private static final String VALUE_XS_HEX_BINARY = "xs:hexBinary";
//	private static final String TAG_FIELD = "field";
//	private static final String TAG_ROW = "row";
//	private static final String TAG_TABLE_DATA = "table_data";
//
//	private MigrationConfiguration config;
//	private int exportingDataTableCount;
//	private IRunnableExecutor executor;
//	private RecordExportedListener oneNewRecord;
//	private List<Character> invalidateChars;
//	private MigrationStatusManager statusManager;
//
//	private SourceEntryTableConfig sourceTable;
//	private List<String[]> recordMap;
//	private List<List<String[]>> recordMapCache;
//	private String columName;
//	private StringBuilder dataStr;
//	private final List<String> finishedTables = new ArrayList<String>();
//	private boolean parsingCompleted = false;
//
//	public PerformMYSQLXMLDataReader() {
//
//	}
//
//	/**
//	 * @param ch char[]
//	 * @param start int
//	 * @param length int
//	 * @throws SAXException e
//	 */
//	public void characters(char[] ch, int start, int length) throws SAXException {
//		//Handling invalid char in XML dump file 
//		for (int i = start; i < start + length; i++) {
//			if (ch[i] == 0xfffd && !invalidateChars.isEmpty()) {
//				ch[i] = invalidateChars.get(0);
//				invalidateChars.remove(0);
//			}
//		}
//
//		if (null == sourceTable || null == columName || null == dataStr) {
//			return;
//		}
//
//		dataStr.append(ch, start, length);
//	}
//
//	/**
//	 * Create a new parsing task.
//	 * 
//	 */
//	private void createNewTask() {
//		executor.execute(new XMLDataParsingTask(config, oneNewRecord, sourceTable.getName(),
//				recordMapCache));
//	}
//
//	/**
//	 * deal with when element ended
//	 * 
//	 * @param uri String
//	 * @param localName String
//	 * @param qName String
//	 * @throws SAXException e
//	 */
//	public void endElement(String uri, String localName, String qName) throws SAXException {
//		if (null == sourceTable) {
//			return;
//		}
//		if (TAG_TABLE_DATA.equals(qName)) {
//			if (CollectionUtils.isNotEmpty(recordMapCache)) {
//				createNewTask();
//			}
//			oneNewRecord.endExportTable(sourceTable.getName());
//			if (sourceTable != null) {
//				finishedTables.add(sourceTable.getName());
//			}
//			recordMapCache = null;
//			sourceTable = null;
//			if (finishedTables.size() == exportingDataTableCount) {
//				parsingCompleted = true;
//			}
//		} else if (TAG_ROW.equals(qName) && null != sourceTable && null != recordMapCache) {
//			recordMapCache.add(recordMap);
//			//Watching memory to avoid out of memory errors
//			int status = MigrationStatusManager.STATUS_WAITING;
//			int counter = 0;
//			while (true) {
//				status = statusManager.isCommitNow(sourceTable.getName(), recordMapCache.size(),
//						config.getCommitCount());
//				if (status == MigrationStatusManager.STATUS_WAITING) {
//					ThreadUtils.threadSleep(1000, null);
//					counter++;
//				} else {
//					break;
//				}
//				//If waiting for 10 seconds, the data will be committed right now.
//				if (counter >= 10) {
//					status = MigrationStatusManager.STATUS_COMMIT;
//					break;
//				}
//			}
//			if (status == MigrationStatusManager.STATUS_COMMIT) {
//				createNewTask();
//				recordMapCache = new ArrayList<List<String[]>>();
//			}
//		} else if (TAG_FIELD.equals(qName) && null != columName && null != recordMap) {
//			recordMap.add(new String[] {columName, dataStr == null ? null : dataStr.toString()});
//		}
//	}
//
//	/**
//	 * deal with when element started
//	 * 
//	 * @param uri String
//	 * @param localName String
//	 * @param qName String
//	 * @param attributes Attributes
//	 * @throws SAXException e
//	 */
//	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
//		String attrName = attributes.getValue(ATTR_NAME);
//		if (TAG_TABLE_DATA.equals(qName)) {
//			SourceEntryTableConfig st = config.getExpEntryTableCfg(null, attrName);
//			if (null == st || !st.isMigrateData()) {
//				return;
//			}
//			sourceTable = st;
//			recordMapCache = new ArrayList<List<String[]>>();
//			oneNewRecord.startExportTable(st.getName());
//		} else if (TAG_ROW.equals(qName) && null != sourceTable) {
//			recordMap = new ArrayList<String[]>();
//		} else if (TAG_FIELD.equals(qName) && null != sourceTable) {
//			String cn = attrName;
//			if (!config.isExportColumn(null, sourceTable.getName(), cn)) {
//				columName = null;
//				return;
//			}
//			columName = cn;
//			String nullAttr = attributes.getValue(ATTR_XSI_NIL);
//			dataStr = VALUE_TRUE.equals(nullAttr) ? null : new StringBuilder();
//			//isHexBinary = VALUE_XS_HEX_BINARY.equals(attributes.getValue(ATTR_XSI_TYPE));
//		}
//	}
//
//	public boolean isParsingCompleted() {
//		return parsingCompleted;
//	}
//
//	/**
//	 * Set migration configuration object.
//	 * 
//	 * @param config MigrationConfiguration
//	 */
//	public void setConfig(MigrationConfiguration config) {
//		this.config = config;
//		int count = 0;
//		for (SourceEntryTableConfig setc : config.getExpEntryTableCfg()) {
//			if (setc.isMigrateData()) {
//				count++;
//			}
//		}
//		exportingDataTableCount = count;
//	}
//
//	public void setExecutor(IRunnableExecutor executor) {
//		this.executor = executor;
//	}
//
//	public void setOneNewRecord(RecordExportedListener oneNewRecord) {
//		this.oneNewRecord = oneNewRecord;
//	}
//
//	public void setInvalidateChars(List<Character> invalidateChars) {
//		this.invalidateChars = invalidateChars;
//	}
//
//	public void setStatusManager(MigrationStatusManager statusManager) {
//		this.statusManager = statusManager;
//	}
//}
