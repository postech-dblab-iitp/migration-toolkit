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
//import java.io.UnsupportedEncodingException;
//import java.sql.Time;
//import java.sql.Timestamp;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.TimeZone;
//
//import org.apache.log4j.Logger;
//
//import com.cubrid.cubridmigration.core.common.log.LogUtil;
//import com.cubrid.cubridmigration.core.dbobject.Column;
//import com.cubrid.cubridmigration.core.dbobject.Record;
//import com.cubrid.cubridmigration.core.dbobject.Table;
//import com.cubrid.cubridmigration.core.engine.RecordExportedListener;
//import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
//import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
//import com.cubrid.cubridmigration.mysql.MySQLDataTypeHelper;
//import com.cubrid.cubridmigration.mysql.trans.MySQL2CUBRIDMigParas;
//
///**
// * ImportTask responses to parse data to record and import to target database.
// * 
// * @author Kevin Cao
// * @version 1.0 - 2011-8-23 created by Kevin Cao
// */
//public class XMLDataParsingTask implements
//		Runnable {
//	private final static Logger LOG = LogUtil.getLogger(XMLDataParsingTask.class);
//
//	private final String tableName;
//	private final List<List<String[]>> recordMaps;
//	private final RecordExportedListener oneNewRecord;
//	private final MigrationConfiguration config;
//	private final MySQLDataTypeHelper dtHelper = MySQLDataTypeHelper.getInstance(null);
//
//	public XMLDataParsingTask(MigrationConfiguration config,
//			RecordExportedListener oneNewRecord, String tableName,
//			List<List<String[]>> recordMaps) {
//		this.config = config;
//		this.oneNewRecord = oneNewRecord;
//		this.tableName = tableName;
//		this.recordMaps = recordMaps;
//	}
//
//	/**
//	 * Retrieves MYSQL column values
//	 * 
//	 * @param column Column
//	 * @param data String
//	 * @return Object value
//	 */
//	private Object getMysqlData(Column column, String data) {
//		if (column == null) {
//			throw new RuntimeException("Column can't be null.");
//		}
//		final boolean binaryType = dtHelper.isBinary(column.getDataType());
//		if (null == data) {
//			return binaryType ? new byte[0] : null;
//		}
//		if (binaryType) {
//			return handleBitType(column, data);
//		} else {
//			TimeZone sourceTz = config.getSourceDatabaseTimeZone();
//			if (column.getDataType().equalsIgnoreCase("DATE")) {
//				try {
//					return new Date(CUBRIDTimeUtil.parseDate2Long(data,
//							sourceTz));
//				} catch (Exception ex) {
//					String dateValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_DATE);
//					return MySQL2CUBRIDMigParas.getReplacedDate(dateValue,
//							sourceTz);
//				}
//			} else if (column.getDataType().equalsIgnoreCase("DATETIME")) {
//				try {
//					return new Timestamp(CUBRIDTimeUtil.parseTimestamp(data,
//							sourceTz));
//				} catch (Exception ex) {
//					String timestampValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP);
//					return MySQL2CUBRIDMigParas.getReplacedTimestamp(
//							timestampValue, sourceTz);
//				}
//			} else if (column.getDataType().equalsIgnoreCase("TIMESTAMP")) {
//				try {
//					return new Timestamp(CUBRIDTimeUtil.parseTimestamp(data,
//							null));
//				} catch (Exception ex) {
//					String timestampValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP);
//					return MySQL2CUBRIDMigParas.getReplacedTimestamp(
//							timestampValue, sourceTz);
//				}
//			} else if (column.getDataType().equalsIgnoreCase("TIME")) {
//				try {
//					return new Time(CUBRIDTimeUtil.parseTime2Long(data,
//							sourceTz));
//				} catch (Exception ex) {
//					String timeValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_TIME);
//					return MySQL2CUBRIDMigParas.getReplacedTime(timeValue,
//							sourceTz);
//				}
//			} else {
//				return data;
//			}
//		}
//	}
//
//	/**
//	 * Transform XML string to bytes if column type is bit/byte/blob...
//	 * 
//	 * @param column Column
//	 * @param data String
//	 * @return byte[] or boolean
//	 */
//	private Object handleBitType(Column column, String data) {
//		try {
//			byte[] bytes = data.getBytes(config.getSourceFileEncoding());
//			if (column.getDataType().equalsIgnoreCase("BIT")
//					&& column.getPrecision() == 1) {
//				return bytes[0] == 1;
//			} else {
//				return bytes;
//			}
//		} catch (UnsupportedEncodingException e) {
//			return new byte[0];
//		}
//	}
//
//	/**
//	 * Run
//	 */
//	public void run() {
//		try {
//			List<Record> records = new ArrayList<Record>(recordMaps.size());
//			Table stable = config.getSrcTableSchema(null, tableName);
//			for (List<String[]> recordMap : recordMaps) {
//				Record record = new Record();
//				for (String[] key : recordMap) {
//					Column column = stable.getColumnByName(key[0]);
//					Object value = key[1];
//					if (value != null) {
//						value = getMysqlData(column, value.toString());
//					}
//					record.addColumnValue(column, value);
//				}
//				records.add(record);
//			}
//			oneNewRecord.processRecords(tableName, records);
//		} catch (Exception ex) {
//			LOG.error("", ex);
//		}
//	}
//}
