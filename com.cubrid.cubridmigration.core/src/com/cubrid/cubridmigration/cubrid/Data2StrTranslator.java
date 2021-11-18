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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.DBUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.trans.IData2StrTranslator;
import com.cubrid.cubridmigration.cubrid.exception.FormatCUBRIDDataTypeException;
import com.cubrid.cubridmigration.cubrid.format.BitToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.CharToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.DateToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.DatetimeToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.DoubleToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.FloatToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.IntegerToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.NumericToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.TimeToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.TimestampToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.csv.CSVBitToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.csv.CSVCharToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.dump.UnloadBitToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.dump.UnloadCharToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.dump.UnloadDateToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.dump.UnloadDatetimeToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.dump.UnloadNCharToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.dump.UnloadNumericToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.dump.UnloadTimeToCUBRIDString;
import com.cubrid.cubridmigration.cubrid.format.dump.UnloadTimestampToCUBRIDString;

/**
 * help to create load DB file
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-18 created by Kevin Cao
 * 
 */
public class Data2StrTranslator implements
		IData2StrTranslator {
	private static final Logger LOG = LogUtil.getLogger(Data2StrTranslator.class);

	public static final String LOBFILEPATH = "%LOBFILEPATH%";
	private static final String VALUE_NULL = "NULL";
	private static final int TOTAL_CHAR_NUMBER_IN_LINE = 70;
	private static final String END_LINE = "\n";
	private static final String TARGET_CHARSET = "utf-8";
	private static final String CLOB_HEADER = "^E'C";
	private static final String BLOB_HEADER = "^E'B";
	private static final String LOB_HEADER = "|file:";

	private final String lobFilePath;

	private final Map<Integer, IFormatValueToString> formaters = new HashMap<Integer, IFormatValueToString>();

	private final int targetDataFileFormat;

	public Data2StrTranslator(String dataFilePath,
			MigrationConfiguration config, int targetDataFileFormat) {
		this.lobFilePath = dataFilePath;
		this.targetDataFileFormat = targetDataFileFormat;
		//Initialize formaters mapping
		formaters.put(DataTypeConstant.CUBRID_DT_MONETARY,
				new IntegerToCUBRIDString());
		formaters.put(DataTypeConstant.CUBRID_DT_INTEGER,
				new IntegerToCUBRIDString());
		formaters.put(DataTypeConstant.CUBRID_DT_SMALLINT,
				new IntegerToCUBRIDString());
		formaters.put(DataTypeConstant.CUBRID_DT_BIGINT,
				new IntegerToCUBRIDString());
		formaters.put(DataTypeConstant.CUBRID_DT_FLOAT,
				new FloatToCUBRIDString());
		formaters.put(DataTypeConstant.CUBRID_DT_DOUBLE,
				new DoubleToCUBRIDString());

		if (targetDataFileFormat == MigrationConfiguration.DEST_CSV
				|| targetDataFileFormat == MigrationConfiguration.DEST_XLS) {
			formaters.put(DataTypeConstant.CUBRID_DT_BIT,
					new CSVBitToCUBRIDString(new BitToCUBRIDString()));
			formaters.put(DataTypeConstant.CUBRID_DT_VARBIT,
					new CSVBitToCUBRIDString(new BitToCUBRIDString()));
			formaters.put(DataTypeConstant.CUBRID_DT_NCHAR,
					new CSVCharToCUBRIDString(new CharToCUBRIDString(), config));
			formaters.put(DataTypeConstant.CUBRID_DT_NVARCHAR,
					new CSVCharToCUBRIDString(new CharToCUBRIDString(), config));
			formaters.put(DataTypeConstant.CUBRID_DT_CHAR,
					new CSVCharToCUBRIDString(new CharToCUBRIDString(), config));
			formaters.put(DataTypeConstant.CUBRID_DT_VARCHAR,
					new CSVCharToCUBRIDString(new CharToCUBRIDString(), config));
			formaters.put(DataTypeConstant.CUBRID_DT_ENUM,
					new CSVCharToCUBRIDString(new CharToCUBRIDString(), config));
			formaters.put(DataTypeConstant.CUBRID_DT_TIME,
					new CSVCharToCUBRIDString(new TimeToCUBRIDString(config),
							config));
			formaters.put(DataTypeConstant.CUBRID_DT_DATE,
					new CSVCharToCUBRIDString(new DateToCUBRIDString(config),
							config));
			formaters.put(DataTypeConstant.CUBRID_DT_TIMESTAMP,
					new CSVCharToCUBRIDString(new TimestampToCUBRIDString(),
							config));
			formaters.put(DataTypeConstant.CUBRID_DT_DATETIME,
					new CSVCharToCUBRIDString(
							new DatetimeToCUBRIDString(config), config));
			formaters.put(DataTypeConstant.CUBRID_DT_NUMERIC,
					new NumericToCUBRIDString());
		} else {
			formaters.put(DataTypeConstant.CUBRID_DT_BIT,
					new UnloadBitToCUBRIDString(new BitToCUBRIDString()));
			formaters.put(DataTypeConstant.CUBRID_DT_VARBIT,
					new UnloadBitToCUBRIDString(new BitToCUBRIDString()));
			formaters.put(DataTypeConstant.CUBRID_DT_NCHAR,
					new UnloadNCharToCUBRIDString(new CharToCUBRIDString()));
			formaters.put(DataTypeConstant.CUBRID_DT_NVARCHAR,
					new UnloadNCharToCUBRIDString(new CharToCUBRIDString()));
			formaters.put(DataTypeConstant.CUBRID_DT_CHAR,
					new UnloadCharToCUBRIDString(new CharToCUBRIDString()));
			formaters.put(DataTypeConstant.CUBRID_DT_VARCHAR,
					new UnloadCharToCUBRIDString(new CharToCUBRIDString()));
			formaters.put(DataTypeConstant.CUBRID_DT_ENUM,
					new UnloadCharToCUBRIDString(new CharToCUBRIDString()));
			formaters.put(
					DataTypeConstant.CUBRID_DT_TIME,
					new UnloadTimeToCUBRIDString(new TimeToCUBRIDString(config)));
			formaters.put(
					DataTypeConstant.CUBRID_DT_DATE,
					new UnloadDateToCUBRIDString(new DateToCUBRIDString(config)));
			formaters.put(DataTypeConstant.CUBRID_DT_TIMESTAMP,
					new UnloadTimestampToCUBRIDString(
							new TimestampToCUBRIDString()));
			formaters.put(DataTypeConstant.CUBRID_DT_DATETIME,
					new UnloadDatetimeToCUBRIDString(
							new DatetimeToCUBRIDString(config)));
			formaters.put(
					DataTypeConstant.CUBRID_DT_NUMERIC,
					new UnloadNumericToCUBRIDString(new NumericToCUBRIDString()));
		}

	}

	/**
	 * exportToCUBRIDUnLoadFile
	 * 
	 * @param dataVal Object
	 * @param cubridColumn Column
	 * @param lobFiles to be uploaded
	 * @return String
	 */
	public String stringValueOf(Object dataVal, Column cubridColumn,
			List<String> lobFiles) {
		if (dataVal == null) {
			return VALUE_NULL;
		}
		//Integer scale = cubridColumn.getScale();
		//Integer dataTypeID = cubridColumn.getJdbcIDOfDataType();
		final String tableName = cubridColumn.getTableOrView().getName();
		CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
		if (dataTypeHelper.isCollection(cubridColumn.getDataType())) {
			String subDataType = cubridColumn.getSubDataType();

			if (dataVal instanceof Collection<?>) {
				Collection<?> list = (Collection<?>) dataVal;
				StringBuffer bf = new StringBuffer();
				bf.append("{");
				int count = 0;

				for (Object item : list) {
					if (count > 0) {
						bf.append(",");
					}
					bf.append(exportToCUBRIDUnLoadFile(item, subDataType,
							tableName, lobFiles));
					count++;
				}
				bf.append("}");
				return bf.toString();
			} else if (dataVal.getClass().isArray()) {
				Object[] list = (Object[]) dataVal;
				StringBuffer bf = new StringBuffer();
				bf.append("{");
				int count = 0;

				for (Object item : list) {
					if (count > 0) {
						bf.append(",");
					}

					bf.append(exportToCUBRIDUnLoadFile(item, subDataType,
							tableName, lobFiles));
					count++;
				}
				bf.append("}");
				return bf.toString();
			}
			FormatDataResult result = CUBRIDFormator.format(
					cubridColumn.getDataTypeInstance(), String.valueOf(dataVal));
			if (result.isSuccess()) {
				StringBuffer bf = new StringBuffer();
				if (StringUtils.isEmpty(result.getFormatResult())) {
					bf.append(VALUE_NULL);
				} else {
					bf.append(result.getFormatResult());
				}
				return bf.toString();
			}
			String message = "Can not format data \"" + dataVal
					+ "\" to data type \"" + cubridColumn.getDataType() + "\"";
			if (result.throwable == null) {
				throw new FormatCUBRIDDataTypeException(message);
			} else {
				throw new FormatCUBRIDDataTypeException(message,
						result.throwable);
			}
		}
		return exportToCUBRIDUnLoadFile(dataVal, cubridColumn.getDataType(),
				tableName, lobFiles);
	}

	/**
	 * return data value string
	 * 
	 * @param dataVal Object
	 * @param dataType Integer
	 * @param tableName String
	 * @param lobFiles to be uploaded
	 * @return String
	 */
	private String exportToCUBRIDUnLoadFile(Object dataVal, String dataType,
			String tableName, List<String> lobFiles) {
		CUBRIDDataTypeHelper cubDTHelper = CUBRIDDataTypeHelper.getInstance(null);
		Integer dataTypeID = cubDTHelper.getCUBRIDDataTypeID(dataType);
		IFormatValueToString formater = formaters.get(dataTypeID);
		if (formater != null) {
			return formater.format(dataVal);
		}
		if (dataTypeID == DataTypeConstant.CUBRID_DT_BLOB) {
			if (targetDataFileFormat == MigrationConfiguration.DEST_DB_UNLOAD) {
				return exportBlobToFile(dataVal, tableName, lobFiles);
			}
			formater = formaters.get(DataTypeConstant.CUBRID_DT_BIT);
			return formater.format(dataVal);
		} else if (dataTypeID == DataTypeConstant.CUBRID_DT_CLOB) {
			if (targetDataFileFormat == MigrationConfiguration.DEST_DB_UNLOAD) {
				return exportClobToFile(dataVal, tableName, lobFiles);
			}
			formater = formaters.get(DataTypeConstant.CUBRID_DT_VARCHAR);
			return formater.format(dataVal);
		}
		return VALUE_NULL;
	}

	/**
	 * return a line of a record in the unload file
	 * 
	 * @param columnList column list
	 * @param columnDataList data list
	 * @return String
	 */
	public String getRecordString(List<ColumnValue> columnList,
			List<String> columnDataList) {
		if (columnDataList == null) {
			return null;
		}
		StringBuffer bf = new StringBuffer();

		for (int i = 0; i < columnDataList.size(); i++) {
			if (i != 0) {
				bf.append(' ');
			}
			bf.append(columnDataList.get(i));
		}
		return bf.toString();
	}

	/**
	 * write content to File
	 * 
	 * @param obj Object
	 * @param fileName String
	 * @throws IOException e
	 * @throws SQLException e
	 */
	private void writeToFile(Object obj, String fileName) throws IOException,
			SQLException {

		if (obj instanceof InputStream) {
			CUBRIDIOUtils.writeToFile(fileName, (InputStream) obj);
		} else if (obj instanceof byte[]) {
			byte buf[] = (byte[]) obj;
			CUBRIDIOUtils.writeToFile(fileName, new ByteArrayInputStream(buf));
		} else if (obj instanceof String) {
			String clob = (String) obj;
			CUBRIDIOUtils.writeToFile(new StringReader(clob), fileName,
					TARGET_CHARSET);
		} else {
			String msg = "Error data type, real data type: "
					+ obj.getClass()
					+ " type ,expected Blob, InputSteam, byte[], String or Clob";
			throw new IllegalArgumentException(msg);
		}
	}

	/**
	 * export Blob
	 * 
	 * @param data Object
	 * @param targetTableName String
	 * @param lobFiles to be uploaded
	 * @return String
	 */
	private String exportBlobToFile(Object data, String targetTableName,
			List<String> lobFiles) {
		if (data == null || lobFiles == null) {
			return VALUE_NULL;
		}
		String blobFileName = targetTableName + "." + DBUtils.getIdentity();
		String blobFilePath = PathUtils.mergePath(lobFilePath, "/lob/"
				+ targetTableName + "/" + blobFileName);
		blobFilePath = PathUtils.getLocalHostFilePath(blobFilePath);

		try {
			File parentFile = new File(blobFilePath).getParentFile();

			if (PathUtils.checkPathExist(parentFile)) {
				writeToFile(data, blobFilePath);
			}
			lobFiles.add(blobFilePath);
			return new StringBuffer(BLOB_HEADER).append(
					new File(blobFilePath).length()).append(LOB_HEADER).append(
					LOBFILEPATH).append(blobFileName).append("|").append(
					targetTableName).append("'").toString();
		} catch (Exception e) {
			LOG.error("", e);
		}

		return VALUE_NULL;
	}

	/**
	 * export Clob
	 * 
	 * @param data Object
	 * @param targetTableName String
	 * @param lobFiles to be uploaded
	 * @return String
	 */
	private String exportClobToFile(Object data, String targetTableName,
			List<String> lobFiles) {
		if (data == null || lobFiles == null) {
			return VALUE_NULL;
		}
		String clobFileName = targetTableName + DBUtils.getIdentity();
		String clobFilePath = PathUtils.mergePath(lobFilePath, "/lob/"
				+ targetTableName + "/" + clobFileName);
		clobFilePath = PathUtils.getLocalHostFilePath(clobFilePath);
		try {
			File parentFile = new File(clobFilePath).getParentFile();
			if (PathUtils.checkPathExist(parentFile)) {
				writeToFile(data, clobFilePath);
			}
			lobFiles.add(clobFilePath);

			return new StringBuffer(CLOB_HEADER).append(
					new File(clobFilePath).length()).append(LOB_HEADER).append(
					LOBFILEPATH).append(clobFileName).append("|").append(
					targetTableName).append("'").toString();
		} catch (Exception e) {
			LOG.error("", e);
		}
		return VALUE_NULL;
	}

	/**
	 * If the char or varchar value is to long (>70), separate it
	 * 
	 * @param data to be handle
	 * @return String with quoted and separated '....'+'....'
	 */
	public static String quoteAndSeparateString(String data) {
		StringBuffer bf = new StringBuffer("'");
		int length = 0;
		int end = data.length() - 1;
		for (int i = 0; i <= end; i++) {
			char c = data.charAt(i);
			if (c == '\'') {
				bf.append(c);
			}
			bf.append(c);
			length++;
			if (i != end && length == TOTAL_CHAR_NUMBER_IN_LINE) {
				bf.append("'+").append(END_LINE).append(" '");
				length = 0;
			}
		}
		bf.append("'");
		return bf.toString();
	}

	/**
	 * append value to string buffer
	 * 
	 * @param value String
	 * @return quoted string '...''...''...'
	 */
	public static String quoteString(String value) {
		StringBuffer bf = new StringBuffer("'");
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if (c == '\'') {
				bf.append(c);
			}
			bf.append(c);
		}
		bf.append("'");
		return bf.toString();
	}
}
