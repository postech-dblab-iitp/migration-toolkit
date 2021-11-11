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
package com.cubrid.cubridmigration.mssql.trans;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDFormator;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.cubrid.FormatDataResult;
import com.cubrid.cubridmigration.cubrid.trans.ToCUBRIDDataConverterFacade;

/**
 * A transform class which helps to data transform in migration of Oracle to
 * CUBRID
 * 
 * @author Kevin Cao
 * 
 */
public class MSSQL2CUBRIDTranformHelper extends
		DBTransformHelper {

	private final static Logger LOG = LogUtil.getLogger(MSSQL2CUBRIDTranformHelper.class);

	public MSSQL2CUBRIDTranformHelper(AbstractDataTypeMappingHelper dataTypeMapping,
			ToCUBRIDDataConverterFacade cf) {
		super(dataTypeMapping, cf);
	}

	/**
	 * return a cloned target view
	 * 
	 * @param sourceView View source view
	 * @param config MigrationConfiguration
	 * 
	 * @return View
	 */
	public View getCloneView(View sourceView, MigrationConfiguration config) {
		View targetView = super.getCloneView(sourceView, config);

		String querySpec = targetView.getQuerySpec();
		querySpec = querySpec.replaceAll("\\[", "\"");
		querySpec = querySpec.replaceAll("\\]", "\"");
		targetView.setQuerySpec(querySpec);

		return targetView;
	}

	/**
	 * return real data type for a user defined data type
	 * 
	 * @param supportedDataType Map<String, List<DataType>>
	 * @param srcDataType String
	 * @return String
	 */
	protected String getRealDataType(Map<String, List<DataType>> supportedDataType,
			String srcDataType) {
		DataType dataType = supportedDataType.get(srcDataType).get(0);
		return dataType.getRealTypeName();
	}

	/**
	 * adjust default value of a column
	 * 
	 * @param sourceColumn Column
	 * @param cubridColumn Column
	 */
	protected void adjustDefaultValue(Column sourceColumn, Column cubridColumn) {
		String defaultValue = sourceColumn.getDefaultValue();

		if (defaultValue == null) {
			cubridColumn.setDefaultValue(null);
			return;
		}

		// ex: N'' or N'value'
		if (isUnicodeConstant(defaultValue)) {
			cubridColumn.setDefaultIsExpression(false);
			if ("N''".equalsIgnoreCase(defaultValue)) {
				cubridColumn.setDefaultValue("''");
				return;
			} else {
				int beginIndex = 2;
				int endIndex = defaultValue.length() - 1;
				String extractedDefaultValue = defaultValue.substring(beginIndex, endIndex);
				cubridColumn.setDefaultValue(extractedDefaultValue);
				return;
			}
		}
		
		String trimValue = defaultValue.trim();
		if (trimValue.startsWith("(") && trimValue.endsWith(")")) {
			defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
		}

		// ex:(NULL)
		if ("NULL".equals(defaultValue)) {
			cubridColumn.setDefaultValue(null);
			return;
		}

		// ex: ('0000-00-00 00:00:00')
		if (trimValue.startsWith("'") && trimValue.endsWith("'") && defaultValue.length() > 2) {
			defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
		} else if (defaultValue.startsWith("(") && defaultValue.endsWith(")")) {
			// ex: ((0))
			defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
		}

		final FormatDataResult format = CUBRIDFormator.format(cubridColumn.getDataTypeInstance(), defaultValue);
		if (format.success) {
			cubridColumn.setDefaultValue(format.getFormatResult());
		} else {
			cubridColumn.setDefaultValue(null);
		}

	}

	/**
	 * Checks that a default value is Unicode Constant
	 * 
	 * @param defaultValue
	 * @return
	 */
	private boolean isUnicodeConstant(String defaultValue) {
	    return defaultValue.startsWith("N'") && defaultValue.endsWith("'");
    }

	/**
	 * Convert JDBC Object To CUBRID Object
	 * 
	 * @param config MigrationConfiguration
	 * @param recordMap used by data handler
	 * @param scc SourceColumnConfig
	 * @param srcColumn Column
	 * @param toColumn Column
	 * @param srcValue Object
	 * @return Object obj
	 */
	public Object convertValueToTargetDBValue(MigrationConfiguration config,
			Map<String, Object> recordMap, SourceColumnConfig scc, Column srcColumn,
			Column toColumn, Object srcValue) {

		if (srcValue == null) {
			return null;
		}

		String dataType = srcColumn.getDataType();

		if ("time".equals(dataType)
				&& toColumn.getJdbcIDOfDataType() == DataTypeConstant.CUBRID_DT_TIME) {
			return ((String) srcValue).substring(0, 8);
		}

		final String dt2 = "datetime2";
		if (dt2.equals(dataType)
				&& toColumn.getJdbcIDOfDataType() == DataTypeConstant.CUBRID_DT_DATETIME
				|| dt2.equals(dataType)
				&& toColumn.getJdbcIDOfDataType() == DataTypeConstant.CUBRID_DT_TIMESTAMP) {
			String datetime = (String) srcValue;
			int length = datetime.length();

			if (length > 23) {
				return datetime.substring(0, 23);
			} else {
				return datetime;
			}
		}

		final String dtos = "datetimeoffset";
		if (dtos.equals(dataType)
				&& (toColumn.getJdbcIDOfDataType() == DataTypeConstant.CUBRID_DT_DATETIME || toColumn.getJdbcIDOfDataType() == DataTypeConstant.CUBRID_DT_TIMESTAMP)) {
			try {
				//2013-11-08 11:31:00.0000000 +12:00
				String datetimeOffset = srcValue.toString();
				int length = datetimeOffset.length();
				String datetime = datetimeOffset.substring(0, 23); //2013-11-08 11:31:00.0000000
				String offsetString = datetimeOffset.substring(length - 6, length); //+12:00
				boolean signed = offsetString.startsWith("+");
				int firstColon = offsetString.indexOf(':');
				int hour = Integer.parseInt(offsetString.substring(1, firstColon));
				int minute = Integer.parseInt(offsetString.substring(firstColon + 1));
				//To MS
				long srcOffset = (hour * 60L + minute) * 60 * 1000;
				srcOffset = signed ? srcOffset : (0L - srcOffset);

				long srcTime = CUBRIDTimeUtil.parseDatetime2Long(datetime, null);
				int targetOffset = (config.targetIsOnline() ? config.getTargetDatabaseTimeZone()
						: TimeZone.getTimeZone(config.getTargetFileTimeZone())).getRawOffset();
				long newTime = srcTime - srcOffset + targetOffset;
				return new Timestamp(newTime);
			} catch (ParseException e) {
				LOG.error(LogUtil.getExceptionString(e));
				throw new NormalMigrationException(e);
			}
		}
		return super.convertValueToTargetDBValue(config, recordMap, scc, srcColumn, toColumn,
				srcValue);
	}

	/**
	 * adjust precision of a column
	 * 
	 * @param srcColumn Column
	 * @param cubColumn Column
	 * @param config MigrationConfiguration
	 */
	protected void adjustPrecision(Column srcColumn, Column cubColumn, MigrationConfiguration config) {
		String dtBasic = cubColumn.getSubDataType() == null ? cubColumn.getDataType()
				: cubColumn.getSubDataType();
		long expectedPrecision = (long) cubColumn.getPrecision();

		CUBRIDDataTypeHelper cubDTHelper = CUBRIDDataTypeHelper.getInstance(null);
		//MSSQLDataTypeHelper dtSrcHelper = MSSQLDataTypeHelper.getInstance(null);
		// 3. get CUBRID precision
		if (cubDTHelper.isString(dtBasic)) {
			long maxValue = DataTypeConstant.CUBRID_MAXSIZE;
			expectedPrecision = Math.min(maxValue, expectedPrecision);
			cubColumn.setPrecision((int) expectedPrecision);
		} else if (cubDTHelper.isStrictNumeric(dtBasic)
				&& expectedPrecision > DataTypeConstant.NUMERIC_MAX_PRECISIE_SIZE) {
			DataTypeInstance dti = new DataTypeInstance();
			dti.setName(DataTypeConstant.CUBRID_VARCHAR);
			dti.setPrecision((int) expectedPrecision + 2);
			cubColumn.setDataTypeInstance(dti);
			cubColumn.setJdbcIDOfDataType(DataTypeConstant.CUBRID_DT_VARBIT);
		} else if (cubDTHelper.isBinary(cubColumn.getDataType())) {
			int maxValue = DataTypeConstant.CUBRID_MAXSIZE;
			expectedPrecision = expectedPrecision * 8;
			expectedPrecision = Math.min(expectedPrecision, maxValue);
			cubColumn.setPrecision((int) expectedPrecision);
		}
	}

	//	/**
	//	 * transform to cubrid partition ddl
	//	 * 
	//	 * @param table Table
	//	 * @return CUBRID partition ddl
	//	 */
	//	public String getToCUBRIDPartitionDDL(Table table) {
	//		if (table == null) {
	//			return null;
	//		}
	//		return CUBRIDDDLUtil.getInstance(null).getTablePartitonDDL(table);
	//	}
}
