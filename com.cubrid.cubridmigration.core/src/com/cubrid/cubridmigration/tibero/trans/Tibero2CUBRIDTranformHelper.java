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
package com.cubrid.cubridmigration.tibero.trans;

import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.common.CommonUtils;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.PartitionTable;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;
import com.cubrid.cubridmigration.core.mapping.model.VerifyInfo;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.cubrid.trans.ToCUBRIDDataConverterFacade;
import com.cubrid.cubridmigration.mysql.trans.MySQL2CUBRIDMigParas;

public class Tibero2CUBRIDTranformHelper extends
		DBTransformHelper {

	public Tibero2CUBRIDTranformHelper(AbstractDataTypeMappingHelper dataTypeMapping,
			ToCUBRIDDataConverterFacade cf) {
		super(dataTypeMapping, cf);
	}

	/**
	 * adjust precision of a column
	 * 
	 * @param srcColumn Column
	 * @param cubColumn Column
	 * @param config MigrationConfiguration
	 */
	protected void adjustPrecision(Column srcColumn, Column cubColumn, MigrationConfiguration config) {
		//UROWID/ROWID/INTERVAL to character varying doesn't need adjust precision
		final String srcDataType = srcColumn.getDataType();
		if ("UROWID".equals(srcDataType) || "ROWID".equals(srcDataType)
				|| srcDataType.indexOf("INTERVAL YEAR") > -1
				|| srcDataType.indexOf("INTERVAL DAY") > -1) {
			return;
		}
		CUBRIDDataTypeHelper cubDTHelper = CUBRIDDataTypeHelper.getInstance(null);
		long expectedPrecision = (long) cubColumn.getPrecision();
		if (cubDTHelper.isStrictNumeric(cubColumn.getDataType())) {
			Integer tarScale = cubColumn.getScale();
			int scale = tarScale == null ? 0 : tarScale;
			if (scale < 0) {
				expectedPrecision = expectedPrecision + Math.abs(scale);
				scale = 0;
				cubColumn.setScale(scale);
			}
			if (scale > expectedPrecision) {
				expectedPrecision = scale;
			}
			if (expectedPrecision <= DataTypeConstant.NUMERIC_MAX_PRECISIE_SIZE) {
				cubColumn.setPrecision((int) expectedPrecision);
				return;
			}
			if (scale == 0) {
				expectedPrecision = expectedPrecision + 1;
			} else if (scale < expectedPrecision) {
				expectedPrecision = expectedPrecision + 2;
			} else if (scale == expectedPrecision) {
				expectedPrecision = expectedPrecision + 3;
			}
			DataTypeInstance dti = new DataTypeInstance();
			dti.setName(DataTypeConstant.CUBRID_VARCHAR);
			dti.setPrecision((int) expectedPrecision);
			dti.setScale(null);

			cubColumn.setDataTypeInstance(dti);
			cubColumn.setJdbcIDOfDataType(DataTypeConstant.CUBRID_DT_VARCHAR);
			return;
		}
		if (cubDTHelper.isBinary(cubColumn.getDataType())) {
			expectedPrecision = Math.min(expectedPrecision * 8, DataTypeConstant.CUBRID_MAXSIZE);
			cubColumn.setPrecision((int) expectedPrecision);
		}
	}

	/**
	 * return a cloned target view
	 * 
	 * @param sourceView View source view
	 * @param config MigrationConfiguration
	 * @return View
	 */
	public View getCloneView(View sourceView, MigrationConfiguration config) {
		View targetView = super.getCloneView(sourceView, config);

		String querySpec = targetView.getQuerySpec();
		String withReadOnly = "with read only";
		int index = querySpec.toLowerCase(Locale.ENGLISH).indexOf(withReadOnly);

		if (index != -1) {
			querySpec = querySpec.substring(0, index)
					+ querySpec.substring(index + withReadOnly.length(), querySpec.length());
		}

		targetView.setQuerySpec(querySpec);

		return targetView;
	}

	/**
	 * get CUBRID column from source column if scale < 0 and |scale|+
	 * |Precision| > 38 convert it to varchar(|scale|+ |Precision|+1) if scale >
	 * Precision and scale > 38 convert it to varchar|scale+ 3)
	 * 
	 * if scale < 0 and |scale|+ |Precision| <= 38
	 * numeric(|scale|+|Precision|,0) if scale > Precision and scale <= 38
	 * convert it to numeric(scale,scale) In convention of column, additional
	 * information is needed like database charset, timezone and so on, these
	 * information is stored in Catalog object.
	 * 
	 * @param srcColumn Column
	 * @param config MigrationConfiguration
	 * @return Column
	 */
	public Column getCUBRIDColumn(Column srcColumn, MigrationConfiguration config) {
		Column cubCol = srcColumn.cloneCol();

		String srcDataType = srcColumn.getDataType();
		Integer srcPrecision = srcColumn.getPrecision();
		Integer srcScale = srcColumn.getScale();

		MapObject mapping = getDataTypeMapping(srcColumn, srcDataType, srcPrecision, srcScale,
				config.getSrcCatalog().getSupportedDataType());

		String cubridDataType = mapping.getDatatype();
		String elemDataType = null;
		int index = cubridDataType.indexOf("(");
		if (index != -1) {
			elemDataType = cubridDataType.substring(index + 1, cubridDataType.length() - 1);
			cubridDataType = cubridDataType.substring(0, index);
		}

		cubCol.setDataType(cubridDataType);
		cubCol.setSubDataType(elemDataType);
		CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
		Integer dataTypeID = dataTypeHelper.getCUBRIDDataTypeID(cubridDataType);
		cubCol.setJdbcIDOfDataType(dataTypeID);

		Integer elementDataTypeID = elemDataType == null ? null
				: dataTypeHelper.getCUBRIDDataTypeID(elemDataType);
		cubCol.setJdbcIDOfSubDataType(elementDataTypeID);
		//if char is char , add '' to default value 
		if (dataTypeHelper.isString(cubCol.getDataType())
				&& StringUtils.isNotEmpty(cubCol.getDefaultValue())
				&& !cubCol.getDefaultValue().startsWith("'")) {
			cubCol.setDefaultValue("'" + cubCol.getDefaultValue() + "'");
		}
		initPecisionScale(mapping, srcColumn, cubCol);

		String nOrPValue = mapping.getPrecision();

		if ("n".equals(nOrPValue) || "p".equals(nOrPValue) || nOrPValue != null) {
			adjustPrecision(srcColumn, cubCol, config);
		}

		adjustDefaultValue(srcColumn, cubCol);

		String dataType = cubCol.getDataType();
		String defaultValue = cubCol.getDefaultValue();
		Integer scale = cubCol.getScale();

		if (!dataTypeHelper.isSupportAutoIncr(dataType, defaultValue, scale)) {
			cubCol.setAutoIncrement(false);
		}

		if ("fbo".equalsIgnoreCase(dataType)) {
			cubCol.setDataType("blob");
		}
		cubCol.setShownDataType(dataTypeHelper.getShownDataType(cubCol));
		dataTypeHelper.setColumnDataType(cubCol.getShownDataType(), cubCol);
		return cubCol;
	}

	/**
	 * 
	 * verify the char length
	 * 
	 * @param sourceColumn Column
	 * @param targetColumn Column
	 * @param config MigrationConfiguration
	 * @return VerifyInfo
	 */
	protected VerifyInfo validateChar(Column sourceColumn, Column targetColumn,
			MigrationConfiguration config) {
		VerifyInfo info = new VerifyInfo(VerifyInfo.TYPE_NO_MATCH, "");
		CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
		if (dataTypeHelper.isString(targetColumn.getDataType())) {
			int sourcePrecision = sourceColumn.getPrecision();
			int targetPrecision = targetColumn.getPrecision();
			int needPrecision = config.getCharsetFactor() * sourcePrecision;

			if (targetPrecision < needPrecision) {
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"ERROR: The target precision should equal or greater than " + needPrecision);
			} else {
				// if success
				info = new VerifyInfo(VerifyInfo.TYPE_MATCH, "");
			}
		} else if (dataTypeHelper.isNString(targetColumn.getDataType())) {
			int sourcePrecision = sourceColumn.getPrecision();
			int targetPrecision = targetColumn.getPrecision();
			if (targetPrecision < sourcePrecision) {
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"ERROR: The target precision should equal or greater than "
								+ sourcePrecision);
			} else {
				// if success
				info = new VerifyInfo(VerifyInfo.TYPE_MATCH, "");
			}
		}

		return info;
	}

	/**
	 * get the precision when Numeric convert to varchar
	 * 
	 * @param sourceColumn Column
	 * @return Integer
	 */
	private Integer getPrecisionOfNumericToVarchar(Column sourceColumn) {
		int srcScale = sourceColumn.getScale();
		int srcPrecision = sourceColumn.getPrecision();
		if (srcScale < 0) {
			return Math.abs(srcScale) + Math.abs(srcPrecision) + 1;
		} else if (srcScale > srcPrecision && srcScale > 38) {
			return Math.abs(srcScale) + 3;
		} else if ("INTEGER".equalsIgnoreCase(sourceColumn.getDataType())) {
			return DataTypeConstant.TIBERO_INTEGERTOVARCHAR_MINSIZE;
		}
		return getNumericToCharLength(sourceColumn);
	}

	/**
	 * adjust default value of a column
	 * 
	 * @param srcColumn Column
	 * @param cubridColumn Column
	 */
	protected void adjustDefaultValue(Column srcColumn, Column cubridColumn) {
		String dataType = srcColumn.getDataType();
		String defaultValue = srcColumn.getDefaultValue();
		if (defaultValue == null) {
			return;
		}

		if ((dataType.indexOf("TIMESTAMP") > -1 || "DATE".equalsIgnoreCase(dataType))
				&& defaultValue.toLowerCase(Locale.US).startsWith("sysdate")) {
			cubridColumn.setDefaultValue("CURRENT_TIMESTAMP");
			return;
		}

		if (isDefaultValueExpression(defaultValue)) {
			defaultValue = convertFunctionInDefaultValue(defaultValue, cubridColumn.getDataType());
			cubridColumn.setDefaultIsExpression(true);
			cubridColumn.setDefaultValue(defaultValue);
			return;
		}

		if ("TIMESTAMP".equalsIgnoreCase(dataType)) {
			try {
				CUBRIDTimeUtil.parseDatetime2Long(defaultValue, TimeZone.getDefault());
			} catch (Exception e) {
				String timestampValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP);
				Timestamp replacedTimestamp = MySQL2CUBRIDMigParas.getReplacedTimestamp(
						timestampValue, TimeZone.getDefault());

				if (replacedTimestamp == null) {
					cubridColumn.setDefaultValue(null);
				} else {
					String formatValue = CUBRIDTimeUtil.defaultFormatDateTime(replacedTimestamp);
					cubridColumn.setDefaultValue(formatValue);
				}
			}
		} else if ("DATE".equalsIgnoreCase(dataType)) {
			// if there is "0000-00-00" in time field, for example
			try {
				CUBRIDTimeUtil.parseDatetime2Long(defaultValue, TimeZone.getDefault());
			} catch (Exception e) {
				try {
					CUBRIDTimeUtil.parseDate2Long(defaultValue, TimeZone.getDefault());
				} catch (Exception e2) {
					String timeValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_DATE);
					cubridColumn.setDefaultValue(timeValue);
				}
			}
		}
	}
	
	/**
	 * If there is no matched condition case, returns defaultValue as it is.
	 * 
	 * @param defaultValue
	 * @param dataType
	 * @return
	 */
	private String convertFunctionInDefaultValue(String defaultValue, String dataType) {
		String lowerCaseDefaultValue = defaultValue.toLowerCase(Locale.US);
		
		if ("datetime".equalsIgnoreCase(dataType) && lowerCaseDefaultValue.startsWith("to_date")) {
			return lowerCaseDefaultValue.replaceFirst("to_date", "to_datetime");
		}
		
		return defaultValue;
	}
	
	/**
	 * isDefaultValueExpression
	 * @param defaultValue
	 * @return
	 */
	private boolean isDefaultValueExpression(String defaultValue) {
		String lowerCaseDefaultValue = defaultValue.toLowerCase(Locale.US);
		
		// Function names should be lowerCases
		String[] functions = { 
				"to_char",
				"to_date"
		};
		
		for (String function : functions) {
			if (lowerCaseDefaultValue.startsWith(function)) {
				return true;
			}
		}

		return false;
	}
	
	/**
	 * 
	 * verify the length that numeric convert to varchar
	 * 
	 * @param sourceColumn Column
	 * @param targetColumn Column
	 * @param config MigrationConfiguration
	 * @return VerifyInfo
	 */
	protected VerifyInfo validateNumericToVarchar(Column sourceColumn, Column targetColumn,
			MigrationConfiguration config) {
		Integer expectPrecision = getPrecisionOfNumericToVarchar(sourceColumn);
		if (targetColumn.getPrecision() < expectPrecision) {
			return new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
					"The precision should equal or greater than: " + expectPrecision);

		}
		return new VerifyInfo(VerifyInfo.TYPE_MATCH, "");
	}

	/**
	 * Turn the DDL of source db to CUBRID DDL. For example: MySQL select `test`
	 * from `code` will be turned to CUBRID select "test" from "code"
	 * 
	 * @param table Source DDL
	 * @return CUBRID DDL
	 */
	public String getToCUBRIDPartitionDDL(Table table) {
		if (table == null || table.getPartitionInfo() == null) {
			return null;
		}

		String srcPartitionDDL = table.getPartitionInfo().getDDL();
		PartitionInfo partInfo = table.getPartitionInfo();
		String partitionMethod = partInfo.getPartitionMethod();
		int partitionColumnCount = partInfo.getPartitionColumnCount();
		int partitionCount = partInfo.getPartitionCount();
		List<Column> partitionColumns = partInfo.getPartitionColumns();
		List<PartitionTable> partitions = partInfo.getPartitions();

		if (partitionColumnCount == 0 || partitionCount == 0) {
			return srcPartitionDDL;
		}
		StringBuilder ddl = new StringBuilder();
		ddl.append("PARTITION BY ");
		if (PartitionInfo.PARTITION_METHOD_RANGE.equalsIgnoreCase(partitionMethod)) {
			ddl.append(" RANGE ");
		} else if (PartitionInfo.PARTITION_METHOD_LIST.equalsIgnoreCase(partitionMethod)) {
			ddl.append(" LIST ");
		} else if (PartitionInfo.PARTITION_METHOD_HASH.equalsIgnoreCase(partitionMethod)) {
			ddl.append(" HASH ");
		} else {
			return srcPartitionDDL;
		}
		ddl.append("(");
		for (int i = 0; i < partitionColumnCount; i++) {
			Column column = partitionColumns.get(i);
			String colName = column.getName();
			if (i > 0) {
				ddl.append(",");
			}
			ddl.append(colName);
		}
		ddl.append(") ");

		if (PartitionInfo.PARTITION_METHOD_HASH.equalsIgnoreCase(partitionMethod)) {
			ddl.append(" PARTITIONS ").append(partitionCount);
		} else {
			ddl.append("(").append(CommonUtils.newLine);
			for (int i = 0; i < partitionCount; i++) {
				PartitionTable partTable = partitions.get(i);
				if (i > 0) {
					ddl.append(",").append(CommonUtils.newLine);
				}
				ddl.append("PARTITION ").append(partTable.getPartitionName());
				if (PartitionInfo.PARTITION_METHOD_RANGE.equalsIgnoreCase(partitionMethod)) {
					ddl.append(" VALUES LESS THAN ");
					if ("MAXVALUE".equalsIgnoreCase(partTable.getPartitionDesc())) {
						ddl.append(partTable.getPartitionDesc());
					} else {
						ddl.append("(");
						ddl.append(partTable.getPartitionDesc());
						ddl.append(")");
					}
				} else if (PartitionInfo.PARTITION_METHOD_LIST.equalsIgnoreCase(partitionMethod)) {
					ddl.append(" VALUES IN (");
					ddl.append(partTable.getPartitionDesc());
					ddl.append(")");
				}

			}
			ddl.append(CommonUtils.newLine).append(")");
		}
		return ddl.toString();
	}

	@Override
	public String getGraphDataType(Column col, MigrationConfiguration cfg) {
		// TODO Auto-generated method stub
		return null;
	}
}
