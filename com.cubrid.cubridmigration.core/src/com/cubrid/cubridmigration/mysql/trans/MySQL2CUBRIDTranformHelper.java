/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
 * Solution.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 */
package com.cubrid.cubridmigration.mysql.trans;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.VerifyInfo;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.cubrid.trans.ToCUBRIDDataConverterFacade;
import com.cubrid.cubridmigration.mysql.MySQLDataTypeHelper;

/**
 * a transform class which helps to data transform in migration of MySQL to
 * CUBRID
 * 
 * @author moulinwang,Kevin.Wang
 * 
 */
public class MySQL2CUBRIDTranformHelper extends
		DBTransformHelper {
	private static final Logger LOG = LogUtil.getLogger(MySQL2CUBRIDTranformHelper.class);

	String[][] mysqlValue2CUBRIDValue = {{"CURRENT_DATE", "CURRENT_DATE"},
			{"CURDATE", "CURRENT_DATE"}, {"CURRENT_TIME", "CURRENT_TIME"},
			{"CURTIME", "CURRENT_TIME"}, {"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
			{"NOW", "CURRENT_TIMESTAMP"}};

	public MySQL2CUBRIDTranformHelper(AbstractDataTypeMappingHelper dataTypeMapping,
			ToCUBRIDDataConverterFacade cf) {
		super(dataTypeMapping, cf);
	}

	/**
	 * adjustPrecision
	 * 
	 * @param srcColumn Column
	 * @param cubridColumn Column
	 * @param config MigrationConfiguration
	 */
	protected void adjustPrecision(Column srcColumn, Column cubridColumn,
			MigrationConfiguration config) {
		String dtBasic = cubridColumn.getSubDataType() == null ? cubridColumn.getDataType()
				: cubridColumn.getSubDataType();

		// get CUBRID precision
		long expectedPrecision = (long) cubridColumn.getPrecision();
		MySQLDataTypeHelper dtHelper = MySQLDataTypeHelper.getInstance(null);
		CUBRIDDataTypeHelper cubDTHelper = CUBRIDDataTypeHelper.getInstance(null);
		if ((!dtHelper.isNumeric(srcColumn.getDataType())) && cubDTHelper.isString(dtBasic)) {
			expectedPrecision = Math.min(expectedPrecision, DataTypeConstant.CUBRID_MAXSIZE);
			cubridColumn.setPrecision((int) expectedPrecision);
			return;
		}
		if (cubDTHelper.isStrictNumeric(cubridColumn.getDataType())) {
			cubridColumn.setPrecision((int) Math.min(expectedPrecision,
					DataTypeConstant.NUMERIC_MAX_PRECISIE_SIZE));
			return;
		}
		if (cubDTHelper.isBinary(cubridColumn.getDataType())) {
			if ("bit".equals(srcColumn.getDataType())) {
				final long factor = expectedPrecision % 8;
				expectedPrecision = factor == 0 ? expectedPrecision
						: (expectedPrecision - factor + 8);
				cubridColumn.setPrecision((int) expectedPrecision);
				return;
			}
			expectedPrecision = Math.min(expectedPrecision * 8, DataTypeConstant.CUBRID_MAXSIZE);
			cubridColumn.setPrecision((int) expectedPrecision);
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
		targetView.setQuerySpec(querySpec);
		targetView.setDDL("CREATE VIEW \"" + targetView.getName() + "\" AS " + querySpec);
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
		if ("SET UNSIGNED".equals(srcDataType)) {
			return "SET";
		}

		return super.getRealDataType(supportedDataType, srcDataType);
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

		for (String[] entry : mysqlValue2CUBRIDValue) {
			if (defaultValue.equalsIgnoreCase(entry[0])) {
				cubridColumn.setDefaultValue(entry[1]);
				return;
			}
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

		} else if ("DATETIME".equalsIgnoreCase(dataType)) {
			// if there is "0000-00-00 00:00:00" in time field, for example,
			// return null
			try {
				CUBRIDTimeUtil.parseDatetime2Long(defaultValue, TimeZone.getDefault());
			} catch (Exception e) {
				String timestampValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP);
				Timestamp replacedTimestamp = MySQL2CUBRIDMigParas.getReplacedTimestamp(
						timestampValue, TimeZone.getDefault());
				if (replacedTimestamp == null) {
					cubridColumn.setDefaultValue(null);
				} else {
					String formatValue = CUBRIDTimeUtil.defaultFormatMilin(replacedTimestamp);
					cubridColumn.setDefaultValue(formatValue);
				}
			}

		} else if ("TIME".equalsIgnoreCase(dataType)) {
			// if there is "-838:59:59" in time field, for example
			try {
				CUBRIDTimeUtil.parseTime2Long(defaultValue, TimeZone.getDefault());
			} catch (Exception e) {
				String timeValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_TIME);
				cubridColumn.setDefaultValue(timeValue);
			}

		} else if ("DATE".equalsIgnoreCase(dataType)) {
			// if there is "0000-00-00" in time field, for example
			try {
				CUBRIDTimeUtil.parseDate2Long(defaultValue, TimeZone.getDefault());
			} catch (Exception e) {
				String timeValue = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_DATE);
				cubridColumn.setDefaultValue(timeValue);
			}
		}

	}

	/**
	 * verify the mysql set type
	 * 
	 * @param sourceColumn Column
	 * @param targetColumn Column
	 * @param config MigrationConfiguration
	 * @return VerifyInfo;
	 */
	protected VerifyInfo validateCollection(Column sourceColumn, Column targetColumn,
			MigrationConfiguration config) {
		VerifyInfo info = new VerifyInfo(VerifyInfo.TYPE_MATCH, "");

		// here set type only transform to set_of(character varying(255))
		Column tempColumn = getCUBRIDColumn(sourceColumn, config);

		// Get the type id
		int targetColumnTypeID = targetColumn.getJdbcIDOfDataType();
		int tempColumnTypeID = tempColumn.getJdbcIDOfDataType();
		Integer targetColumnSubTypeID = targetColumn.getJdbcIDOfSubDataType();
		Integer tempColumnSubTypeID = tempColumn.getJdbcIDOfSubDataType();

		// check the type and precision
		if (targetColumnTypeID == tempColumnTypeID && targetColumnSubTypeID != null
				&& targetColumnSubTypeID.equals(tempColumnSubTypeID)) {
			if (targetColumn.getPrecision() < tempColumn.getPrecision()) {
				info.setResult(VerifyInfo.TYPE_NOENOUGH_LENGTH);
				info.setMessage("The precision should equal or greater than:"
						+ tempColumn.getPrecision());

				LOG.info("The precision should equal or greater than:" + tempColumn.getPrecision());
			}
		} else {
			info.setResult(VerifyInfo.TYPE_NO_MATCH);
			info.setMessage(targetColumn.getSubDataType() + "\t can't transform to"
					+ tempColumn.getSubDataType() + "for Set");

			LOG.info(targetColumn.getDataType() + "\t can't transform to"
					+ tempColumn.getDataType() + "for Set");
		}
		return info;
	}

	/**
	 * Convert Jdbc Object To Cubrid Object
	 * 
	 * @param config MigrationConfiguration
	 * @param recordMap the column name to value map.
	 * @param scc SourceColumnConfig
	 * @param srcColumn Column
	 * @param toColumn Column
	 * @param sourceValue Object
	 * @return Object obj @ e
	 */

	public Object convertValueToTargetDBValue(MigrationConfiguration config,
			Map<String, Object> recordMap, SourceColumnConfig scc, Column srcColumn,
			Column toColumn, Object sourceValue) {

		if (sourceValue instanceof String) {
			String strValue = (String) sourceValue;

			String replacedChar = MySQL2CUBRIDMigParas.getMigrationParamter(MySQL2CUBRIDMigParas.REPLAXE_CHAR0);
			if (replacedChar.length() == 3) {
				replacedChar = replacedChar.substring(1, replacedChar.length() - 1);
			}

			return super.convertValueToTargetDBValue(config, recordMap, scc, srcColumn, toColumn,
					strValue.replaceAll("\0", replacedChar));
		}

		return super.convertValueToTargetDBValue(config, recordMap, scc, srcColumn, toColumn,
				sourceValue);
	}

	/**
	 * Turn the DDL of source db to CUBRID DDL. For example: MySQL select `test`
	 * from `code` will be turned to CUBRID select "test" from "code"
	 * 
	 * @param ddl Source DDL
	 * @return CUBRID DDL
	 */
	public String getFitTargetFormatSQL(String ddl) {
		String result = ddl == null ? "" : ddl.replace("`", "\"").replace("`", "\"");
		return result.replaceAll("\\s*ENGINE\\s*=\\s*\\w+", "");
	}

	@Override
	public String getGraphDataType(Column col, MigrationConfiguration cfg) {
		// TODO Auto-generated method stub
		return null;
	}
}
