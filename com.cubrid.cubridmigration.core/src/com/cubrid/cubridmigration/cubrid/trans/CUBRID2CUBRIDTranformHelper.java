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
package com.cubrid.cubridmigration.cubrid.trans;

import java.util.Locale;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.VerifyInfo;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;

/**
 * a transform class which helps to data transform in migration of CUBRID to
 * CUBRID
 * 
 * @author moulinwang,Kevin.Wang
 * 
 */
public class CUBRID2CUBRIDTranformHelper extends
		DBTransformHelper {

	private static final Logger LOG = LogUtil.getLogger(CUBRID2CUBRIDTranformHelper.class);

	public CUBRID2CUBRIDTranformHelper(
			AbstractDataTypeMappingHelper dataTypeMapping) {
		super(dataTypeMapping, ToCUBRIDDataConverterFacade.getIntance());
	}

	/**
	 * Adjust column data type's precision
	 * 
	 * @param srcColumn Column
	 * @param cubColumn Column
	 * @param config MigrationConfiguration
	 */
	protected void adjustPrecision(Column srcColumn, Column cubColumn,
			MigrationConfiguration config) {
		//CUBRID to CUBRID, it does not need to adjust.

		//		String dtBasic = cubColumn.getSubDataType() == null ? cubColumn.getDataType()
		//				: cubColumn.getSubDataType();
		//		String sourceDataType = srcColumn.getDataType();
		//
		//		CUBRIDDataTypeHelper cubDTHelper = CUBRIDDataTypeHelper.getInstance(null);
		//		// get CUBRID precision
		//		long expectedPrecision = (long) cubColumn.getPrecision();
		//		if (cubDTHelper.isString(dtBasic)
		//				&& cubDTHelper.isString(sourceDataType)) {
		//			cubColumn.setPrecision((int) expectedPrecision);
		//			return;
		//		}
		//		if (cubDTHelper.isStrictNumeric(cubColumn.getDataType())) {
		//			cubColumn.setPrecision((int) expectedPrecision);
		//			if (expectedPrecision <= DataTypeConstant.NUMERIC_MAX_PRECISIE_SIZE) {
		//				cubColumn.setPrecision((int) expectedPrecision);
		//			} else {
		//				cubColumn.setDataType(DataTypeConstant.CUBRID_VARCHAR);
		//				cubColumn.setJdbcIDOfDataType(DataTypeConstant.CUBRID_DT_VARCHAR);
		//				cubColumn.setPrecision((int) expectedPrecision + 2);
		//				cubColumn.setScale(null);
		//			}
		//			return;
		//		}
		//		if (cubDTHelper.isBinary(cubColumn)) {
		//			if (cubDTHelper.isBinary(srcColumn)) {
		//				cubColumn.setPrecision((int) expectedPrecision);
		//				return;
		//			}
		//			final long tmpPre = expectedPrecision % 8;
		//			if (tmpPre > 0) {
		//				cubColumn.setPrecision((int) (expectedPrecision - tmpPre + 8));
		//				return;
		//			}
		//		}
		//		cubColumn.setPrecision((int) expectedPrecision);
	}

	/**
	 * validate Collection data type
	 * 
	 * @param sourceColumn Column
	 * @param targetColumn Column
	 * @param config MigrationConfiguration
	 * @return VerifyInfo
	 */
	protected VerifyInfo validateCollection(Column sourceColumn,
			Column targetColumn, MigrationConfiguration config) {
		VerifyInfo info = new VerifyInfo(VerifyInfo.TYPE_MATCH, "");
		CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
		int sourceColumnTypeId = dataTypeHelper.getCUBRIDDataTypeID(sourceColumn.getDataType());
		int targetColumnTypeId = dataTypeHelper.getCUBRIDDataTypeID(targetColumn.getDataType());

		if (sourceColumnTypeId == targetColumnTypeId) {
			//verify the subtype of the collection
			Column subSColumn = sourceColumn.cloneCol();
			subSColumn.setDataType(sourceColumn.getSubDataType());

			Column subTColumn = targetColumn.cloneCol();
			subTColumn.setDataType(targetColumn.getSubDataType());

			return super.verifyColumnDataType(subSColumn, subTColumn, config);
		} else {
			info.setResult(VerifyInfo.TYPE_NO_MATCH);
			String message = sourceColumn.getDataType()
					+ "\t can't transform to" + targetColumn.getDataType();
			info.setMessage(message);

			LOG.info(message);
		}
		return info;
	}

	/**
	 * judge is collection type
	 * 
	 * @param type String
	 * @return boolean
	 */
	protected boolean isCollection(String type) {
		if (type.toLowerCase(Locale.ENGLISH).indexOf("set") >= 0
				|| type.toLowerCase(Locale.ENGLISH).indexOf("sequence") >= 0) {
			return true;
		}
		return false;
	}

	/**
	 * verify the char column same database compare char's length don't use
	 * charset
	 * 
	 * @param sourceColumn Column
	 * @param targetColumn Column
	 * @param config MigrationConfiguration
	 * @return VerifyInfo
	 */
	protected VerifyInfo validateChar(Column sourceColumn, Column targetColumn,
			MigrationConfiguration config) {
		VerifyInfo info = null;

		int sourcePrecision = sourceColumn.getPrecision();
		int targetPrecision = targetColumn.getPrecision();

		if (sourcePrecision > targetPrecision) {
			LOG.info("ERROR: The target precision should equal or greater than "
					+ targetPrecision);
			info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
					"ERROR: The target precision should equal or greater than "
							+ targetPrecision);
			return info;
		}

		// if success
		info = new VerifyInfo(VerifyInfo.TYPE_MATCH, "");
		return info;
	}

}
