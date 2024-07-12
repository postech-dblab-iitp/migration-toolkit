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
package com.cubrid.cubridmigration.core.trans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.TableOrView;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.UserDefinedDataHandlerManager;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.MapItem;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;
import com.cubrid.cubridmigration.core.mapping.model.VerifyInfo;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.trans.ToCUBRIDDataConverterFacade;
import com.cubrid.cubridmigration.graph.trans.ToNeo4jDataConverterFacade;
import com.cubrid.cubridmigration.graph.trans.ToTiberoDataConverterFacade;
import com.cubrid.cubridmigration.graph.trans.ToTurboDataConverterFacade;

/**
 * a class help to transform database data
 * 
 * @author moulinwang
 * 
 */
public abstract class DBTransformHelper {

	private static final Logger LOG = LogUtil.getLogger(DBTransformHelper.class);

	protected final AbstractDataTypeMappingHelper dataTypeMappingHelper;

	protected final IDataConvertorFacade convertFactory;

	protected DBTransformHelper(AbstractDataTypeMappingHelper dataTypeMapping,
			ToCUBRIDDataConverterFacade cf) {
		this.dataTypeMappingHelper = dataTypeMapping;
		this.convertFactory = cf;
	}

	protected DBTransformHelper(AbstractDataTypeMappingHelper dataTypeMapping,
			ToNeo4jDataConverterFacade toNeo4jDataConverterFacade) {
		this.dataTypeMappingHelper = dataTypeMapping;
		this.convertFactory = toNeo4jDataConverterFacade;
	}

	protected DBTransformHelper(AbstractDataTypeMappingHelper dataTypeMapping,
			ToTurboDataConverterFacade toTurboDataConverterFacade) {
		this.dataTypeMappingHelper = dataTypeMapping;
		this.convertFactory = toTurboDataConverterFacade;
	}

	protected DBTransformHelper(AbstractDataTypeMappingHelper dataTypeMapping,
			ToTiberoDataConverterFacade toTiberoDataConverterFacade) {
		this.dataTypeMappingHelper = dataTypeMapping;
		this.convertFactory = toTiberoDataConverterFacade;
	}

	/**
	 * adjust precision of a column
	 * 
	 * @param srcColumn    Column
	 * @param cubridColumn Column
	 * @param config       MigrationConfiguration
	 */
	protected abstract void adjustPrecision(Column srcColumn, Column cubridColumn,
			MigrationConfiguration config);

	/**
	 * initial precision of a column
	 * 
	 * @param mappingItem  DataTypeMappingItem
	 * @param srcColumn    Column
	 * @param cubridColumn Column
	 */
	protected void initPecisionScale(MapObject mappingItem, Column srcColumn, Column cubridColumn) {
		String srcDataType = srcColumn.getDataType();
		Integer srcPrecision = srcColumn.getPrecision();
		Integer srcScale = srcColumn.getScale();

		String cubridDataType = mappingItem.getDatatype();

		// 2. set precision
		String mappingPrecision = mappingItem.getPrecision();
		String mappingScale = mappingItem.getScale();
		Integer expectedPrecision;
		// 1. initial
		if (StringUtils.isEmpty(mappingPrecision)) {
			expectedPrecision = null;
		} else if ("p".equalsIgnoreCase(mappingPrecision) || "n".equalsIgnoreCase(mappingPrecision)) {
			expectedPrecision = srcPrecision;
		} else {
			try {
				expectedPrecision = Integer.parseInt(mappingPrecision);
			} catch (NumberFormatException ignored) {
				LOG.error("Can not parse \"" + mappingPrecision
						+ "\" as a number in data type mapping(data type=" + srcDataType
						+ ",precision=" + srcPrecision + ",scale=" + srcScale + " to data type="
						+ cubridDataType + ",precision=" + mappingPrecision + ",scale="
						+ mappingScale + ")");
				expectedPrecision = 0;
			}
		}
		cubridColumn.setPrecision(expectedPrecision);
		// 3. set scale
		CUBRIDDataTypeHelper cubDTHelper = CUBRIDDataTypeHelper.getInstance(null);
		if (cubDTHelper.isStrictNumeric(cubridColumn.getDataType())) {
			String sValue = mappingScale == null ? "" : mappingScale;

			Integer expectedScale = srcScale;

			if ("s".equalsIgnoreCase(sValue)) {
				expectedScale = srcScale;
			} else if (StringUtils.isBlank(sValue)) {
				expectedScale = 0;
			} else {
				try {
					expectedScale = Integer.parseInt(sValue);
				} catch (NumberFormatException ignored) {
					LOG.error("Can not parse \"" + sValue
							+ "\" as a number in data type mapping(data type=" + srcDataType
							+ ",precision=" + srcPrecision + ",scale=" + srcScale
							+ " to data type=" + cubridDataType + ",precision=" + mappingPrecision
							+ ",scale=" + mappingScale + ")");
				}
			}
			cubridColumn.setScale(expectedScale);
		} else {
			cubridColumn.setScale(null);
		}
	}

	/**
	 * get CUBRID column from source column, which may be Oracle, MySQL, CUBRID
	 * and so on.
	 * 
	 * For it is used in GUI side, so effective is not necessary.
	 * 
	 * In convention of column, additional information is needed like database
	 * charset, timezone and so on, these information is stored in Catalog
	 * object.
	 * 
	 * @param srcCol Column
	 * @param config MigrationConfiguration
	 * @return Column
	 */
	public Column getCUBRIDColumn(Column srcCol, MigrationConfiguration config) {
		CUBRIDDataTypeHelper cubDTHelper = CUBRIDDataTypeHelper.getInstance(null);
		Column cubCol = srcCol.cloneCol();
		cubCol.setName(StringUtils.lowerCase(cubCol.getName()));

		String srcDataType = srcCol.getDataType();

		// CUBRID old special data type, it was deprecated in the new CUBRID database
		// version.
		if (DataTypeConstant.CUBRID_FBO.equalsIgnoreCase(srcDataType)) {
			final String glo = DataTypeConstant.CUBRID_GLO;
			DataTypeInstance dti = new DataTypeInstance();
			dti.setName(glo);
			cubCol.setDataTypeInstance(dti);
			cubCol.setJdbcIDOfDataType(cubDTHelper.getCUBRIDDataTypeID(glo));
			return cubCol;
		}

		Integer srcPrecision = srcCol.getPrecision();
		Integer srcScale = srcCol.getScale();

		// Get supported data type
		Catalog catalog = config.getSrcCatalog();
		Map<String, List<DataType>> supportedDataType = null;
		if (catalog != null) {
			supportedDataType = catalog.getSupportedDataType();
		}
		if (supportedDataType == null) {
			supportedDataType = new HashMap<String, List<DataType>>();
		}
		MapObject mapping = getDataTypeMapping(srcCol, srcDataType, srcPrecision, srcScale,
				supportedDataType);
		// if cubDataType==ENUM
		String cubDataType = mapping.getDatatype();
		if (cubDTHelper.isEnum(cubDataType)) {
			Integer dataTypeID = cubDTHelper.getCUBRIDDataTypeID(cubDataType);
			cubCol.setJdbcIDOfDataType(dataTypeID);

			cubCol.setDataType(cubDataType);
			final String elements = srcCol.getEnumElements();
			cubCol.setEnumElements(elements);
			String shownDataType = cubDataType + "(" + elements + ")";
			cubCol.setShownDataType(shownDataType);
			return cubCol;
		}

		// if is set/multiset/list
		if (cubDTHelper.isCollection(cubDataType)) {
			String mainType = cubDTHelper.getMainDataType(cubDataType);
			cubCol.setDataType(mainType);

			Integer dataTypeID = cubDTHelper.getCUBRIDDataTypeID(cubDataType);
			cubCol.setJdbcIDOfDataType(dataTypeID);

			String srcSubDataType = srcCol.getSubDataType();
			String tarSubDataType;
			if (StringUtils.isBlank(srcSubDataType)) {
				tarSubDataType = cubDTHelper.getRemain(cubDataType);
			} else {
				mapping = getDataTypeMapping(srcCol, srcSubDataType, srcPrecision, srcScale, null);
				tarSubDataType = mapping.getDatatype();
			}
			cubCol.setSubDataType(tarSubDataType);
			Integer subDataTypeId = tarSubDataType == null ? null
					: cubDTHelper.getCUBRIDDataTypeID(tarSubDataType);
			cubCol.setJdbcIDOfSubDataType(subDataTypeId);
		} else {
			String cubMainType = cubDTHelper.getMainDataType(cubDataType);
			cubCol.setDataType(cubMainType);
			Integer dataTypeID = cubDTHelper.getCUBRIDDataTypeID(cubMainType);
			cubCol.setJdbcIDOfDataType(dataTypeID);
		}
		// if char is char , add '' to default value
		// if (cubDTHelper.isString(cubCol.getDataType()) &&
		// StringUtils.isNotEmpty(defaultValue)
		// && !defaultValue.startsWith("'")) {
		// cubCol.setDefaultValue("'" + defaultValue + "'");
		// }
		// Initialize the precision and scale
		initPecisionScale(mapping, srcCol, cubCol);

		String nOrPValue = mapping.getPrecision();
		if ("n".equalsIgnoreCase(nOrPValue) || "p".equalsIgnoreCase(nOrPValue) || nOrPValue != null) {
			adjustPrecision(srcCol, cubCol, config);
		}
		adjustDefaultValue(srcCol, cubCol);

		String dataType = cubCol.getDataType();
		Integer scale = cubCol.getScale();

		String defaultValue = cubCol.getDefaultValue();
		if (!cubDTHelper.isSupportAutoIncr(dataType, defaultValue, scale)) {
			cubCol.setAutoIncrement(false);
		}
		// Update the shown data type finally.
		cubCol.setShownDataType(cubDTHelper.getShownDataType(cubCol));
		return cubCol;
	}

	/**
	 * adjust Default Value
	 * 
	 * @param srcColumn    Column
	 * @param cubridColumn Column
	 */
	protected void adjustDefaultValue(Column srcColumn, Column cubridColumn) {
		// Do nothing
	}

	/**
	 * return DataTypeMappingItem
	 * 
	 * @param srcColumn      Column
	 * @param srcDataType    String
	 * @param srcPrecision   Integer
	 * @param srcScale       Integer
	 * @param supportedTypes supported data Types
	 * @return DataTypeMappingItem
	 */
	protected MapObject getDataTypeMapping(Column srcColumn, String srcDataType,
			Integer srcPrecision, Integer srcScale, Map<String, List<DataType>> supportedTypes) {
		MapObject mapObject = dataTypeMappingHelper.getTargetFromPreference(srcDataType,
				srcPrecision, srcScale);
		// try again
		if (null == mapObject) {
			String realDataType = getRealDataType(supportedTypes, srcDataType);
			mapObject = dataTypeMappingHelper.getTargetFromPreference(realDataType, srcPrecision,
					srcScale);
		}
		// try again
		if (null == mapObject) {
			String realDataType = getRealDataType(supportedTypes, srcDataType);
			mapObject = dataTypeMappingHelper.getTargetFromPreference(realDataType);
		}

		if (null == mapObject) {
			StringBuffer bf = new StringBuffer(100);
			bf.append("Can not find data type mapping of current column(");
			final TableOrView tableOrView = srcColumn.getTableOrView();
			bf.append("database object name=").append(
					tableOrView == null ? "" : tableOrView.getName());
			bf.append(",column name=").append(srcColumn.getName());
			bf.append(",data type=").append(srcDataType);
			bf.append(",precision=").append(srcPrecision);
			bf.append(",scale=").append(srcScale).append(").");
			throw new IllegalArgumentException(bf.toString());
		}
		return mapObject;
	}

	/**
	 * return real data type for a user defined data type
	 * 
	 * @param supportedDataType Map<String, List<DataType>>
	 * @param srcDataType       String
	 * @return String
	 */
	protected String getRealDataType(Map<String, List<DataType>> supportedDataType,
			String srcDataType) {
		return srcDataType;
	}

	/**
	 * return a cloned target view
	 * 
	 * @param sourceView View source view
	 * @param config     MigrationConfiguration
	 * @return View
	 */
	public View getCloneView(View sourceView, MigrationConfiguration config) {
		View targetView = new View();
		targetView.setName(sourceView.getName());
		targetView.setQuerySpec(getFitTargetFormatSQL(sourceView.getQuerySpec()));
		targetView.setDDL(sourceView.getDDL());
		List<Column> columns = sourceView.getColumns();

		List<Column> newColumns = new ArrayList<Column>();

		for (Column column : columns) {
			try {
				Column newColumn = getCUBRIDColumn(column, config);
				newColumn.setTableOrView(targetView);
				newColumns.add(newColumn);
			} catch (Exception ex) {
				LOG.error("", ex);
			}
		}

		targetView.setColumns(newColumns);

		return targetView;
	}

	/**
	 * newTargetTable
	 * 
	 * @param stc         SourceTableConfig
	 * @param sourceTable Table
	 * @param config      MigrationConfiguration
	 * @return TargetTable
	 */
	public Table createRDBTable(SourceTableConfig stc, Table sourceTable,
			MigrationConfiguration config) {
		Table tarTable = new Table();
		tarTable.setName(stc.getTarget());
		tarTable.setReuseOID(sourceTable.isReuseOID());
		tarTable.setOwner(stc.getOwner());

		List<Column> srcColumns = sourceTable.getColumns();
		List<Column> newColumns = new ArrayList<Column>();

		// Columns
		for (Column srcColumn : srcColumns) {
			SourceColumnConfig scc = stc.getColumnConfig(srcColumn.getName());
			Column cubridColumn = getCUBRIDColumn(srcColumn, config);
			if (scc == null) {
				cubridColumn.setName(StringUtils.lowerCase(srcColumn.getName()));
			} else {
				cubridColumn.setName(scc.getTarget());
			}
			cubridColumn.setTableOrView(tarTable);
			newColumns.add(cubridColumn);
		}
		tarTable.setColumns(newColumns);

		// PK
		PK sPK = sourceTable.getPk();
		if (sPK != null) {
			PK tPK = new PK(tarTable);
			tPK.setName(StringUtils.lowerCase(sPK.getName()));
			for (String scol : sPK.getPkColumns()) {
				SourceColumnConfig scc = stc.getColumnConfig(scol);
				if (scc == null) {
					tPK.addColumn(StringUtils.lowerCase(scol));
				} else {
					tPK.addColumn(scc.getTarget());
				}
			}
			tarTable.setPk(tPK);
		}
		if (!(stc instanceof SourceEntryTableConfig)) {
			return tarTable;
		}
		SourceEntryTableConfig setc = (SourceEntryTableConfig) stc;
		// FK
		List<FK> sfks = sourceTable.getFks();
		if (CollectionUtils.isNotEmpty(sfks)) {
			for (FK sfk : sfks) {
				SourceConfig sc = setc.getFKConfig(sfk.getName());
				FK tfk = new FK(tarTable);
				Map<String, String> fkcolumns = sfk.getColumns();
				for (Map.Entry<String, String> entry : fkcolumns.entrySet()) {
					tfk.addRefColumnName(StringUtils.lowerCase(entry.getKey()),
							StringUtils.lowerCase(entry.getValue()));
				}

				String referencedTableName = sfk.getReferencedTableName();
				Map<String, Integer> allTablesCountMap = config.getSrcCatalog().getAllTablesCountMap();
				Integer tableCount = allTablesCountMap.get(referencedTableName);
				if (tableCount != null && tableCount > 1) {
					String owner = sfk.getTable().getOwner();
					tfk.setReferencedTableName(StringUtils.lowerCase(owner) + "_" + referencedTableName);
				} else {
					tfk.setReferencedTableName(referencedTableName);
				}

				if (sc == null) {
					tfk.setName(StringUtils.lowerCase(sfk.getName()));
				} else {
					tfk.setName(sc.getTarget());
				}
				// tfk.setDeferability(sfk.getDeferability());
				tfk.setDeleteRule(sfk.getDeleteRule());
				tfk.setUpdateRule(sfk.getUpdateRule());
				// tfk.setOnCacheObject(sfk.getOnCacheObject());
				tarTable.addFK(tfk);
			}
		}
		// indexes
		List<Index> sidxs = sourceTable.getIndexes();
		for (Index sidx : sidxs) {
			SourceConfig sc = setc.getIndexConfig(sidx.getName());
			Index tidx = new Index(tarTable);
			tidx.setIndexType(sidx.getIndexType());
			if (sc == null) {
				tidx.setName(StringUtils.lowerCase(sidx.getName()));
			} else {
				tidx.setName(sc.getTarget());
			}
			Map<String, Boolean> indexColumns = sidx.getIndexColumns();
			for (Map.Entry<String, Boolean> entry : indexColumns.entrySet()) {
				String key = replaceStrValue(entry.getKey());
				tidx.addColumn(key, entry.getValue());
			}
			tidx.setReverse(sidx.isReverse());
			tidx.setUnique(sidx.isUnique());
			tarTable.addIndex(tidx);
		}
		// Partitions
		if (sourceTable.getPartitionInfo() != null) {
			PartitionInfo targetPartitionInfo = new PartitionInfo();
			tarTable.setPartitionInfo(targetPartitionInfo);
			targetPartitionInfo.setDDL(getToCUBRIDPartitionDDL(sourceTable));
		}
		return tarTable;
	}

	public static String replaceStrValue(String key1) {
		String key = key1;
		String lkey = StringUtils.lowerCase(key);
		if (key.indexOf('\'') >= 0) {
			List<int[]> points = new ArrayList<int[]>();
			int[] point = null;
			for (int i = 0; i < key.length() - 1; i++) {
				if (key.charAt(i) == '\'') {
					if (point == null) {
						point = new int[2];
						point[0] = i;
					} else {
						point[1] = i;
						points.add(point);
						point = null;
					}
				}
			}
			StringBuilder sb = new StringBuilder(lkey);
			for (int[] pp : points) {
				sb.replace(pp[0], pp[1], key.substring(pp[0], pp[1]));
			}
			key = sb.toString();
		} else {
			key = lkey;
		}
		return key;
	}

	/**
	 * newTargetTable
	 * 
	 * @param srcColumn   Column
	 * @param targetTable Table
	 * @param config      MigrationConfiguration
	 * @return TargetTable
	 */
	public Column newTargetColumn(Column srcColumn, Table targetTable, MigrationConfiguration config) {
		Column cubridColumn = getCUBRIDColumn(srcColumn, config);
		String name = cubridColumn.getName();
		int indexOfDot = name.indexOf(".");
		if (indexOfDot != -1) {
			String columnName = name.substring(indexOfDot + 1, name.length());
			cubridColumn.setName(columnName);
		}
		cubridColumn.setTableOrView(targetTable);
		return cubridColumn;
	}

	/**
	 * Convert Jdbc Object To Cubrid Object
	 * 
	 * @param config    MigrationConfiguration
	 * @param recordMap used by data handler
	 * @param scc       SourceColumnConfig
	 * @param srcColumn Column
	 * @param toColumn  Column
	 * @param srcValue  Object
	 * @return Object obj
	 */
	public Object convertValueToTargetDBValue(MigrationConfiguration config,
			Map<String, Object> recordMap, SourceColumnConfig scc, Column srcColumn,
			Column toColumn, Object srcValue) {
		if (srcValue == null) {
			return null;
		}
		final UserDefinedDataHandlerManager uddhm = UserDefinedDataHandlerManager.getInstance();
		Object handler = uddhm.getColumnDataHandler(scc.getUserDataHandler());
		// If user defined handler is not null, CMT will use the handler's return value.
		if (handler != null) {
			return uddhm.handleColumnData(handler, recordMap, scc.getName());
		}
		Object result = convertFactory.convert(srcValue, toColumn.getDataTypeInstance(), config);
		if (result instanceof String) {
			if (scc.isNeedTrim()) {
				result = result.toString().trim();
			}
			result = scc.getReplaceValue(result.toString());
		}
		return result;
	}

	/**
	 * Verify is the sourceColumn convert to targetColumn
	 * 
	 * @param sourceColumn Column
	 * @param targetColumn Column
	 * @param config       MigrationConfiguration
	 * @return VerifyInfo
	 */
	public VerifyInfo verifyColumnDataType(Column sourceColumn, Column targetColumn,
			MigrationConfiguration config) {
		CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
		VerifyInfo info = new VerifyInfo(VerifyInfo.TYPE_NO_MATCH, "");

		String sourceType = sourceColumn.getDataType();
		String sourcePrecision = String.valueOf(sourceColumn.getPrecision());
		String sourceScale = String.valueOf(sourceColumn.getScale());

		// if the sourceType is set
		if (isCollection(sourceType)) {
			return validateCollection(sourceColumn, targetColumn, config);
		}

		String targetType = targetColumn.getDataType();
		int targetTypeID = dataTypeHelper.getCUBRIDDataTypeID(targetType);

		String key = dataTypeMappingHelper.getMapKey(sourceType, sourcePrecision, sourceScale);
		if (!dataTypeMappingHelper.getXmlConfigMap().containsKey(key)) {
			key = sourceType;
		}
		// check weather the target type is in dataTypeMapping according to source type
		boolean result = false;
		MapItem mapItem;
		if (dataTypeMappingHelper.getXmlConfigMap().containsKey(key)) {
			mapItem = dataTypeMappingHelper.getXmlConfigMap().get(key);
			for (MapObject targetMappingItem : mapItem.getAvailableTargetList()) {
				if (targetTypeID == dataTypeHelper.getCUBRIDDataTypeID(targetMappingItem.getDatatype())) {
					result = true;
					break;
				}
			}
		} else {
			// if can't find the source type
			info.setResult(VerifyInfo.TYPE_NO_MATCH);
			info.setMessage("ERROR: Can't find the sourceType:" + sourceType);
			return info;
		}
		// if not in mappng list , return TYPE_NO_MATCH
		if (!result) {
			info.setResult(VerifyInfo.TYPE_NO_MATCH);
			info.setMessage("Can't convert source type:" + sourceType + " to target type: "
					+ targetType);
			return info;
		}
		// if the sourceType is related to the database charset,the char only can
		// convert to char
		if (isRelatedToCharset(sourceType)) {
			return validateChar(sourceColumn, targetColumn, config);
		}
		// if numeric to varchar
		if (isNumericType(sourceType) && isRelatedToCharset(targetType)) {
			return validateNumericToVarchar(sourceColumn, targetColumn, config);
		}

		for (MapObject targetMappingItem : mapItem.getAvailableTargetList()) {
			if (targetTypeID == dataTypeHelper.getCUBRIDDataTypeID(targetMappingItem.getDatatype())) {
				// check the precision
				info = checkPrecision(targetMappingItem, sourceColumn, targetColumn);
				if (info != null) {
					return info;
				}

				// check the scale
				info = checkScale(targetMappingItem, sourceColumn, targetColumn);
				if (info != null) {
					return info;
				}

				// if verify success
				info = new VerifyInfo(VerifyInfo.TYPE_MATCH, "success");
				break;
			}
		}

		return info;
	}

	/**
	 * check the precision
	 * 
	 * @param targetMappingItem MapObject
	 * @param sourceColumn      Column
	 * @param targetColumn      Column
	 * @return VerifyInfo
	 */
	protected VerifyInfo checkPrecision(MapObject targetMappingItem, Column sourceColumn,
			Column targetColumn) {
		VerifyInfo info;
		int sourcePrecision = sourceColumn.getPrecision();
		int targetPrecision = targetColumn.getPrecision();

		if ("n".equalsIgnoreCase(targetMappingItem.getPrecision())) {
			if (Integer.valueOf(targetPrecision) < Integer.valueOf(sourcePrecision)) {
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"The precision of targetColumn should equal and greater than "
								+ targetMappingItem.getPrecision());
				return info;
			}
		} else if ("p".equalsIgnoreCase(targetMappingItem.getPrecision())) { // if the precision is "p"
			if (Integer.valueOf(targetPrecision) < Integer.valueOf(sourcePrecision)) {
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"The precision of targetColumn should equal and greater than sourceColumn's");
				return info;
			}
		} else { // verify the target scale
			int mapingItemPrecision = -1;
			if (targetMappingItem.getPrecision() != null
					&& targetMappingItem.getPrecision().length() > 0) {
				try {
					mapingItemPrecision = Integer.parseInt(targetMappingItem.getPrecision());
				} catch (NumberFormatException ex) {
					LOG.error("transform:" + targetMappingItem.getPrecision() + "\t to int error");
				}
			}

			if (Integer.valueOf(targetPrecision) < mapingItemPrecision) {
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"The target precision should equal or greater than : "
								+ targetMappingItem.getPrecision());
				return info;
			}

		}

		return null;
	}

	/**
	 * check the scale
	 * 
	 * @param targetMappingItem MapObject
	 * @param sourceColumn      Column
	 * @param targetColumn      Column
	 * @return VerifyInfo
	 */
	protected VerifyInfo checkScale(MapObject targetMappingItem, Column sourceColumn,
			Column targetColumn) {
		VerifyInfo info;
		int sourceScale = sourceColumn.getScale();
		int targetScale = targetColumn.getScale();

		// if the scale is variable,we should verify it
		if ("n".equalsIgnoreCase(targetMappingItem.getScale())) {
			if (targetScale < sourceScale) {
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"The scale of target column should be equal or greater than " + sourceScale);
				return info;
			}
		} else if ("s".equalsIgnoreCase(targetMappingItem.getScale())) {
			if (targetScale < sourceScale) {
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"The scale of target column should be equal or greater than source column's");
				return info;
			}
			// if we need verify the deta (targetPrecision - targetScale)
			int sourcePrecision = sourceColumn.getPrecision();
			int targetPrecision = targetColumn.getPrecision();

			if ((targetPrecision - targetScale) < (sourcePrecision - sourceScale)) {
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"The deta (target precision - target scale) should be equal or greater than source column's");
				return info;
			}
		} else {
			int mapingItemScale = -1;
			if (StringUtils.isNotBlank(targetMappingItem.getScale())) {
				try {
					mapingItemScale = Integer.parseInt(targetMappingItem.getScale());
				} catch (NumberFormatException ex) {
					LOG.error("transform:" + targetMappingItem.getPrecision() + "\t to int error");
				}
			}
			if (Integer.valueOf(targetScale) < mapingItemScale) {
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"The target scale should be equal or greater than : "
								+ targetMappingItem.getScale());
				return info;
			}

		}
		return null;
	}

	/**
	 * 
	 * verify the length that numeric convert to varchar
	 * 
	 * @param sourceColumn Column
	 * @param targetColumn Column
	 * @param config       MigrationConfiguration
	 * @return VerifyInfo
	 */
	protected VerifyInfo validateNumericToVarchar(Column sourceColumn, Column targetColumn,
			MigrationConfiguration config) {
		VerifyInfo info;

		int needPrecision = getNumericToCharLength(sourceColumn);
		if (targetColumn.getPrecision() < needPrecision) {
			LOG.info("ERROR: The target precision should equal or greater than " + needPrecision);

			info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
					"ERROR: The target precision should equal or greater than " + needPrecision);
		} else {
			info = new VerifyInfo(VerifyInfo.TYPE_MATCH, "");
		}

		return info;
	}

	/**
	 * 
	 * verify the char length
	 * 
	 * @param sourceColumn Column
	 * @param targetColumn Column
	 * @param config       MigrationConfiguration
	 * @return VerifyInfo
	 */
	protected VerifyInfo validateChar(Column sourceColumn, Column targetColumn,
			MigrationConfiguration config) {
		VerifyInfo info = new VerifyInfo(VerifyInfo.TYPE_NO_MATCH, "");
		CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
		if (dataTypeHelper.isString(targetColumn.getDataType())
				|| dataTypeHelper.isNString(targetColumn.getDataType())) {
			int sourcePrecision = sourceColumn.getPrecision();
			int targetPrecision = targetColumn.getPrecision();
			int needPrecision = config.getCharsetFactor() * sourcePrecision;

			if (targetPrecision < needPrecision) {
				LOG.info("ERROR: The target precision should equal or greater than "
						+ needPrecision);
				info = new VerifyInfo(VerifyInfo.TYPE_NOENOUGH_LENGTH,
						"ERROR: The target precision should equal or greater than " + needPrecision);
			} else {
				// if success
				info = new VerifyInfo(VerifyInfo.TYPE_MATCH, "");
			}
		}

		return info;
	}

	/**
	 * 
	 * verify the collection is match
	 * 
	 * @param sourceColumn Column
	 * @param targetColumn Column
	 * @param config       MigrationConfiguration
	 * @return VerifyInfo verifyInfo
	 */
	protected VerifyInfo validateCollection(Column sourceColumn, Column targetColumn,
			MigrationConfiguration config) {
		return new VerifyInfo(VerifyInfo.TYPE_MATCH, "");
	}

	/**
	 * 
	 * judge the type is collection type
	 * 
	 * @param type data type
	 * @return true if is a collection data type
	 */
	protected boolean isCollection(String type) {
		if (type.toLowerCase(Locale.ENGLISH).indexOf("set") >= 0) {
			return true;
		}
		return false;
	}

	/**
	 * calculate the varchar length that numeric type convert to varchar
	 * 
	 * @param column Column
	 * @return length
	 */
	protected int getNumericToCharLength(Column column) {
		int length = 0;

		String dataType = column.getDataType();

		if (!isUnsignedNumeric(dataType)) {
			// add the sign symbol
			length = 1;
		}
		length += column.getPrecision();
		if (column.getScale() > 0) {
			// add the point symbol
			length++;
		}

		int valp = column.getPrecision() == null ? 0 : column.getPrecision().intValue();
		int vals = column.getScale() == null ? 0 : column.getScale().intValue();
		if (valp == vals) {
			// add the zero symbol
			length++;
		}
		return length;
	}

	/**
	 * judge the type is numeric
	 * 
	 * @param type String
	 * @return boolean isNumericType
	 */
	protected boolean isNumericType(String type) {
		String lowerCase = type.toLowerCase(Locale.ENGLISH);

		if (lowerCase.indexOf("numeric") >= 0 || lowerCase.indexOf("decimal") >= 0
				|| lowerCase.indexOf("number") >= 0 || lowerCase.indexOf("integer") >= 0) {
			return true;
		}

		return false;
	}

	/**
	 * judge the type is unsigned
	 * 
	 * @param type String
	 * @return boolean isUnsignedNumeric
	 */
	protected boolean isUnsignedNumeric(String type) {

		if (type.toLowerCase(Locale.ENGLISH).indexOf("unsigned") >= 0) {
			return true;
		}

		return false;
	}

	/**
	 * judge the type is related to the database charset
	 * 
	 * @param type String
	 * @return boolean
	 */
	private boolean isRelatedToCharset(String type) {
		if (type.toLowerCase(Locale.ENGLISH).indexOf("char") >= 0) {
			return true;
		}
		return false;
	}

	/**
	 * Turn the DDL of source db to CUBRID DDL. For example: MySQL select `test`
	 * from `code` will be turned to CUBRID select "test" from "code"
	 * 
	 * TODO need to improve
	 * 
	 * @param ddl Source DDL
	 * @return CUBRID DDL
	 */
	public String getFitTargetFormatSQL(String ddl) {
		return ddl;
	}

	/**
	 * transform to cubrid partition ddl
	 * 
	 * @param table Table
	 * @return CUBRID partition ddl
	 */
	public String getToCUBRIDPartitionDDL(Table table) {
		if (table == null || table.getPartitionInfo() == null) {
			return null;
		}
		return getFitTargetFormatSQL(table.getPartitionInfo().getDDL());
	}

	public AbstractDataTypeMappingHelper getDataTypeMappingHelper() {
		return dataTypeMappingHelper;
	}

	public abstract String getGraphDataType(Column col, MigrationConfiguration cfg);

}
