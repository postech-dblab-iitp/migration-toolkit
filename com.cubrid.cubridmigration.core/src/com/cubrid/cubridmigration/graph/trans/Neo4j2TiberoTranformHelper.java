package com.cubrid.cubridmigration.graph.trans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.datatype.DataType;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.tibero.TiberoDataTypeHelper;

public class Neo4j2TiberoTranformHelper extends DBTransformHelper {

	public Neo4j2TiberoTranformHelper(
			AbstractDataTypeMappingHelper dataTypeMapping,
			ToTiberoDataConverterFacade cf) {
		super(dataTypeMapping, cf);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void adjustPrecision(Column srcColumn, Column cubridColumn,
			MigrationConfiguration config) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getGraphDataType(Column col, MigrationConfiguration cfg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Table createRDBTable(SourceTableConfig stc, Table sourceTable, MigrationConfiguration config) {
		Table tarTable = new Table();
		tarTable.setName(stc.getTarget());

		List<Column> srcColumns = sourceTable.getColumns();
		List<Column> newColumns = new ArrayList<Column>();

		for (Column srcColumn : srcColumns) {
			SourceColumnConfig scc = stc.getColumnConfig(srcColumn.getName());
			Column cubridColumn = getRDBColumn(srcColumn, config);
			if (scc == null) {
				cubridColumn.setName(StringUtils.lowerCase(srcColumn.getName()));
			} else {
				cubridColumn.setName(scc.getTarget());
			}
			cubridColumn.setTableOrView(tarTable);
			newColumns.add(cubridColumn);
		}
		tarTable.setColumns(newColumns);

		return tarTable;
	}

	public Column getRDBColumn(Column srcCol, MigrationConfiguration config) {
		TiberoDataTypeHelper dataTypeHelper = TiberoDataTypeHelper.getInstance(null);
		Column tarCol = srcCol.cloneCol();
		tarCol.setName(StringUtils.lowerCase(tarCol.getName()));
		
		String srcDataType = srcCol.getDataType();
		
		Integer srcPrecision = srcCol.getPrecision();
		Integer srcScale = srcCol.getScale();
		
		Map<String, List<DataType>> supportedDataType = null;
		
		Catalog catalog = config.getSrcCatalog();
		
		if (catalog != null) {
			supportedDataType = catalog.getSupportedDataType();
		}
		if (supportedDataType == null) {
			supportedDataType = new HashMap<String, List<DataType>>();
		}
		
		MapObject mapping = getDataTypeMapping(srcCol, srcDataType, srcPrecision, srcScale,
				supportedDataType);

		String precision = mapping.getPrecision();
		String scale = mapping.getScale();
		
		tarCol.setDataType(mapping.getDatatype());
		tarCol.setShownDataType(mapping.getDatatype());
		
		
		if (precision != null) {
			tarCol.setPrecision(Integer.parseInt(mapping.getPrecision()));
		} else {
			tarCol.setPrecision(4000);
		}
		
		if (scale != null) {
			tarCol.setScale(Integer.parseInt(mapping.getScale()));
		} else {
			tarCol.setScale(0);
		}
		
		return tarCol;
	}
}