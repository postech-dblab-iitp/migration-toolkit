package com.cubrid.cubridmigration.graph.trans;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;

public class Tibero2Neo4jTranformHelper extends DBTransformHelper {
	public Tibero2Neo4jTranformHelper(
			AbstractDataTypeMappingHelper dataTypeMapping,
			ToNeo4jDataConverterFacade toNeo4jDataConverterFacade) {
		super(dataTypeMapping, toNeo4jDataConverterFacade);
		// TODO Auto-generated constructor stub
	}

	public ToNeo4jDataConverterFacade converter;
	
//	public CUBRID2Neo4jTranformHelper (AbstractDataTypeMappingHelper dataTypeMapping) {
//		this.converter = new ToNeo4jDataConverterFacade();
//	}

	@Override
	protected void adjustPrecision(Column srcColumn, Column cubridColumn,
			MigrationConfiguration config) {
		// TODO Auto-generated method stub
		
	}
	
	public String getGraphDataType(Column srcColumn, MigrationConfiguration config) {
		String srcDataType = srcColumn.getDataType();
		Integer srcPrecision = srcColumn.getPrecision();
		Integer srcScale = srcColumn.getScale();
		
		MapObject mapping = getDataTypeMapping(srcColumn, srcDataType, srcPrecision, srcScale,
				config.getSrcCatalog().getSupportedDataType());
		
		String graphType = mapping.getDatatype();
		
		return graphType;
	}
}
