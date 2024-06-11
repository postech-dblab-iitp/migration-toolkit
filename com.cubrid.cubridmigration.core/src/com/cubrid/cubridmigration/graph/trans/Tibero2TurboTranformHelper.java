package com.cubrid.cubridmigration.graph.trans;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;

public class Tibero2TurboTranformHelper extends DBTransformHelper {
	public Tibero2TurboTranformHelper(
			AbstractDataTypeMappingHelper dataTypeMapping,
			ToTurboDataConverterFacade toTurboDataConverterFacade) {
		super(dataTypeMapping, toTurboDataConverterFacade);
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
		String graphType = new String();
		Integer srcPrecision = srcColumn.getPrecision();
		Integer srcScale = srcColumn.getScale();
		
		try {
			MapObject mapping = getDataTypeMapping(srcColumn, srcDataType, srcPrecision, srcScale,
					config.getSrcCatalog().getSupportedDataType());
			
			graphType = mapping.getDatatype();
		} catch (IllegalArgumentException e) {
			return "not support";
		}
		
		return graphType;
	}
}
