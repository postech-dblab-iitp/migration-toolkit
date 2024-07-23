package com.cubrid.cubridmigration.graph.trans;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.trans.ToCUBRIDDataConverterFacade;

public class Turbo2CUBRIDTransformHelper extends DBTransformHelper {

	public Turbo2CUBRIDTransformHelper(
			AbstractDataTypeMappingHelper dataTypeMapping,
			ToCUBRIDDataConverterFacade cf) {
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
}