package com.cubrid.cubridmigration.graph.trans;

import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.trans.IDataConvertorFacade;

public class ToNeo4jDataConverterFacade  implements
	IDataConvertorFacade{
	
	private final static ToNeo4jDataConverterFacade instance = new ToNeo4jDataConverterFacade();

	public Object convert(Object obj, DataTypeInstance dti,
			MigrationConfiguration config) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static ToNeo4jDataConverterFacade getInstance() {
		return instance;
	}

}
