package com.cubrid.cubridmigration.graph.trans;

import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.trans.IDataConvertorFacade;

public class ToGraphDataConverterFacade  implements
	IDataConvertorFacade{
	
	private final static ToGraphDataConverterFacade instance = new ToGraphDataConverterFacade();

	public Object convert(Object obj, DataTypeInstance dti,
			MigrationConfiguration config) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static ToGraphDataConverterFacade getInstance() {
		return instance;
	}

}
