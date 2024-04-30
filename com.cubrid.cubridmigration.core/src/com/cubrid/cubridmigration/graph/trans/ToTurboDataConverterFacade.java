package com.cubrid.cubridmigration.graph.trans;

import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.trans.IDataConvertorFacade;

public class ToTurboDataConverterFacade  implements
	IDataConvertorFacade{
	
	private final static ToTurboDataConverterFacade instance = new ToTurboDataConverterFacade();

	public Object convert(Object obj, DataTypeInstance dti,
			MigrationConfiguration config) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static ToTurboDataConverterFacade getInstance() {
		return instance;
	}

}
