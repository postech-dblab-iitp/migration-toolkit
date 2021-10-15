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
package com.cubrid.cubridmigration.cubrid.trans;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.common.CommonUtils;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.trans.AbstractDataConverter;
import com.cubrid.cubridmigration.core.trans.IDataConvertorFacade;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.trans.converter.BigIntConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.BitConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.BlobConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.CharConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.ClobConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.DateConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.DateTimeConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.DoubleConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.FloatConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.IntegerConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.NumericConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.SmallIntConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.TimeConverter;
import com.cubrid.cubridmigration.cubrid.trans.converter.TimeStampConverter;

/**
 * 
 * CubridObjectFactory Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-12-1 created by Kevin Cao
 */
public final class ToCUBRIDDataConverterFacade implements
		IDataConvertorFacade {

	private final static ToCUBRIDDataConverterFacade INSTANCE = new ToCUBRIDDataConverterFacade();
	private final Map<Integer, AbstractDataConverter> converterMap = new HashMap<Integer, AbstractDataConverter>();

	/**
	 * Singleton factory
	 * 
	 * @return CubridObjectFactory
	 */
	public static ToCUBRIDDataConverterFacade getIntance() {
		return INSTANCE;
	}

	private ToCUBRIDDataConverterFacade() {
		converterMap.put(DataTypeConstant.CUBRID_DT_SMALLINT,
				new SmallIntConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_INTEGER,
				new IntegerConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_BIGINT,
				new BigIntConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_NUMERIC,
				new NumericConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_FLOAT, new FloatConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_DOUBLE,
				new DoubleConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_MONETARY,
				new DoubleConverter());

		converterMap.put(DataTypeConstant.CUBRID_DT_CHAR, new CharConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_VARCHAR,
				new CharConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_NCHAR, new CharConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_NVARCHAR,
				new CharConverter());

		converterMap.put(DataTypeConstant.CUBRID_DT_TIME, new TimeConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_DATE, new DateConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_TIMESTAMP,
				new TimeStampConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_DATETIME,
				new DateTimeConverter());

		converterMap.put(DataTypeConstant.CUBRID_DT_BIT, new BitConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_VARBIT, new BitConverter());

		//converterMap.put(DataTypeConstant.CUBRID_DT_GLO, new GLOConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_CLOB, new ClobConverter());
		converterMap.put(DataTypeConstant.CUBRID_DT_BLOB, new BlobConverter());
	}

	/**
	 * Convert input value to a data with specified data type.
	 * 
	 * @param obj Object
	 * @param dti Integer
	 * @param config MigrationConfiguration
	 * @return value Object
	 */
	public Object convert(Object obj, DataTypeInstance dti,
			MigrationConfiguration config) {

		if (obj == null) {
			return null;
		}
		CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
		if (dataTypeHelper.isEnum(dti.getName())) {
			return (String) obj;
		} else if (dataTypeHelper.isCollection(dti.getName())) {
			return toCUBRIDCollection(obj, dti, config);
		}
		Object dataTypeID = dataTypeHelper.getCUBRIDDataTypeID(dti.getName());
		final AbstractDataConverter cvter = converterMap.get(dataTypeID);
		if (cvter != null) {
			Object value = obj;
			//Transform Byte[] to byte[]
			if (obj instanceof Byte[]) {
				value = CommonUtils.getBytesFromByteArray((Byte[]) obj);
			}

			return cvter.convert(value, dti, config);
		}
		throw new RuntimeException("ERROR: could not convert:" + obj
				+ " to CUBRID type" + dti.getName());
	}

	/**
	 * If transforming Collection instance to String, then quote and comma
	 * character must be dealt with here, and in importing step, the transformed
	 * string must be dealt again to proper data type, so we just transform
	 * inner objects in the collection
	 * 
	 * @param obj Object
	 * @param dti DataTypeInstance
	 * @param config MigrationConfiguration
	 * @return Object array or list
	 */
	protected Object toCUBRIDCollection(Object obj, DataTypeInstance dti,
			MigrationConfiguration config) {
		if (obj != null && obj.getClass().isArray()) {
			return obj;
		}
		if (obj instanceof Collection || obj instanceof byte[][]) {
			Collection<?> c = (Collection<?>) obj;
			List<Object> newCollection = new ArrayList<Object>();

			for (Iterator<?> i = c.iterator(); i.hasNext();) {
				Object o = i.next();
				Object cubridObject = this.convert(o, dti.getSubType(), config);
				newCollection.add(cubridObject);
			}
			return newCollection;
		}
		return obj == null ? null : obj.toString();
	}

}
