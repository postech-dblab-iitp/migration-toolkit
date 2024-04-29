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

import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.cubrid.trans.CUBRID2CUBRIDTranformHelper;
import com.cubrid.cubridmigration.cubrid.trans.CUBRIDDataTypeMappingHelper;
import com.cubrid.cubridmigration.cubrid.trans.ToCUBRIDDataConverterFacade;
import com.cubrid.cubridmigration.graph.trans.CUBRID2Neo4jTranformHelper;
import com.cubrid.cubridmigration.graph.trans.Neo4j2CUBRIDTranformHelper;
import com.cubrid.cubridmigration.graph.trans.Neo4jDataTypeMappingHelper;
import com.cubrid.cubridmigration.graph.trans.ToNeo4jDataConverterFacade;
import com.cubrid.cubridmigration.mssql.trans.MSSQL2CUBRIDTranformHelper;
import com.cubrid.cubridmigration.mssql.trans.MSSQLDataTypeMappingHelper;
import com.cubrid.cubridmigration.mysql.trans.MySQL2CUBRIDTranformHelper;
import com.cubrid.cubridmigration.mysql.trans.MySQLDataTypeMappingHelper;
import com.cubrid.cubridmigration.oracle.trans.Oracle2CUBRIDTranformHelper;
import com.cubrid.cubridmigration.oracle.trans.OracleDataTypeMappingHelper;
import com.cubrid.cubridmigration.tibero.trans.Tibero2CUBRIDTranformHelper;
import com.cubrid.cubridmigration.tibero.trans.TiberoDataTypeMappingHelper;

/**
 * MigrationTransFactory will return the DBTransform instance by input source
 * database type and target database type.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-11-14 created by Kevin Cao
 */
public class MigrationTransFactory {

	private static final MSSQL2CUBRIDTranformHelper MSSQL2CUBRID_TRANFORM_HELPER = new MSSQL2CUBRIDTranformHelper(
			new MSSQLDataTypeMappingHelper(),
			ToCUBRIDDataConverterFacade.getIntance());
	
	private static final MySQL2CUBRIDTranformHelper MY_SQL2CUBRID_TRANFORM_HELPER = new MySQL2CUBRIDTranformHelper(
			new MySQLDataTypeMappingHelper(),
			ToCUBRIDDataConverterFacade.getIntance());
		
	private static final Oracle2CUBRIDTranformHelper ORACLE2CUBRID_TRANFORM_HELPER = new Oracle2CUBRIDTranformHelper(
			new OracleDataTypeMappingHelper(),
			ToCUBRIDDataConverterFacade.getIntance());

	private static final CUBRID2CUBRIDTranformHelper CUBRID2CUBRID_TRANFORM_HELPER = new CUBRID2CUBRIDTranformHelper(
			new CUBRIDDataTypeMappingHelper());
	
	private static final CUBRID2Neo4jTranformHelper CUBRID2GRAPH_TRANFORM_HELPER = new CUBRID2Neo4jTranformHelper(
			new Neo4jDataTypeMappingHelper(), 
			ToNeo4jDataConverterFacade.getInstance());

	private static final Neo4j2CUBRIDTranformHelper GRAPH2CUBRID_TRANFORM_HELPER = new Neo4j2CUBRIDTranformHelper(
			new CUBRIDDataTypeMappingHelper(),
			ToCUBRIDDataConverterFacade.getIntance());
	
	private static final Tibero2CUBRIDTranformHelper TIBERO2CUBRID_TRANFORM_HELPER = new Tibero2CUBRIDTranformHelper(
			new TiberoDataTypeMappingHelper(),
			ToCUBRIDDataConverterFacade.getIntance());
	
	/**
	 * getTransformHelper of source to target migration
	 * 
	 * @param srcDT DatabaseType of source
	 * @param tarDT DatabaseType of target
	 * @return DBTranformHelper
	 */
	public static DBTransformHelper getTransformHelper(DatabaseType srcDT,
			DatabaseType tarDT) {
		if (tarDT.getID() == DatabaseType.GRAPH.getID()){
			return CUBRID2GRAPH_TRANFORM_HELPER;
		} else if (srcDT.getID() == DatabaseType.GRAPH.getID() &&
				tarDT.getID() == DatabaseType.CUBRID.getID()) {
			return GRAPH2CUBRID_TRANFORM_HELPER;
		} else if (srcDT.getID() == DatabaseType.CUBRID.getID()) {
			return CUBRID2CUBRID_TRANFORM_HELPER;
		} else if (srcDT.getID() == DatabaseType.ORACLE.getID()) {
			return ORACLE2CUBRID_TRANFORM_HELPER;
		} else if (srcDT.getID() == DatabaseType.TIBERO.getID()) {
			return TIBERO2CUBRID_TRANFORM_HELPER;
		} else if (srcDT.getID() == DatabaseType.MYSQL.getID()) {
			return MY_SQL2CUBRID_TRANFORM_HELPER;
		} else if (srcDT.getID() == DatabaseType.MSSQL.getID()) {
			return MSSQL2CUBRID_TRANFORM_HELPER;
		}
		
		throw new IllegalArgumentException("Can't support migration type.");
	}
}
