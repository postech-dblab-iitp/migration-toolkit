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
package com.cubrid.cubridmigration.core.datatype;

import java.sql.Types;

/**
 * DataTypeConstant Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-1 created by Kevin Cao
 */
public class DataTypeConstant {

	public static final int NUMERIC_MAX_PRECISIE_SIZE = 38;
	public static final int NUMERIC_MAX_SCALE_SIZE = 38;
	public static final int CUBRID_MAXSIZE = 1073741823;
	public static final int CUBRID_NCHAR_MAXSIZE = 536870911;

	//JDBC datatype id
	public static final int CUBRID_DT_SMALLINT = Types.SMALLINT; //5;
	public static final int CUBRID_DT_INTEGER = Types.INTEGER; //4;
	public static final int CUBRID_DT_BIGINT = Types.BIGINT; //-5;
	public static final int CUBRID_DT_NUMERIC = Types.NUMERIC; //2;
	public static final int CUBRID_DT_FLOAT = Types.REAL; //7;
	public static final int CUBRID_DT_DOUBLE = Types.DOUBLE; //8;
	public static final int CUBRID_DT_MONETARY = 30008; //8;
	public static final int CUBRID_DT_CHAR = Types.CHAR; //1;
	public static final int CUBRID_DT_VARCHAR = Types.VARCHAR; //12;
	public static final int CUBRID_DT_NCHAR = Types.NCHAR; //13;
	public static final int CUBRID_DT_NVARCHAR = Types.NVARCHAR; //14;
	public static final int CUBRID_DT_TIME = Types.TIME; //92;
	public static final int CUBRID_DT_DATE = Types.DATE; //91;
	public static final int CUBRID_DT_TIMESTAMP = Types.TIMESTAMP; //93;
	public static final int CUBRID_DT_DATETIME = 30093; //93
	public static final int CUBRID_DT_BIT = Types.BINARY; //-2;
	public static final int CUBRID_DT_VARBIT = Types.VARBINARY; //-3;
	public static final int CUBRID_DT_SET = 31111; //1111
	public static final int CUBRID_DT_MULTISET = 41111; //1111
	public static final int CUBRID_DT_SEQUENCE = 51111; //1111
	public static final int CUBRID_DT_GLO = 32004; //1111
	public static final int CUBRID_DT_FBO = 42004; //1111
	public static final int CUBRID_DT_OBJECT = 32000; //2000
	public static final int CUBRID_DT_CLOB = Types.CLOB; //2005;
	public static final int CUBRID_DT_BLOB = Types.BLOB; //2004;
	public static final int CUBRID_DT_ENUM = 61111; //12

	/** MYSQL **/
	public static final int MYSQL_MAXSIZE = 1073741823;
	public static final String MYSQL_NCHARMAXSIZE = "536870911";

	/** ORACLE **/
	public static final int ORACLE_MAXSIZE = 1073741823;
	public static final String ORACLE_NCHAR_MAXSIZE = "536870911";
	public static final int ORACLE_INTEGERTOVARCHAR_MINSIZE = 127;

	/** SQLSERVER **/
	public static final int SQLSERVER_MAXSIZE = 1073741823;
	public static final String SQLSERVER_NCHAR_MAXSIZE = "536870911";

	public static final String CUBRID_VARCHAR = "varchar";
	public static final String CUBRID_STRING = "string";
	public static final String CUBRID_MAX_VARCHAR = "varchar(1073741823)";

	public static final String CUBRID_FBO = "fbo";
	public static final String CUBRID_GLO = "glo";
}
