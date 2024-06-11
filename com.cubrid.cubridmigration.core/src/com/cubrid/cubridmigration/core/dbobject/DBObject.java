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
package com.cubrid.cubridmigration.core.dbobject;

import java.io.Serializable;

/**
 * DBObject is the base class of
 * Table,View,Sequence,Column,Index,Partition,Trigger,Procedure,PK,FK,and etc...
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-7 created by Kevin Cao
 */
public abstract class DBObject implements
		Serializable {

	private static final long serialVersionUID = -4165580169748817694L;
	public final static String OBJ_TYPE_TABLE = "table";
	public final static String OBJ_TYPE_PARTITION = "table partition";
	public final static String OBJ_TYPE_COLUMN = "column";
	public final static String OBJ_TYPE_VIEW = "view";
	public final static String OBJ_TYPE_PK = "primary key";
	public final static String OBJ_TYPE_FK = "foreign key";
	public final static String OBJ_TYPE_INDEX = "index";
	public final static String OBJ_TYPE_SEQUENCE = "sequence";
	public final static String OBJ_TYPE_TRIGGER = "trigger";
	public final static String OBJ_TYPE_PROCEDURE = "procedure";
	public final static String OBJ_TYPE_FUNCTION = "function";
	public final static String OBJ_TYPE_RECORD = "record";
	
	public final static String OBJ_GRAPH_TYPE_VERTEX = "Vertex";
	public final static String OBJ_GRAPH_TYPE_EDGE = "Edge";
	public final static String OBJ_GRAPH_TYPE_INDEX= "Index";

	/**
	 * Retrieves the Object's name
	 * 
	 * @return String name
	 */
	public abstract String getName();

	/**
	 * Retrieves the Object's type table/view/index ......
	 * 
	 * @return String
	 */
	public abstract String getObjType();

	/**
	 * Retrieves the Object's DDL
	 * 
	 * @return String DDL
	 */
	public abstract String getDDL();
	
	public abstract String getSourceDBObject();
}
