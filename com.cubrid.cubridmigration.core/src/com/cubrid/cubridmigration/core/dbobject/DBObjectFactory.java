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

import java.math.BigInteger;

import com.cubrid.cubridmigration.core.datatype.DataType;

/**
 * 
 * DBObjectFactory Description
 * 
 * @author moulinwang
 * @version 1.0 - 2009-10-21
 */
public class DBObjectFactory {

	/**
	 * return Catalog
	 * 
	 * @return Catalog
	 */
	public Catalog createCatalog() {
		return new Catalog();
	}

	/**
	 * return Schema
	 * 
	 * @return Schema
	 */
	public Schema createSchema() {
		return new Schema();
	}

	/**
	 * return Table
	 * 
	 * @return Table
	 */
	public Table createTable() {
		return new Table();
	}

	/**
	 * return PK
	 * 
	 * @param table table
	 * @return PK
	 */
	public PK createPK(Table table) {
		return new PK(table);
	}

	/**
	 * return Index
	 * 
	 * @param table table
	 * @return Index
	 */
	public Index createIndex(Table table) {
		return new Index(table);
	}

	/**
	 * return FK
	 * 
	 * @param table table
	 * @return FK
	 */
	public FK createFK(Table table) {
		return new FK(table);
	}

	/**
	 * return Column
	 * 
	 * @return Column
	 */
	public Column createColumn() {
		return new Column();
	}

	/**
	 * return View
	 * 
	 * @return View
	 */
	public View createView() {
		return new View();
	}

	/**
	 * return Trigger
	 * 
	 * @return Trigger
	 */
	public Trigger createTrigger() {
		return new Trigger();
	}

	/**
	 * return Function
	 * 
	 * @return Function
	 */
	public Function createFunction() {
		return new Function();
	}

	/**
	 * return Procedure
	 * 
	 * @return Procedure
	 */
	public Procedure createProcedure() {
		return new Procedure();
	}

	/**
	 * return PartitionInfo
	 * 
	 * @return PartitionInfo
	 */
	public PartitionInfo createPartitionInfo() {
		return new PartitionInfo();
	}

	/**
	 * return PartitionTable
	 * 
	 * @return PartitionTable
	 */
	public PartitionTable createPartitionTable() {
		return new PartitionTable();
	}

	/**
	 * return Sequence
	 * 
	 * @param sequenceName String
	 * @param minValue BigInteger
	 * @param maxValue BigInteger
	 * @param incrementBy BigInteger
	 * @param currentValue BigInteger
	 * @param cycleFlag boolean
	 * @param cacheSize int
	 * @return Sequence
	 */
	public Sequence createSequence(String sequenceName, BigInteger minValue,
			BigInteger maxValue, BigInteger incrementBy,
			BigInteger currentValue, boolean cycleFlag, int cacheSize) {
		return new Sequence(sequenceName, minValue, maxValue, incrementBy,
				currentValue, cycleFlag, cacheSize);
	}

	/**
	 * return DataType
	 * 
	 * @return DataType
	 */
	public DataType createDataType() {
		return new DataType();
	}

}
