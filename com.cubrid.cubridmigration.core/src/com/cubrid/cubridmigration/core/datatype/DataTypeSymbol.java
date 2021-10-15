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

import java.util.ArrayList;
import java.util.List;

/**
 * DataTypeSymbol --Used for save data type symbol
 * 
 * @author Kevin.Wang
 * @version 1.0 - 2011-11-29 created by Kevin.Wang
 */
public class DataTypeSymbol implements
		Cloneable {

	private final Integer dataTypeID;
	private final String shownDataType;
	private final String innerDataType;

	private List<String> nickNames = new ArrayList<String>();

	/**
	 * constructor
	 * 
	 * @param dataTypeID Integer
	 * @param showDataType String
	 * @param subDataType String
	 */
	public DataTypeSymbol(Integer dataTypeID, String showDataType,
			String subDataType) {
		this.dataTypeID = dataTypeID;
		this.shownDataType = showDataType;
		this.innerDataType = subDataType;
	}

	/**
	 * get cubrid data type id
	 * 
	 * @return cubridDataTypeID Integer
	 */
	public Integer getDataTypeID() {
		return dataTypeID;
	}

	/**
	 * get ShownDataType
	 * 
	 * @return shownDataType String
	 */
	public String getShownDataType() {
		return shownDataType;
	}

	/**
	 * getInnerDataType
	 * 
	 * @return innerDataType String
	 */
	public String getInnerDataType() {
		return innerDataType;
	}

	/**
	 * getNickNames
	 * 
	 * @return nickNames List<String>
	 */
	public List<String> getNickNames() {
		return nickNames;
	}

	/**
	 * setNickNames
	 * 
	 * @param nickNames List<String>
	 */
	public void setNickNames(List<String> nickNames) {
		this.nickNames = nickNames;
	}

	/**
	 * clone
	 * 
	 * @return cloned DataTypeSymbol
	 * @throws CloneNotSupportedException when error
	 */
	public DataTypeSymbol clone() throws CloneNotSupportedException {
		return (DataTypeSymbol) super.clone();
	}

}
