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
package com.cubrid.cubridmigration.core.mapping.model;

/**
 * 
 * 
 * MapObject Description
 * 
 * @author Kevin.Wang
 * @version 1.0 - 2011-11-29 created by Kevin.Wang
 */
public class MapObject implements
		Cloneable {

	private String datatype;
	private String precision = "-1";
	private String scale = "-1";

	/**
	 * get the datatype
	 * 
	 * @return datatype String
	 */
	public String getDatatype() {
		return datatype;
	}

	/**
	 * 
	 * set the data type
	 * 
	 * @param datatype String
	 */
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	/**
	 * 
	 * get the precision
	 * 
	 * @return String
	 */
	public String getPrecision() {
		return precision;
	}

	/**
	 * set the precision
	 * 
	 * @param precision String
	 */
	public void setPrecision(String precision) {
		this.precision = precision;
	}

	/**
	 * 
	 * get the scale
	 * 
	 * @return scale String
	 */
	public String getScale() {
		return scale;
	}

	/**
	 * 
	 * set the scale
	 * 
	 * @param scale String
	 */
	public void setScale(String scale) {
		this.scale = scale;
	}

	/**
	 * return a clone
	 * 
	 * @return DataTypeMappingItem
	 */
	public MapObject clone() {
		try {
			return (MapObject) super.clone();
		} catch (CloneNotSupportedException e) {
			return null;
		}
	}
}
