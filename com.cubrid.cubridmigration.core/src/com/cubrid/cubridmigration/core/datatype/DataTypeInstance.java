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

import java.io.Serializable;

/**
 * DataTypeInstance describes the columns data type information.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-11-21 created by Kevin Cao
 */
public class DataTypeInstance implements
		Serializable,
		Cloneable {

	private static final long serialVersionUID = -8412581658210221583L;

	private String name;
	private Integer precision;
	private Integer scale;
	private String elments; //For enum type

	private DataTypeInstance subType;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getPrecision() {
		return precision;
	}

	public void setPrecision(Integer precision) {
		this.precision = precision;
	}

	public Integer getScale() {
		return scale;
	}

	public void setScale(Integer scale) {
		this.scale = scale;
	}

	public String getElments() {
		return elments;
	}

	public void setElments(String elments) {
		this.elments = elments;
	}

	public DataTypeInstance getSubType() {
		return subType;
	}

	public void setSubType(DataTypeInstance subType) {
		this.subType = subType;
	}

	/**
	 * Get the data type instance's full data type.
	 * 
	 * @return data type with precision and scale...
	 */
	public String getShownDataType() {
		StringBuffer sb = new StringBuffer(name);
		if (subType != null) {
			sb.append("(").append(subType.getShownDataType()).append(")");
			return sb.toString();
		}
		if (this.elments != null) {
			sb.append("(").append(this.elments).append(")");
			return sb.toString();
		}
		if (this.precision != null) {
			sb.append("(").append(precision.toString());
			if (scale != null) {
				sb.append(",").append(scale.toString());
			}
			sb.append(")");
		}
		return sb.toString();
	}

	/**
	 * Deep clone object.
	 * 
	 * @return cloned.
	 */
	public DataTypeInstance clone() {
		try {
			final DataTypeInstance result = (DataTypeInstance) super.clone();
			if (subType != null) {
				result.setSubType(subType.clone());
			}
			return result;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
}
