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
 * 
 * 
 * DataType
 * 
 * @author Kevin.Wang
 * @version 1.0 - 2011-11-29 created by Kevin.Wang
 */
public class DataType implements
		Serializable {
	private static final long serialVersionUID = -8768732886385204888L;
	private String typeName;
	private Integer jdbcDataTypeID;
	private Long precision;
	private String prefix;
	private String suffix;
	private String createParams;
	private Boolean nullable;
	private Boolean caseSensitive;
	private Integer searchable;
	private Boolean unsigned;
	private Boolean fixedPrecisionScale;
	private Boolean autoIncrement;
	private Integer minimumScale;
	private Integer maximumScale;
	private String realTypeName;

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public Integer getJdbcDataTypeID() {
		return jdbcDataTypeID;
	}

	public void setJdbcDataTypeID(Integer jdbcDataTypeID) {
		this.jdbcDataTypeID = jdbcDataTypeID;
	}

	public Long getPrecision() {
		return precision;
	}

	public void setPrecision(Long precision) {
		this.precision = precision;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getSuffix() {
		return suffix;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	public String getCreateParams() {
		return createParams;
	}

	public void setCreateParams(String createParams) {
		this.createParams = createParams;
	}

	public Boolean getNullable() {
		return nullable;
	}

	public void setNullable(Boolean nullable) {
		this.nullable = nullable;
	}

	public Boolean getCaseSensitive() {
		return caseSensitive;
	}

	public void setCaseSensitive(Boolean caseSensitive) {
		this.caseSensitive = caseSensitive;
	}

	public Integer getSearchable() {
		return searchable;
	}

	public void setSearchable(Integer searchable) {
		this.searchable = searchable;
	}

	public Boolean getUnsigned() {
		return unsigned;
	}

	public void setUnsigned(Boolean unsigned) {
		this.unsigned = unsigned;
	}

	public Boolean getFixedPrecisionScale() {
		return fixedPrecisionScale;
	}

	public void setFixedPrecisionScale(Boolean fixedPrecisionScale) {
		this.fixedPrecisionScale = fixedPrecisionScale;
	}

	public Boolean getAutoIncrement() {
		return autoIncrement;
	}

	public void setAutoIncrement(Boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}

	public Integer getMinimumScale() {
		return minimumScale;
	}

	public void setMinimumScale(Integer minimumScale) {
		this.minimumScale = minimumScale;
	}

	public Integer getMaximumScale() {
		return maximumScale;
	}

	public void setMaximumScale(Integer maximumScale) {
		this.maximumScale = maximumScale;
	}

	public String getRealTypeName() {
		return realTypeName;
	}

	public void setRealTypeName(String realTypeName) {
		this.realTypeName = realTypeName;
	}

}
