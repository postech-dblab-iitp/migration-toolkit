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

/**
 * sequence object
 * 
 * @author moulinwang
 * @version 1.0 - 2010-05-19
 */
public class Sequence extends
		DBObject implements
		Cloneable {

	private static final long serialVersionUID = 9178276389620479685L;
	private String sequenceName;
	private BigInteger minValue;
	private BigInteger maxValue;
	private BigInteger incrementBy;
	private BigInteger currentValue;
	private boolean cycleFlag = false;
	private int cacheSize = 2;

	private boolean isNoCache = true;
	private boolean isNoMinValue = true;
	private boolean isNoMaxValue = true;

	private String ddl;
	private String owner;

	public Sequence() {
		//do nothing: for json to bean
	}

	public Sequence(String sequenceName, BigInteger minValue,
			BigInteger maxValue, BigInteger incrementBy,
			BigInteger currentValue, boolean cycleFlag, int cacheSize) {
		super();
		this.sequenceName = sequenceName;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.incrementBy = incrementBy;
		this.currentValue = currentValue;
		this.cycleFlag = cycleFlag;
		this.cacheSize = cacheSize;
	}

	/**
	 * clone
	 * 
	 * @return Sequence
	 */
	public Object clone() {
		final Sequence sequence = new Sequence(sequenceName, minValue,
				maxValue, incrementBy, currentValue, cycleFlag, cacheSize);
		sequence.setNoCache(isNoCache);
		sequence.setNoMinValue(isNoMinValue);
		sequence.setNoMaxValue(isNoMaxValue);
		sequence.setDDL(ddl);
		sequence.setOwner(owner);
		return sequence;
	}

	public long getCacheSize() {
		return cacheSize;
	}

	public BigInteger getCurrentValue() {
		return currentValue == null ? new BigInteger("1") : currentValue;
	}

	public String getDDL() {
		return ddl;
	}

	public BigInteger getIncrementBy() {
		return incrementBy == null ? new BigInteger("1") : incrementBy;
	}

	public BigInteger getMaxValue() {
		return maxValue == null ? new BigInteger(
				"1000000000000000000000000000000000000") : maxValue;
	}

	public BigInteger getMinValue() {
		return minValue == null ? new BigInteger(
				"-10000000000000000000000000000000000000") : minValue;
	}

	public String getName() {
		return sequenceName;
	}

	/**
	 * @return object type
	 */
	public String getObjType() {
		return OBJ_TYPE_SEQUENCE;
	}

	public boolean isCycleFlag() {
		return cycleFlag;
	}

	public boolean isNoCache() {
		return isNoCache;
	}

	public boolean isNoMaxValue() {
		return isNoMaxValue;
	}

	public boolean isNoMinValue() {
		return isNoMinValue;
	}

	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}

	public void setCurrentValue(BigInteger currentValue) {
		this.currentValue = currentValue;
	}

	public void setCycleFlag(boolean cycleFlag) {
		this.cycleFlag = cycleFlag;
	}

	public void setDDL(String ddl) {
		this.ddl = ddl;
	}

	public void setIncrementBy(BigInteger incrementBy) {
		this.incrementBy = incrementBy;
	}

	public String getOwner() {
		return owner;
	}

	public void setMaxValue(BigInteger maxValue) {
		this.maxValue = maxValue;
	}

	public void setMinValue(BigInteger minValue) {
		this.minValue = minValue;
	}

	public void setName(String sequenceName) {
		this.sequenceName = sequenceName;
	}

	public void setNoCache(boolean isNoCache) {
		this.isNoCache = isNoCache;
	}

	public void setNoMaxValue(boolean isNoMaxValue) {
		this.isNoMaxValue = isNoMaxValue;
	}

	public void setNoMinValue(boolean isNoMinValue) {
		this.isNoMinValue = isNoMinValue;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}
}
