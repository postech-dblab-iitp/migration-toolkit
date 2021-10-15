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
package com.cubrid.cubridmigration.core.engine.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

/**
 * SourceTableConfig
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-9-8 created by Kevin Cao
 */
public class SourceEntryTableConfig extends
		SourceTableConfig {

	private boolean createPK;
	private boolean createPartition;
	private boolean isEnableExpOpt;
	private boolean startFromTargetMax;

	private final List<SourceFKConfig> fks = new ArrayList<SourceFKConfig>();
	private final List<SourceIndexConfig> indexes = new ArrayList<SourceIndexConfig>();
	private String condition = "";

	/**
	 * Create PK on target table.
	 * 
	 * @return the createPK
	 */
	public boolean isCreatePK() {
		return createPK;
	}

	/**
	 * @param createPK the createPK to set
	 */
	public void setCreatePK(boolean createPK) {
		this.createPK = createPK;
	}

	/**
	 * Create partition on target table.
	 * 
	 * @return the createPartition
	 */
	public boolean isCreatePartition() {
		return createPartition;
	}

	/**
	 * @param createPartition the createPartition to set
	 */
	public void setCreatePartition(boolean createPartition) {
		this.createPartition = createPartition;
	}

	/**
	 * addFKConfig
	 * 
	 * @param name source foreign key name
	 * @param target foreign key name
	 * @param create default create or not
	 */
	public void addFKConfig(String name, String target, boolean create) {
		SourceFKConfig source = getFKConfig(name);
		if (source == null) {
			source = new SourceFKConfig();
			fks.add(source);
		}
		source.setCreate(create);
		source.setName(name);
		source.setTarget(target);
		source.setParent(this);
	}

	/**
	 * getFKConfig
	 * 
	 * @param sourceName source foreign key name
	 * @return SourceFKConfig
	 */
	public SourceFKConfig getFKConfig(String sourceName) {
		for (SourceFKConfig scc : fks) {
			if (scc.getName().equals(sourceName)) {
				return scc;
			}
		}
		return null;
	}

	/**
	 * removeFKConfig
	 * 
	 * @param sourceName foreign key name
	 */
	public void removeFKConfig(String sourceName) {
		for (SourceFKConfig scc : fks) {
			if (scc.getName().equals(sourceName)) {
				fks.remove(scc);
				break;
			}
		}
	}

	public List<SourceFKConfig> getFKConfigList() {
		return new ArrayList<SourceFKConfig>(fks);
	}

	/**
	 * addIndexConfig
	 * 
	 * @param name of index
	 * @param target name of index
	 * @param create default create or not
	 */
	public void addIndexConfig(String name, String target, boolean create) {
		SourceIndexConfig source = getIndexConfig(name);
		if (source == null) {
			source = new SourceIndexConfig();
			indexes.add(source);
		}
		source.setCreate(create);
		source.setName(name);
		source.setTarget(target);
		source.setParent(this);
	}

	/**
	 * getIndexConfig
	 * 
	 * @param sourceName of index
	 * @return SourceIndexConfig
	 */
	public SourceIndexConfig getIndexConfig(String sourceName) {
		for (SourceIndexConfig scc : indexes) {
			if (scc.getName().equals(sourceName)) {
				return scc;
			}
		}
		return null;
	}

	/**
	 * removeIndexConfig
	 * 
	 * @param sourceName of source index
	 */
	public void removeIndexConfig(String sourceName) {
		for (SourceIndexConfig scc : indexes) {
			if (scc.getName().equals(sourceName)) {
				indexes.remove(scc);
				break;
			}
		}
	}

	public List<SourceIndexConfig> getIndexConfigList() {
		return new ArrayList<SourceIndexConfig>(indexes);
	}

	public String getCondition() {
		return condition == null ? "" : condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	/**
	 * Reset the FK configurations
	 * 
	 * @param sFKCfgs List<SourceFKConfig>
	 */
	public void setFKs(List<SourceFKConfig> sFKCfgs) {
		if (CollectionUtils.isEmpty(sFKCfgs)) {
			this.fks.clear();
			return;
		}
		fks.clear();
		fks.addAll(sFKCfgs);
	}

	/**
	 * Set indexes
	 * 
	 * @param sics List<SourceIndexConfig>
	 */
	public void setIndexes(List<SourceIndexConfig> sics) {
		if (CollectionUtils.isEmpty(sics)) {
			this.indexes.clear();
			return;
		}
		indexes.clear();
		indexes.addAll(sics);

	}

	/**
	 * @param createNewTable the createNewTable to set
	 */
	public void setCreateNewTable(boolean createNewTable) {
		boolean old = this.isCreateNewTable();
		super.setCreateNewTable(createNewTable);
		if (!old && createNewTable) {
			boolean flag = false;
			for (SourceFKConfig fk : this.fks) {
				if (fk.isCreate()) {
					flag = true;
					break;
				}
			}
			if (!flag) {
				for (SourceFKConfig fk : this.fks) {
					fk.setCreate(true);
					fk.setReplace(true);
				}
			}
			flag = false;
			for (SourceIndexConfig idx : this.indexes) {
				if (idx.isCreate()) {
					flag = true;
					break;
				}
			}
			if (!flag) {
				for (SourceIndexConfig idx : this.indexes) {
					idx.setCreate(true);
					idx.setReplace(true);
				}
			}
		}
	}

	public boolean isStartFromTargetMax() {
		return startFromTargetMax;
	}

	public void setStartFromTargetMax(boolean startFromTargetMax) {
		this.startFromTargetMax = startFromTargetMax;
	}

	public boolean isEnableExpOpt() {
		return isEnableExpOpt;
	}

	public void setEnableExpOpt(boolean isEnableExpOpt) {
		this.isEnableExpOpt = isEnableExpOpt;
	}

}
