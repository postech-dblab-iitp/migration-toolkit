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

/**
 * SourceTableConfig
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-9-8 created by Kevin Cao
 */
public class SourceTableConfig {

	private String name;
	/**
	 * Owner name such as SCOTT of SCOTT.EMP on Oracle.
	 * It is a null value except Oracle.
	 */
	private String owner;
	private String target;
	private boolean createNewTable = true;
	private boolean migrateData = true;
	private boolean replace = true;
	private String sqlBefore;
	private String sqlAfter;
	private final List<SourceColumnConfig> columns = new ArrayList<SourceColumnConfig>();

	/**
	 * addAllColumnList
	 * 
	 * @param list List<SourceColumnConfig>
	 */
	public void addAllColumnList(List<SourceColumnConfig> list) {
		columns.addAll(list);
		for (SourceColumnConfig scc : columns) {
			scc.setParent(this);
		}
	}

	/**
	 * addColumnConfig
	 * 
	 * @param name String
	 * @param target String
	 * @param isCreate create or not
	 */
	public void addColumnConfig(String name, String target, boolean isCreate) {
		SourceColumnConfig scc = getColumnConfig(name);
		if (scc == null) {
			scc = new SourceColumnConfig();
			columns.add(scc);
		}
		scc.setName(name);
		scc.setTarget(target);
		scc.setCreate(isCreate);
		scc.setReplace(isCreate);
		scc.setParent(this);
	}

	/**
	 * clearColumnList
	 * 
	 */
	public void clearColumnList() {
		columns.clear();
	}

	/**
	 * getColumnConfig
	 * 
	 * @param sourceName String
	 * @return SourceColumnConfig
	 */
	public SourceColumnConfig getColumnConfig(String sourceName) {
		for (SourceColumnConfig scc : columns) {
			if (scc.getName().equals(sourceName)) {
				return scc;
			}
		}
		return null;
	}

	/**
	 * getColumnConfigByTarget
	 * 
	 * @param colName String
	 * @return SourceColumnConfig
	 */
	public SourceColumnConfig getColumnConfigByTarget(String colName) {
		for (SourceColumnConfig scc : columns) {
			if (scc.getTarget().equals(colName)) {
				return scc;
			}
		}
		return null;
	}

	public List<SourceColumnConfig> getColumnConfigList() {
		return new ArrayList<SourceColumnConfig>(columns);
	}

	/**
	 * Source table name
	 * 
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	public String getOwner() {
		return owner;
	}

	public String getSqlAfter() {
		return sqlAfter == null ? "" : sqlAfter;
	}

	public String getSqlBefore() {
		return sqlBefore == null ? "" : sqlBefore;
	}

	/**
	 * Target table name to import
	 * 
	 * @return the target
	 */
	public String getTarget() {
		return target;
	}

	/**
	 * Has column to be exported
	 * 
	 * @return true if has
	 */
	private boolean hasColumn2Exp() {
		for (SourceColumnConfig scc : columns) {
			if (scc.isCreate()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Create a new table
	 * 
	 * @return the createNewTable
	 */
	public boolean isCreateNewTable() {
		return createNewTable;
	}

	/**
	 * Migrate data.
	 * 
	 * @return the migrationData
	 */
	public boolean isMigrateData() {
		return migrateData;
	}

	/**
	 * If true, the old table of target DB will be dropped firstly.
	 * 
	 * @return the replace
	 */
	public boolean isReplace() {
		return replace;
	}

	/**
	 * removeColumnConfig
	 * 
	 * @param columnName String
	 */
	public void removeColumnConfig(String columnName) {
		for (SourceColumnConfig scc : columns) {
			if (scc.getName().equals(columnName)) {
				columns.remove(scc);
				break;
			}
		}
	}

	/**
	 * @param createNewTable the createNewTable to set
	 */
	public void setCreateNewTable(boolean createNewTable) {
		if (!this.createNewTable && createNewTable && !hasColumn2Exp()) {
			for (SourceColumnConfig scc : columns) {
				scc.setCreate(true);
			}
		}
		this.createNewTable = createNewTable;
	}

	/**
	 * @param migrationData the migrationData to set
	 */
	public void setMigrateData(boolean migrationData) {
		if (!this.migrateData && migrationData && !hasColumn2Exp()) {
			for (SourceColumnConfig scc : columns) {
				scc.setCreate(true);
			}
		}
		this.migrateData = migrationData;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * @param replace the replace to set
	 */
	public void setReplace(boolean replace) {
		this.replace = replace;
	}

	public void setSqlAfter(String sqlAfter) {
		this.sqlAfter = sqlAfter;
	}

	public void setSqlBefore(String sqlBefore) {
		this.sqlBefore = sqlBefore;
	}

	/**
	 * @param target the target to set
	 */
	public void setTarget(String target) {
		this.target = target;
	}
}
