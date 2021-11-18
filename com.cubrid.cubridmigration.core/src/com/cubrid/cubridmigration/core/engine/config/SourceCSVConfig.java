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
import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Table;

/**
 * SourceCSVConfig Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-3-15 created by Kevin Cao
 */
public class SourceCSVConfig extends
		SourceConfig {

	private boolean importFirstRow = true;

	private final List<SourceCSVColumnConfig> columnConfig = new ArrayList<SourceCSVColumnConfig>();

	private List<String[]> previewData = new ArrayList<String[]>();

	public boolean isImportFirstRow() {
		return importFirstRow;
	}

	/**
	 * setImportFirstRow
	 * 
	 * @param ifr boolean
	 */
	public void setImportFirstRow(boolean ifr) {
		this.importFirstRow = ifr;
		refreshColumns();
	}

	/**
	 * getTargetColumn
	 * 
	 * @param colIndex integer
	 * @return SourceCSVColumnConfig
	 */
	public SourceCSVColumnConfig getTargetColumn(int colIndex) {
		if (colIndex >= columnConfig.size()) {
			return null;
		}
		return columnConfig.get(colIndex);
	}

	public List<SourceCSVColumnConfig> getColumnConfigs() {
		return new ArrayList<SourceCSVColumnConfig>(columnConfig);
	}

	public List<String[]> getPreviewData() {
		return new ArrayList<String[]>(previewData);
	}

	/***
	 * Set preview data and initialize columns
	 * 
	 * @param data List<String[]>
	 */
	public void setPreviewData(List<String[]> data) {
		previewData.clear();
		if (CollectionUtils.isEmpty(data)) {
			columnConfig.clear();
			return;
		}
		previewData.addAll(data);
		refreshColumns();
	}

	/**
	 * refreshColumns
	 * 
	 */
	private void refreshColumns() {
		if (CollectionUtils.isEmpty(previewData)) {
			return;
		}
		String[] header;
		if (isImportFirstRow()) {
			header = new String[previewData.get(0).length];
			for (int i = 0; i < header.length; i++) {
				header[i] = "col" + (i + 1);
			}
		} else {
			header = previewData.get(0);
		}
		for (int i = 0; i < header.length; i++) {
			String ss = header[i];
			SourceCSVColumnConfig scc = getTargetColumn(i);
			if (scc == null) {
				scc = new SourceCSVColumnConfig();
				scc.setName(ss);
				scc.setTarget(StringUtils.lowerCase(ss));
				scc.setCreate(true);
				scc.setReplace(true);
				columnConfig.add(scc);
			} else {
				scc.setName(ss);
			}
		}
	}

	/**
	 * Change the csv target and auto mapping.
	 * 
	 * @param tblInTarget Table
	 */
	public void changeTarget(Table tblInTarget) {
		if (tblInTarget == null) {
			return;
		}
		setTarget(tblInTarget.getName());
		if (columnConfig.isEmpty()) {
			return;
		}
		final List<Column> tcolumns = tblInTarget.getColumns();
		List<String> colName = new ArrayList<String>();
		for (Column col : tcolumns) {
			colName.add(col.getName());
		}
		if (isImportFirstRow()) {
			for (SourceCSVColumnConfig scc : columnConfig) {
				if (!colName.isEmpty()) {
					scc.setTarget(colName.get(0));
					colName.remove(0);
				}
			}
			return;
		}
		for (SourceCSVColumnConfig sccc : columnConfig) {
			final String tcolName = StringUtils.lowerCase(sccc.getName());
			Column tc = tblInTarget.getColumnByName(tcolName);
			if (tc != null) {
				sccc.setTarget(tcolName);
				colName.remove(tcolName);
			}
		}
		for (SourceCSVColumnConfig sccc : columnConfig) {
			Column tc = tblInTarget.getColumnByName(StringUtils.lowerCase(sccc.getName()));
			if (tc != null) {
				continue;
			}
			if (!colName.isEmpty()) {
				sccc.setTarget(colName.get(0));
				colName.remove(0);
			}
		}
	}

	/**
	 * Add column information
	 * 
	 * @param sccc SourceCSVColumnConfig
	 */
	public void addColumn(SourceCSVColumnConfig sccc) {
		columnConfig.add(sccc);
	}
}
