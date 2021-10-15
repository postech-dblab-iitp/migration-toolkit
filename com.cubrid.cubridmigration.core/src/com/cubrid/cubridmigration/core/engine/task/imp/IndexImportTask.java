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
package com.cubrid.cubridmigration.core.engine.task.imp;

import java.util.List;

import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.event.IgnoreCreateObjectEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;

/**
 * IndexImportTask Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-10 created by Kevin Cao
 */
public class IndexImportTask extends
		ImportTask {

	private final Table targetTable;

	public IndexImportTask(Table targetTable) {
		this.targetTable = targetTable;
	}

	/**
	 * Execute import operation
	 * 
	 */
	protected void executeImport() {

		for (Index idx : targetTable.getIndexes()) {
			try {
				//If PK and index have same columns, the index should not be created.
				if (isDuplicatedWithPK(idx)) {
					eventHandler.handleEvent(new IgnoreCreateObjectEvent(idx));
					continue;
				}
				importer.createIndex(idx);
			} catch (NormalMigrationException e) {
				eventHandler.handleEvent(new MigrationErrorEvent(e));
			}
		}
	}

	/**
	 * Check if the index is duplicated with table primary key's set up.
	 * 
	 * @param idx Index
	 * @return true if they have same columns
	 */
	private boolean isDuplicatedWithPK(Index idx) {
		if (targetTable.getPk() != null && idx.isUnique()) {
			PK pk = targetTable.getPk();
			List<String> pkCols = pk.getPkColumns();
			List<String> idxCols = idx.getColumnNames();
			if (pkCols.size() > 0 && pkCols.size() == idxCols.size()) {
				boolean flag = true;
				for (String col : pkCols) {
					if (idxCols.indexOf(col) < 0) {
						flag = false;
						break;
					}
				}
				return flag;
			}
		}
		return false;
	}
}
