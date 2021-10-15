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
package com.cubrid.cubridmigration.ui.common.navigator.node;

import com.cubrid.common.ui.navigator.DefaultCUBRIDNode;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Table;

/**
 * 
 * TableNode
 * 
 * @author moulinwang
 * @version 1.0 - 2009-10-13
 */
public class ColumnNode extends
		DefaultCUBRIDNode {
	Column column;

	public Column getColumn() {
		return column;
	}

	public void setColumn(Column column) {
		this.column = column;
	}

	/**
	 * The constructor
	 * 
	 * @param id String
	 * @param label String
	 */
	public ColumnNode(String id, String label) {
		super(id, label, "icon/db/table_column_item.png");
		setType(CubridNodeType.Column);
		setContainer(false);
		setEditorId(null);
	}

	/**
	 * 
	 * @return node is PK column
	 */
	public boolean isColumnNodePK() {
		Column column = this.getColumn();
		TableNode tableNode = (TableNode) (this.getParent().getParent());
		Table table = tableNode.getTable(); // get table
		// check the pk column whether equal this node
		if (table.getPk() != null) {
			PK pk = table.getPk();
			for (String pkColumn : pk.getPkColumns()) {
				if (column.getName().equalsIgnoreCase(pkColumn)) {
					return true;
				}
			}
		}
		return false;
	}
}
