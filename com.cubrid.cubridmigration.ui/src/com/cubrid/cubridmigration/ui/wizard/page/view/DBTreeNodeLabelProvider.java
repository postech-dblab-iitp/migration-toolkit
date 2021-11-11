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
package com.cubrid.cubridmigration.ui.wizard.page.view;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.viewers.CellLabelProvider;
import org.eclipse.jface.viewers.ViewerCell;

import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.navigator.node.ColumnNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.DatabaseNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FKNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.IndexNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.PKNode;

/**
 * DBTreeNodeLabelProvider Description
 * 
 * @author Kevin Cao
 */
public class DBTreeNodeLabelProvider extends
		CellLabelProvider {

	/**
	 * update
	 * 
	 * @param cell ViewerCell
	 */
	public void update(ViewerCell cell) {
		if (cell.getColumnIndex() != 0) {
			return;
		}
		Object cellElement = cell.getElement();
		if (!(cellElement instanceof ICUBRIDNode)) {
			return;
		}
		ICUBRIDNode cellNode = (ICUBRIDNode) cellElement;
		String txt = cellNode.getLabel();
		String iconPath = cellNode.getIconPath();

		if (cellElement instanceof DatabaseNode) {
			DatabaseNode dbNode = (DatabaseNode) cellElement;
			//txt = getDatabaseString(dbNode);
			txt = dbNode.getName();
		} else if (cellElement instanceof PKNode) {
			PKNode pkNode = (PKNode) cellElement;
			txt = pkNode.getName();
		} else if (cellElement instanceof ColumnNode) {
			ColumnNode columnNode = (ColumnNode) cellElement;
			if (columnNode.isColumnNodePK()) {
				iconPath = "icon/primary_key.png";
			}
			Column column = columnNode.getColumn();
			txt = column.getName() + "(" + column.getShownDataType() + ")";
		} else if (cellElement instanceof IndexNode) {
			Index index = ((IndexNode) cellElement).getIndex();
			if (index.isIndexNodePK()) {
				iconPath = "icon/primary_key.png";
			}
			txt = index.getIndexString();
		} else if (cellElement instanceof FKNode) {
			FK fk = ((FKNode) cellElement).getFk();
			txt = fk.getFKString();
		}
		if (StringUtils.isNotBlank(iconPath)) {
			cell.setImage(MigrationUIPlugin.getImage(iconPath));
		}
		if (StringUtils.isEmpty(txt)) {
			txt = "...";
		}
		cell.setText(txt);
	}
}