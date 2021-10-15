/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
 * Solution.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 */
package com.cubrid.cubridmigration.ui.common.navigator;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.swt.graphics.Image;

import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.navigator.node.DBHostNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.DatabaseNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FunctionNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FunctionsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ProcedureNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ProceduresNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SchemaNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SequenceNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SequencesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.StoredProceduresNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TableNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TablesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TriggerNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TriggersNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ViewNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ViewsNode;
import com.cubrid.cubridmigration.ui.script.MigrationScript;

/**
 * 
 * TreeLabelProvider
 * 
 * @author moulinwang
 * @version 1.0 - 2009-10-13
 */
public class TreeLabelProvider extends
		LabelProvider {

	/**
	 * get Imange
	 * 
	 * @param element Object
	 * @return Image
	 */
	public Image getImage(Object element) {
		String iconPath = "";
		if (element instanceof MigrationScript) {
			iconPath = "icon/exportReport.gif";
		} else if (element instanceof DBHostNode) {
			iconPath = "icon/db/host.png";
		} else if (element instanceof DatabaseNode) {
			iconPath = "icon/db/DB.png";
		} else if (element instanceof SchemaNode) {
			iconPath = "icon/db/schema.png";
		} else if (element instanceof TablesNode) {
			iconPath = "icon/db/tables.png";
		} else if (element instanceof ViewsNode) {
			iconPath = "icon/db/views.png";
		} else if (element instanceof TableNode) {
			iconPath = "icon/db/table.png";
		} else if (element instanceof ViewNode) {
			iconPath = "icon/db/view.png";
		} else if (element instanceof TriggersNode) {
			iconPath = "icon/db/trigger_group.png";
		} else if (element instanceof TriggerNode) {
			iconPath = "icon/db/trigger_item.png";
		} else if (element instanceof SequencesNode) {
			iconPath = "icon/db/serial_group.png";
		} else if (element instanceof SequenceNode) {
			iconPath = "icon/db/serial_item.png";
		} else if (element instanceof ProceduresNode
				|| element instanceof FunctionsNode) {
			iconPath = "icon/db/folder.png";
		} else if (element instanceof ProcedureNode) {
			iconPath = "icon/db/procedure_sp_item.png";
		} else if (element instanceof FunctionNode) {
			iconPath = "icon/db/procedure_func_item.png";
		} else if (element instanceof StoredProceduresNode) {
			iconPath = "icon/db/procedure_group.png";
		}

		if (StringUtils.isNotBlank(iconPath)) {
			return MigrationUIPlugin.getImage(iconPath.trim());
		}

		return super.getImage(element);
	}

	/**
	 * get Text
	 * 
	 * @param element Object
	 * @return String
	 */
	public String getText(Object element) {
		if (element instanceof MigrationScript) {
			return ((MigrationScript) element).getName();
		}
		if (element instanceof ICUBRIDNode) {
			return ((ICUBRIDNode) element).getLabel();
		}
		return super.getText(element);
	}
}
