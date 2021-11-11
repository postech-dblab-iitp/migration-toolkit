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
package com.cubrid.cubridmigration.ui.script;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.swt.graphics.Image;

import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.navigator.node.MigrationScriptNode;

/**
 * 
 * TreeLabelProvider
 * 
 * @author caoyilin
 * @version 1.0 - 2012-6-26
 */
public class MigrationScriptTreeLabelProvider extends
		LabelProvider {

	/**
	 * get Imange
	 * 
	 * @param element Object
	 * @return Image
	 */
	public Image getImage(Object element) {
		if (!(element instanceof ICUBRIDNode)) {
			return null;
		}
		String iconPath = ((ICUBRIDNode) element).getIconPath();
		if (element instanceof MigrationScriptNode) {
			MigrationScriptNode script = (MigrationScriptNode) element;
			if (script.getScript().getCronPatten() == null) {
				iconPath = "icon/exportReport.gif";
			} else {
				iconPath = "icon/tb/mnu_reservation.gif";
			}

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
		if (!(element instanceof ICUBRIDNode)) {
			return "";
		}
		if (element instanceof ICUBRIDNode) {
			ICUBRIDNode script = (ICUBRIDNode) element;
			return script.getName();
		}
		return super.getText(element);
	}
}
