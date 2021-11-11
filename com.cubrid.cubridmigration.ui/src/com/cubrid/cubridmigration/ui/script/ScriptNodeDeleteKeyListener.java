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
package com.cubrid.cubridmigration.ui.script;

import java.text.MessageFormat;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import com.cubrid.cubridmigration.ui.common.navigator.node.MigrationScriptNode;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * ScriptNodeDeleteKey
 * 
 * @author Kevin Cao
 * 
 */
final class ScriptNodeDeleteKeyListener extends
		KeyAdapter {

	/**
	 * @param ev key event
	 */
	public void keyPressed(KeyEvent ev) {
		Tree tree = (Tree) ev.getSource();
		//Delete key
		if (ev.keyCode != 127 || tree.getSelectionCount() == 0) {
			return;
		}

		TreeItem[] objects = tree.getSelection();
		StringBuffer sb = new StringBuffer();
		int i = 0;
		for (TreeItem obj : objects) {
			if (!(obj.getData() instanceof MigrationScriptNode)) {
				continue;
			}
			MigrationScript spt = ((MigrationScriptNode) obj.getData()).getScript();
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(spt.getName());
			i++;
			if (i == 3 && objects.length > 3) {
				sb.append(" ...");
				break;
			}
		}
		if (!MessageDialog.openConfirm(Display.getDefault().getActiveShell(),
				Messages.titleConfirmDelete,
				MessageFormat.format(Messages.msgConfirmDelete, sb.toString()))) {
			return;
		}
		for (TreeItem obj : objects) {
			if (obj.getData() instanceof MigrationScriptNode) {
				MigrationScriptManager.getInstance().remove(
						((MigrationScriptNode) obj.getData()).getScript());
			}
		}
		MigrationScriptManager.getInstance().save();
	}
}