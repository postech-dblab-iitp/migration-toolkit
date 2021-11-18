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
package com.cubrid.cubridmigration.ui.preference;

import org.eclipse.jface.preference.IPreferenceNode;
import org.eclipse.jface.preference.PreferenceManager;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.internal.dialogs.WorkbenchPreferenceDialog;

import com.cubrid.cubridmigration.ui.common.UICommonTool;

/**
 * 
 * Configuration dialog
 * 
 * @author pangqiren
 * @version 1.0 - 2009-12-21 created by pangqiren
 */
@SuppressWarnings("restriction")
public class ConfigurationDialog extends
		WorkbenchPreferenceDialog {

	private final String title;

	public ConfigurationDialog(Shell parentShell, PreferenceManager manager,
			String title) {
		super(parentShell, manager);
		this.title = title;
		this.setHelpAvailable(false);
		IPreferenceNode[] rootSubNodes = manager.getRootSubNodes();
		IPreferenceNode removedNode = null;
		for (IPreferenceNode rootSubNode : rootSubNodes) {
			if (rootSubNode.getId().equals(
					"org.eclipse.help.ui.browsersPreferencePage")) {
				removedNode = rootSubNode;
				break;
			}
		}
		if (removedNode != null) {
			manager.remove(removedNode);
		}
	}

	/**
	 * constrainShellSize
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		UICommonTool.centerShell(getShell());
		getShell().setText(title);
	}

	/**
	 * @return dialog's shell style : resize and max
	 */
	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE | SWT.MAX;
	}

	/**
	 * Closing dialog when a text editor is on focus, some SWT errors may be
	 * raised. So set the text invisible first.
	 * 
	 * @return can be closed or not.
	 */
	public boolean close() {
		Control focusControl = Display.getCurrent().getFocusControl();
		if (focusControl instanceof Text) {
			focusControl.setVisible(false);
		}
		return super.close();
	}

}
