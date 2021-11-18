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
package com.cubrid.cubridmigration.ui.script.controller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.osgi.service.prefs.BackingStoreException;

import com.cubrid.common.ui.navigator.GroupNodeManager;
import com.cubrid.common.ui.navigator.ICUBRIDGroupNodeManager;
import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.navigator.node.MigrationScriptNode;
import com.cubrid.cubridmigration.ui.script.MigrationScript;
import com.cubrid.cubridmigration.ui.script.MigrationScriptGroupAndItemNodeFactory;
import com.cubrid.cubridmigration.ui.script.MigrationScriptManager;
import com.cubrid.cubridmigration.ui.script.dialog.GroupSettingDialog;

/**
 * 
 * MigrationScriptExplorerController
 * 
 * @author Kevin Cao
 */
public class MigrationScriptExplorerController {

	private static final Logger LOGGER = LogUtil.getLogger(MigrationScriptExplorerController.class);

	private final IEclipsePreferences node = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
	private final ICUBRIDGroupNodeManager groupNodeManager;

	public MigrationScriptExplorerController() {
		String configFile = PathUtils.getScriptDir() + MigrationScriptManager.MIGRATION_SCRIPTS_GRP;
		groupNodeManager = new GroupNodeManager(configFile,
				new MigrationScriptGroupAndItemNodeFactory(), MigrationScriptManager.getInstance());
	}

	/**
	 * Save preference.
	 * 
	 * @param isShowGroup boolean
	 */
	public void setShowGroupPreference(boolean isShowGroup) {
		node.putBoolean("script.group.display", isShowGroup);
		try {
			node.flush();
		} catch (BackingStoreException e) {
			LOGGER.error("Save preference error.", e);
		}
	}

	/**
	 * @return display group node
	 */
	public boolean getShowGroupPreference() {
		return node.getBoolean("script.group.display", false);
	}

	/**
	 * Open group management dialog
	 */
	public void openGroupSettingDialog() {
		GroupSettingDialog dlg = new GroupSettingDialog(getShell(), groupNodeManager);
		dlg.open();
	}

	/**
	 * Move nodes
	 * 
	 * @param selection target dropped node
	 * @param targetNode the target node on drop
	 * @param isBefore is before current or after
	 */
	@SuppressWarnings("rawtypes")
	public void moveNodes(IStructuredSelection selection, ICUBRIDNode targetNode, boolean isBefore) {
		if (getShowGroupPreference()) {
			List<ICUBRIDNode> selected = new ArrayList<ICUBRIDNode>();

			Iterator iterator = selection.iterator();
			while (iterator.hasNext()) {
				selected.add(((ICUBRIDNode) iterator.next()));
			}
			groupNodeManager.changeItemPosition(targetNode, selected, isBefore);
		} else {
			MigrationScript ms = targetNode == null ? null
					: ((MigrationScriptNode) targetNode).getScript();
			List<MigrationScript> selected = new ArrayList<MigrationScript>();
			Iterator iterator = selection.iterator();
			while (iterator.hasNext()) {
				selected.add(((MigrationScriptNode) iterator.next()).getScript());
			}
			MigrationScriptManager.getInstance().changeOrder(ms, selected, isBefore);
		}
	}

	/**
	 * 
	 * @return current shell
	 */
	private Shell getShell() {
		return Display.getDefault().getActiveShell();
	}

	/**
	 * Refresh
	 */
	public void reloadGroups() {
		groupNodeManager.reloadGroups();
	}

	/**
	 * 
	 * @return the script tree view input
	 */
	public List<? extends ICUBRIDNode> getTreeInput() {
		return getShowGroupPreference() ? groupNodeManager.getAllGroupNodes()
				: groupNodeManager.getAllGroupItems();
	}

	/**
	 * 
	 * @return ICUBRIDGroupNodeManager
	 */
	protected ICUBRIDGroupNodeManager getGgroupNodeManager() {
		return groupNodeManager;
	}
}
