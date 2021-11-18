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

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.swt.widgets.Display;

import com.cubrid.common.ui.navigator.IItemModelOfGroupProvider;
import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.MigrationTemplateParser;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.dialog.EditScriptDialog;

/**
 * 
 * MigrationScriptManager with Singleton, it responses to manage migration
 * scripts.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-6-19 created by Kevin Cao
 */
public final class MigrationScriptManager implements
		IItemModelOfGroupProvider,
		Serializable {

	private static final long serialVersionUID = 703425171902805087L;

	private static final Logger LOGGER = LogUtil.getLogger(MigrationScriptManager.class);
	private static final String MIGRATION_SCRIPTS_LIST = "/migration_scripts.list";
	public static final String MIGRATION_SCRIPTS_GRP = "/script_group.list";

	private static MigrationScriptManager msm = null;

	/**
	 * Retrieves the singleton instance of this class.
	 * 
	 * @return MigrationScriptManager
	 */
	public static MigrationScriptManager getInstance() {
		if (msm == null) {
			throw new RuntimeException("Migration script manager was not initialized.");
		}
		return msm;
	}

	/**
	 * Initialize singleton instance, call before application started.
	 * 
	 */
	public static void initialize() {
		//Initialize the migration scripts
		synchronized (MigrationScriptManager.class) {
			if (msm != null) {
				return;
			}
			msm = new MigrationScriptManager();
			msm.loadScripts();
		}

	}

	private final ArrayList<MigrationScript> scripts = new ArrayList<MigrationScript>();

	private final List<MigrationScriptManagerListener> listeners = new ArrayList<MigrationScriptManagerListener>();

	private MigrationScriptManager() {
		//Do nothing
	}

	/**
	 * Add a listener which will be called when save method was called.
	 * 
	 * @param listener to be added
	 */
	public void addListener(MigrationScriptManagerListener listener) {
		listeners.add(listener);
	}

	/**
	 * Copy migration script
	 * 
	 * @param text name of script
	 * @param sourceScript to be copied
	 */
	public void copyScript(String text, MigrationScript sourceScript) {
		try {
			MigrationScript ms = (MigrationScript) sourceScript.clone();
			String configFile = getConfigFileName(String.valueOf(System.currentTimeMillis()));
			String configFullPath = PathUtils.getScriptDir() + File.separatorChar + configFile;
			CUBRIDIOUtils.mergeFile(sourceScript.getAbstractConfigFileName(), configFullPath);
			ms.setConfigFileName(configFile);
			ms.setName(text);
			addScript(ms);
			save();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Add script to list
	 * 
	 * 
	 * @param ms to be added
	 */
	private void addScript(MigrationScript ms) {
		scripts.add(ms);
		//addNode(ms);
	}

	/**
	 * Export script to a xml file.
	 * 
	 * @param script to be exported
	 * @param configFile to be saved.
	 * @throws IOException if file error.
	 */
	public void exportScript(MigrationScript script, String configFile) throws IOException {
		final File file = new File(configFile);
		if (file.exists()) {
			PathUtils.deleteFile(file);
		}
		MigrationConfiguration config = MigrationTemplateParser.parse(script.getAbstractConfigFileName());
		//synchronized configuration name with script name
		config.setName(script.getName());
		MigrationTemplateParser.save(config, configFile, false);
	}

	/**
	 * Generate configuration file name
	 * 
	 * @param name long
	 * @return configuration file name
	 */
	protected String getConfigFileName(String name) {
		return name + ".script";
	}

	/**
	 * A copy of script list
	 * 
	 * @return List<MigrationScript>
	 */
	public List<MigrationScript> getScripts() {
		return new ArrayList<MigrationScript>(scripts);
	}

	/**
	 * Import a XML migration script
	 * 
	 * @param configFile to be import
	 * 
	 * @return is successfully
	 */
	public boolean importScript(String configFile) {
		MigrationConfiguration config = MigrationTemplateParser.parse(configFile);
		if (nameExists(config.getName(), null)) {
			String name = EditScriptDialog.getMigrationScriptName(
					Display.getDefault().getActiveShell(), config.getName());
			if (StringUtils.isBlank(name)) {
				return false;
			}
			config.setName(name);
		}
		newScript(config, config.getOfflineSrcCatalog() != null);
		return true;
	}

	/**
	 * Load scripts from local configuration file.
	 * 
	 */
	private void loadScripts() {
		try {
			File scriptFile = new File(PathUtils.getScriptDir() + File.separatorChar
					+ MIGRATION_SCRIPTS_LIST);
			XMLDecoder xe = new XMLDecoder(new FileInputStream(scriptFile));
			try {
				@SuppressWarnings("unchecked")
				List<MigrationScript> list = (List<MigrationScript>) xe.readObject();
				if (CollectionUtils.isNotEmpty(list)) {
					MigrationScript msTask = null;
					for (MigrationScript ms : list) {
						if (ms == null) {
							continue;
						}
						//Change abstract path to Relative path
						ms.setConfigFileName(new File(ms.getConfigFileName()).getName());
						addScript(ms);
						if (ms.getCronPatten() != null) {
							msTask = ms;
						}
					}
					if (msTask != null) {
						MigrationScriptSchedulerManager.addReservation(msTask);
					}
				}
			} finally {
				xe.close();
			}
		} catch (Exception ex) {
			LOGGER.error("", ex);
		}
	}

	/**
	 * Check if the name is validate.
	 * 
	 * @param name new name
	 * @param script the script which will be named.
	 * @return true if other script has the name.
	 */
	public boolean nameExists(String name, MigrationScript script) {
		for (MigrationScript sp : scripts) {
			if (sp.getName().equalsIgnoreCase(name) && sp != script) {
				return true;
			}
		}
		return false;
	}

	/**
	 * A new migration script will be added.
	 * 
	 * @param config MigrationConfiguration to be added
	 * @param saveSchema if save the schema information into output script file
	 * @return new migration script
	 */
	public MigrationScript newScript(MigrationConfiguration config, boolean saveSchema) {
		final MigrationScript ms = new MigrationScript();
		ms.setName(config.getName());
		String configFile = getConfigFileName(String.valueOf(System.currentTimeMillis()));
		ms.setConfigFileName(configFile);
		MigrationTemplateParser.save(config, ms.getAbstractConfigFileName(), saveSchema);
		addScript(ms);
		save();
		return ms;
	}

	/**
	 * Remove script, the save() method should be called after this method was
	 * called.
	 * 
	 * @param obj to be removed.
	 */
	public void remove(MigrationScript obj) {
		scripts.remove(obj);
		PathUtils.deleteFile(new File(obj.getAbstractConfigFileName()));
		//deleteNode(obj);
	}

	/**
	 * Remove listeners
	 * 
	 * @param listener to be removed.
	 */
	public void removeListener(MigrationScriptManagerListener listener) {
		listeners.remove(listener);
	}

	/**
	 * Save object to file
	 * 
	 */
	public void save() {
		try {
			File scriptFile = new File(PathUtils.getScriptDir() + File.separatorChar
					+ MIGRATION_SCRIPTS_LIST);
			XMLEncoder xe = new XMLEncoder(new FileOutputStream(scriptFile.getCanonicalPath()));
			try {
				xe.writeObject(scripts);
				xe.flush();
			} finally {
				xe.close();
			}
			for (MigrationScriptManagerListener lst : listeners) {
				lst.scriptChanged();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Check the script's name
	 * 
	 * @param name to be checked
	 * @param script MigrationScript
	 * @return error message
	 */
	public String checkScriptName(String name, MigrationScript script) {
		if (StringUtils.isBlank(name)) {
			return Messages.msgErrEmptyScriptName;
		}
		if (nameExists(name, script)) {
			return Messages.msgErrDuplicatedScriptName;
		}
		return null;
	}

	/**
	 * @return copy of scripts list
	 */
	public List<Object> getItems() {
		return new ArrayList<Object>(scripts);
	}

	/**
	 * Change the display order by migration script
	 * 
	 * @param ms to be reorder
	 * @param selected List<MigrationScript>.
	 * @param isBefore true if insert before ms.
	 */
	public void changeOrder(MigrationScript ms, List<MigrationScript> selected, boolean isBefore) {
		//Remove to be re-ordered object.
		for (MigrationScript tobe : selected) {
			scripts.remove(tobe);
		}
		int oldIdx = ms == null ? scripts.size() : scripts.indexOf(ms);
		oldIdx = isBefore ? oldIdx : (oldIdx + 1);
		oldIdx = Math.max(0, oldIdx);
		int idx = Math.min(oldIdx, scripts.size());
		scripts.addAll(idx, selected);
		save();
	}
}
