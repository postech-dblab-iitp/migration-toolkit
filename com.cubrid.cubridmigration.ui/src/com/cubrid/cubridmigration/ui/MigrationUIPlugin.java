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
package com.cubrid.cubridmigration.ui;

import java.io.File;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.eclipse.core.runtime.Platform;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.osgi.service.datalocation.Location;
import org.eclipse.swt.graphics.Image;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.osgi.framework.BundleContext;

import com.cubrid.common.configuration.jdbc.IJDBCConnecInfo;
import com.cubrid.common.configuration.jdbc.JDBCChangingManager;
import com.cubrid.common.configuration.jdbc.JDBCDriverChangingManager;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.CMTConParamManager;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.ui.database.CMTDriverChangingObserver;
import com.cubrid.cubridmigration.ui.database.JDBCConfigDataManager;
import com.cubrid.cubridmigration.ui.preference.DataTypeMappingUtil;
import com.cubrid.cubridmigration.ui.preference.MigrationConfigPage;
import com.cubrid.cubridmigration.ui.preference.MigrationPreferenceUtils;
import com.cubrid.cubridmigration.ui.script.MigrationScriptManager;

/**
 * The activator class controls the plug-in life cycle
 * 
 * @author moulinwang
 * @version 1.0 - 2009-10-13
 */
public class MigrationUIPlugin extends
		AbstractUIPlugin {
	private static final Logger LOG = LogUtil.getLogger(MigrationUIPlugin.class);
	// The plug-in ID
	public static final String PLUGIN_ID = "com.cubrid.cubridmigration.ui";

	// The shared instance
	private static MigrationUIPlugin plugin;

	/**
	 * The constructor
	 */
	public MigrationUIPlugin() {
		//do nothing
	}

	/**
	 * TODO: some global parameters should be initialized here.
	 * 
	 * @see org.eclipse.ui.plugin.AbstractUIPlugin#start(org.osgi.framework.BundleContext)
	 * @param context BundleContext
	 * @throws Exception e
	 */
	public void start(BundleContext context) throws Exception {
		super.start(context);
		plugin = this;
		initPaths();
		initObservers();
		initTypeMapping();
		initJDBCConnections();
		//Initialize scripts
		MigrationScriptManager.initialize();
		//initCMS();
	}

	/**
	 * initObservers
	 * 
	 */
	private void initObservers() {
		try {
			//Register subjects and observers
			JDBCDriverChangingManager instance = JDBCDriverChangingManager.getInstance();
			instance.registObservor(new CMTDriverChangingObserver());
		} catch (Exception ignored) {
			LOG.error("", ignored);
		}
	}

	/**
	 * initPaths, this method is called when CMT running as a plugin. The work
	 * space directory is exists.
	 */
	private void initPaths() {
		try {
			//If install path is not null, the paths are initialized already.
			if (PathUtils.getInstallPath() == null) {
				Location instanceLoc = Platform.getInstanceLocation();
				PathUtils.initPaths(
						PathUtils.getURLFilePath(Platform.getInstallLocation().getURL()),
						MigrationConfigPage.getTemplateFilePath(),
						PathUtils.getURLFilePath(instanceLoc.getURL()));
			}
		} catch (Exception ignored) {
			LOG.error("", ignored);
		}
	}

	/**
	 * initTypeMapping
	 * 
	 */
	private void initTypeMapping() {
		try {
			//init data type mapping from client setting
			DataTypeMappingUtil.initDataTypeMapping();
			MigrationPreferenceUtils.initMigrationParameters();
		} catch (Exception ignored) {
			LOG.error("", ignored);
		}
	}

	/**
	 * initJDBCConnections
	 * 
	 */
	private void initJDBCConnections() {
		//Initialize JDBC drivers
		JDBCConfigDataManager.loadJdbc();
		//Initialize the JDBC connection manager.
		MigrationUIPlugin defPlg = MigrationUIPlugin.getDefault();
		try {
			File file = defPlg.getStateLocation().append("jdbcconnection.xml").toFile();
			CMTConParamManager instance = CMTConParamManager.getInstance();
			instance.setDefaultFile(file);
			if (file.exists()) {
				instance.loadFromFile(file);
			} else {
				//Backward compatibility
				File file2 = defPlg.getStateLocation().append("connection.xml").toFile();
				instance.loadFromFile(file2);
				instance.save2File();
			}
			//Synchronize from other system: CM/CQB/DAQB and etc...
			List<IJDBCConnecInfo> allConnections = JDBCChangingManager.getInstance().getAllConnections();
			for (IJDBCConnecInfo con : allConnections) {
				ConnParameters cp = ConnParameters.getConParamByInfo(con);
				instance.addConnection(cp, false);
			}
		} catch (Exception e) {
			LOG.error("", e);
		}
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.ui.plugin.AbstractUIPlugin#stop(org.osgi.framework.BundleContext)
	 * @param context BundleContext
	 * @throws Exception e
	 */
	public void stop(BundleContext context) throws Exception {
		plugin = null;
		try {
			FileSystem.closeAll();
			clearTempDir();
		} finally {
			super.stop(context);
		}
	}

	/**
	 * Returns the shared instance
	 * 
	 * @return the shared instance
	 */
	public static MigrationUIPlugin getDefault() {
		return plugin;
	}

	/**
	 * Returns an image descriptor for the image file at the given plug-in
	 * relative path.
	 * 
	 * @param path the path
	 * @return the image descriptor
	 */
	public static ImageDescriptor getImageDescriptor(String path) {
		ImageDescriptor imageDesc = getDefault().getImageRegistry().getDescriptor(path);

		if (imageDesc == null) {
			imageDesc = AbstractUIPlugin.imageDescriptorFromPlugin(PLUGIN_ID, path);
			MigrationUIPlugin.getDefault().getImageRegistry().put(path, imageDesc);
		}
		return imageDesc;
	}

	/**
	 * Returns an image for the image file at the given plug-in relative path.
	 * 
	 * @param path the path
	 * @return the image
	 */
	public static Image getImage(String path) {
		Image image = getDefault().getImageRegistry().get(path);

		if (image == null || image.isDisposed()) {
			final ImageDescriptor imageDesc = AbstractUIPlugin.imageDescriptorFromPlugin(PLUGIN_ID,
					path);
			MigrationUIPlugin.getDefault().getImageRegistry().put(path, imageDesc);
			return MigrationUIPlugin.getDefault().getImageRegistry().get(path);
		}
		return image;
	}

	/**
	 * Clear temp directory.
	 * 
	 */
	private void clearTempDir() {
		try {
			PathUtils.clearTempDir();
		} catch (Exception ex) {
			LOG.error("Clear temporay directory error.", ex);
		}
	}
}
