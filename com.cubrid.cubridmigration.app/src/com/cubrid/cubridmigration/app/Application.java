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
package com.cubrid.cubridmigration.app;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.Platform;
import org.eclipse.equinox.app.IApplication;
import org.eclipse.equinox.app.IApplicationContext;
import org.eclipse.osgi.service.datalocation.Location;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;

/**
 * This class controls all aspects of the application's execution
 * 
 * @author moulinwang,Kevin Cao
 * @version 1.0 - 2009-10-13
 */
public class Application implements
		IApplication {

	/**
	 * start
	 * 
	 * @param context IApplicationContext
	 * @return Object
	 * @throws Exception e
	 */
	public Object start(IApplicationContext context) throws Exception {
		initPaths();
		Display display = PlatformUI.createDisplay();
		try {
			int returnCode = PlatformUI.createAndRunWorkbench(display,
					new ApplicationWorkbenchAdvisor());

			if (returnCode == PlatformUI.RETURN_RESTART) {
				return IApplication.EXIT_RESTART;
			} else {
				return IApplication.EXIT_OK;
			}

		} finally {
			display.dispose();
		}

	}

	/**
	 * Initialize the paths
	 * 
	 * @throws MalformedURLException ex
	 */
	private void initPaths() throws MalformedURLException {
		//Initialize PathUtils: install location, workspace, log
		PathUtils.initPaths(PathUtils.getURLFilePath(Platform.getInstallLocation().getURL()), null,
				null);
		try {
			LogUtil.initLog(PathUtils.getLogDir());
			//Move old to here for MAC OS
		} catch (Exception err) {
			err.printStackTrace();
		}
		moveOldWorkspace();
		try {
			String wsPath = PathUtils.getWorkspace();
			URL localWs = new URL("file", null, wsPath);
			Location instanceLoc = Platform.getInstanceLocation();
			instanceLoc.set(localWs, true);
		} catch (Exception err) {
			err.printStackTrace();
		}
	}

	/**
	 * 
	 * Before this method is called. the install path should be initialized
	 * firstly.
	 * 
	 */
	private void moveOldWorkspace() {
		String[] oldPaths = new String[] {"./workspace", "./report", "./script", "./history",
				"./schemacache"};
		String[] newPaths = new String[] {PathUtils.getWorkspace(), PathUtils.getReportDir(),
				PathUtils.getScriptDir(), PathUtils.getMonitorHistoryDir(),
				PathUtils.getSchemaCacheDir()};
		for (int i = 0; i < oldPaths.length; i++) {
			try {
				File old = new File(oldPaths[i]);
				File nPath = new File(newPaths[i]);
				if (StringUtils.equals(old.getCanonicalPath(), nPath.getCanonicalPath())) {
					continue;
				}
				CUBRIDIOUtils.copyFolder(old, nPath);
				CUBRIDIOUtils.clearFileOrDir(old);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * stop
	 */
	public void stop() {
		final IWorkbench workbench = PlatformUI.getWorkbench();

		if (workbench == null) {
			return;
		}
		final Display display = workbench.getDisplay();
		display.syncExec(new Runnable() {

			public void run() {
				if (!display.isDisposed()) {
					workbench.close();
				}
			}

		});
	}
}
