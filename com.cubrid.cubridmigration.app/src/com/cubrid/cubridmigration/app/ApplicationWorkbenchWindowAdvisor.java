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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.action.IContributionItem;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.application.ActionBarAdvisor;
import org.eclipse.ui.application.IActionBarConfigurer;
import org.eclipse.ui.application.IWorkbenchWindowConfigurer;
import org.eclipse.ui.application.WorkbenchWindowAdvisor;
import org.eclipse.ui.internal.WorkbenchWindow;

import com.cubrid.common.ui.common.notice.ApplicationType;
import com.cubrid.common.ui.common.notice.NoticeDashboardEditor;
import com.cubrid.common.ui.common.notice.NoticeDashboardInput;
import com.cubrid.common.update.p2.P2Util;
import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.UrlConnUtils;
import com.cubrid.cubridmigration.ui.preference.GeneralPreference;
import com.cubrid.cubridmigration.ui.product.CopyrightDialog;
import com.cubrid.cubridmigration.ui.product.Version;

/**
 * ApplicationWorkbenchWindowAdvisor
 * 
 * @author moulinwang,Kevin Cao
 * @version 1.0 - 2009-10-13
 */
@SuppressWarnings("restriction")
public class ApplicationWorkbenchWindowAdvisor extends
		WorkbenchWindowAdvisor {
	private final static Logger LOG = LogUtil.getLogger(ApplicationWorkbenchWindowAdvisor.class);
	private static final String CLIENT = ApplicationType.CUBRID_MIGRATION_TOOLKIT.getRssName();

	public ApplicationWorkbenchWindowAdvisor(IWorkbenchWindowConfigurer configurer) {
		super(configurer);
	}

	/**
	 * createActionBarAdvisor
	 * 
	 * @param configurer IActionBarConfigurer
	 * @return ActionBarAdvisor
	 */
	public ActionBarAdvisor createActionBarAdvisor(IActionBarConfigurer configurer) {
		return new ApplicationActionBarAdvisor(configurer);
	}

	/**
	 * preWindowOpen
	 */
	public void preWindowOpen() {
		try {
			IEclipsePreferences preference = new InstanceScope().getNode(MigrationUIPlugin.PLUGIN_ID);
			if (preference.getBoolean("btnShowWindowAgain", true)) {
				InputStream is = this.getClass().getClassLoader().getResourceAsStream(
						"com/cubrid/cubridmigration/app/copyright.txt");
				if (is != null) {
					//String installPath = Platform.getInstallLocation().getURL().getPath();
					String copyrignt = CUBRIDIOUtils.readFile(new InputStreamReader(is));
					int buttonId = new CopyrightDialog(
							PlatformUI.getWorkbench().getDisplay().getActiveShell(), copyrignt).open();
					if (IDialogConstants.OK_ID != buttonId) {
						PlatformUI.getWorkbench().close();
					}
				}
			}
		} catch (IOException ex) {
			LOG.error(LogUtil.getExceptionString(ex));
		}
		IWorkbenchWindowConfigurer configurer = getWindowConfigurer();
		configurer.setTitle(Messages.workbenchWindowAdvisorTitle);
		configurer.setShowProgressIndicator(true);
		configurer.setShowMenuBar(true);
		configurer.setInitialSize(new Point(800, 600));
		configurer.setShowCoolBar(true);
		configurer.setShowStatusLine(true);
	}

	/**
	 * postWindowCreate
	 */
	public void postWindowCreate() {
		Shell shell = getWindowConfigurer().getWindow().getShell();
		shell.setMaximized(GeneralPreference.isMaximizeWindowOnStartUp());
	}

	/**
	 * postWindowOpen
	 */
	public void postWindowOpen() {
		Display.getDefault().asyncExec(new Runnable() {

			public void run() {
				try {
					//Remove useless menu: install new software.
					WorkbenchWindow window = (WorkbenchWindow) PlatformUI.getWorkbench().getActiveWorkbenchWindow();
					MenuManager manager2 = window.getMenuBarManager();
					IContributionItem help = manager2.find("help");
					if (help instanceof MenuManager) {
						manager2 = (MenuManager) help;
						manager2.remove("com.cubrid.common.update.p2.menu.install");
						manager2.update(true);
					}

					final IWorkbenchPage activePage = window.getActivePage();
					if (GeneralPreference.isCheckNewInfoOnStartUp()) {
						activePage.openEditor(new NoticeDashboardInput(CLIENT),
								NoticeDashboardEditor.ID);
					}
					UrlConnUtils.isExistNewCubridVersion(Version.buildVersionId, "CUBRID-MIGRATION");
				} catch (Exception ignored) {
					LOG.error("", ignored);
				}
			}
		});
		P2Util.checkForUpdate(GeneralPreference.isAutoCheckUpdate());
	}

	/**
	 * Before the window close.
	 * 
	 * @return true:close;
	 */
	public boolean preWindowShellClose() {
		Shell shell = getWindowConfigurer().getWindow().getShell();
		GeneralPreference.setMaximizeWindowOnStartUp(shell.getMaximized());

		if (GeneralPreference.isAlwaysExit()) {
			return true;
		}
		MessageDialogWithToggle dialog = MessageDialogWithToggle.openOkCancelConfirm(
				getWindowConfigurer().getWindow().getShell(), Messages.titleExitConfirm,
				Messages.msgExistConfirm, Messages.msgToggleExitConfirm, false,
				MigrationUIPlugin.getDefault().getPreferenceStore(),
				GeneralPreference.IS_ALWAYS_EXIT);
		return dialog.getReturnCode() == 0;
	}

}
