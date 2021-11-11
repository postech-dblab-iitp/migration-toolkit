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
package com.cubrid.cubridmigration.ui.script.handler;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizardFactory;

/**
 * 
 * load migration script action
 * 
 * @author caoyilin
 * @version 1.0 - 2012-10-18
 */
public class OpenExternalScripHandler extends
		AbstractHandler {
	private static final Logger LOG = LogUtil.getLogger(OpenExternalScripHandler.class);

	public static final String ID = OpenExternalScripHandler.class.getName();

	/**
	 * Override Action's method
	 * 
	 * @param event ExecutionEvent
	 * @return NULL
	 * @throws ExecutionException ex
	 */
	public Object execute(ExecutionEvent event) throws ExecutionException {
		if (MigrationWizardFactory.migrationIsRunning()) {
			return null;
		}
		final FileDialog fileDialog = new FileDialog(
				PlatformUI.getWorkbench().getDisplay().getActiveShell(), SWT.SINGLE);
		fileDialog.setFilterPath(".");
		fileDialog.setFilterExtensions(new String[] {"*.xml", "*.*"});
		fileDialog.setFilterNames(new String[] {"*.xml", "*.*"});

		final String migrationFileName = fileDialog.open();

		if (StringUtils.isEmpty(migrationFileName)) {
			return null;
		}
		try {
			MigrationWizardFactory.openMigrationScript(migrationFileName);
		} catch (Exception e) {
			Shell shell = PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell();
			UICommonTool.openErrorBox(shell, Messages.errInvalidScriptFile);
			LOG.error(LogUtil.getExceptionString(e));
		}
		return null;
	}
}