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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ISelectionProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IViewPart;
import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.common.navigator.node.MigrationScriptNode;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.MigrationScript;
import com.cubrid.cubridmigration.ui.script.MigrationScriptExplorerView;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizardFactory;

/**
 * 
 * BaseMigrationScriptHandler
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-06-25
 */
public class BaseMigrationScriptHandler extends
		AbstractHandler {

	/**
	 * Open migration wizard with script
	 * 
	 * @param event ExecutionEvent
	 * @return NULl
	 * @throws ExecutionException ex
	 */
	public Object execute(ExecutionEvent event) throws ExecutionException {
		if (MigrationWizardFactory.migrationIsRunning()) {
			return null;
		}
		Shell shell = getShell();
		try {
			IStructuredSelection iStructuredSelection = getSelection();
			if (iStructuredSelection.isEmpty()) {
				MessageDialog.openError(shell, Messages.titleExportScript,
						Messages.msgErrNoScriptSelected);
				return null;
			}
			Iterator<?> iterator = iStructuredSelection.iterator();
			List<MigrationScript> scripts = new ArrayList<MigrationScript>();
			while (iterator.hasNext()) {
				Object obj = iterator.next();
				if (obj instanceof MigrationScriptNode) {
					scripts.add(((MigrationScriptNode) obj).getScript());
				}
			}
			handlerScripts(scripts);
		} catch (Exception ex) {
			UICommonTool.openErrorBox(shell, Messages.errInvalidScriptFile);
		}
		return null;
	}

	/**
	 * Retrieves the tree view's selection
	 * 
	 * @return IStructuredSelection
	 */
	protected IStructuredSelection getSelection() {
		TreeViewer selectedObject = (TreeViewer) getSelectionProvider();
		IStructuredSelection iStructuredSelection = (IStructuredSelection) selectedObject.getSelection();
		return iStructuredSelection;
	}

	/**
	 * The tree view
	 * 
	 * @return ISelectionProvider
	 */
	protected ISelectionProvider getSelectionProvider() {
		try {
			IWorkbenchWindow aww = PlatformUI.getWorkbench().getActiveWorkbenchWindow();
			IViewPart viewd = aww.getActivePage().showView(MigrationScriptExplorerView.ID);
			TreeViewer selectedObject = (TreeViewer) viewd.getAdapter(TreeViewer.class);
			return selectedObject;
		} catch (PartInitException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Active shell
	 * 
	 * @return Active shell
	 */
	protected Shell getShell() {
		Shell shell = PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell();
		return shell;
	}

	/**
	 * Handle the selected migration script,To be Override
	 * 
	 * @param scripts List<MigrationScript>
	 */
	protected void handlerScripts(List<MigrationScript> scripts) {
		//To be Override
	}

	/**
	 * If at least one script selected.
	 * 
	 * @return true if >= one script selected.
	 * 
	 */
	protected boolean atLeastOneScriptSelected() {
		IStructuredSelection selection = getSelection();
		Iterator<?> iterator = selection.iterator();
		while (iterator.hasNext()) {
			if (iterator.next() instanceof MigrationScriptNode) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 * @return true if only one script selected
	 */
	protected boolean isSingleScriptSelected() {
		IStructuredSelection selection = getSelection();
		if (selection.isEmpty()) {
			return false;
		}
		Iterator<?> iterator = selection.iterator();
		return (iterator.next() instanceof MigrationScriptNode) && !iterator.hasNext();
	}
}
