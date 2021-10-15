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
package com.cubrid.cubridmigration.ui.history.handler;

import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorPart;
import org.eclipse.ui.IPersistableElement;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.ui.history.MigrationHistoryEditorPart;

/**
 * 
 * Open migration history management editor part handler for menu and command.
 * 
 * @author Kevin Cao
 */
public class MigrationHistoryHandler extends
		AbstractHandler {

	/**
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private final class MigrationHistoryManagerEditorInput implements
			IEditorInput {

		/**
		 * @param adapter class
		 * @return null will be returned
		 */
		@SuppressWarnings("rawtypes")
		public Object getAdapter(Class adapter) {
			return null;
		}

		public String getToolTipText() {
			return "Migration History";
		}

		public IPersistableElement getPersistable() {
			return null;
		}

		public String getName() {
			return "Migration History";
		}

		public ImageDescriptor getImageDescriptor() {
			return null;
		}

		/**
		 * @return false always be returned.
		 */
		public boolean exists() {
			return false;
		}

	}

	public static final String ID = MigrationHistoryHandler.class.getName();
	private final IEditorInput input = new MigrationHistoryManagerEditorInput();

	/**
	 * Override Action's method
	 * 
	 * @param event ExecutionEvent
	 * @return null
	 * @throws ExecutionException ex
	 */
	public Object execute(ExecutionEvent event) throws ExecutionException {
		try {
			final IWorkbenchPage activePage = PlatformUI.getWorkbench().getActiveWorkbenchWindow().getActivePage();
			final IEditorPart findEditor = activePage.findEditor(input);
			if (findEditor == null) {
				activePage.openEditor(input, MigrationHistoryEditorPart.ID);
			} else {
				activePage.activate(findEditor);
			}
		} catch (PartInitException e) {
			e.printStackTrace();
		}
		return null;
	}
}