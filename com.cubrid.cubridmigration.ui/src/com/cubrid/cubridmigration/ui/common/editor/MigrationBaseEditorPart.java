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
package com.cubrid.cubridmigration.ui.common.editor;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorSite;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.part.EditorPart;

/**
 * MigrationBaseEditorPart is the base editor part class for other editor part.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-11 created by Kevin Cao
 */
public class MigrationBaseEditorPart extends
		EditorPart {

	/**
	 * save
	 * 
	 * @param monitor IProgressMonitor
	 */
	public void doSave(IProgressMonitor monitor) {
		//Do nothing
	}

	/**
	 * save as
	 * 
	 */
	public void doSaveAs() {
		//Do nothing
	}

	/**
	 * Default initialization
	 * 
	 * @param site IEditorSite
	 * @param input IEditorInput
	 * @throws PartInitException if input error
	 */
	public void init(IEditorSite site, IEditorInput input) throws PartInitException {
		setSite(site);
		setInput(input);
		setTitleToolTip(input.getToolTipText());
	}

	/**
	 * Is dirty
	 * 
	 * @return false
	 */
	public boolean isDirty() {
		return false;
	}

	/**
	 * Is save as allowed
	 * 
	 * @return false
	 */
	public boolean isSaveAsAllowed() {
		return false;
	}

	/**
	 * Set focus
	 */
	public void setFocus() {
		//Default do nothing.
	}

	/**
	 * Create part control
	 * 
	 * @param parent of the control
	 * 
	 */
	public void createPartControl(Composite parent) {
		//Default do nothing.
	}

}
