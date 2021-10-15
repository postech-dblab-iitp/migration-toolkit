/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
 * Solution.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 */
package com.cubrid.cubridmigration.ui.preference;

import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;

import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * General Preference page
 * 
 * @author pangqiren
 * @version 1.0 - 2009-12-23 created by pangqiren
 */
public class GeneralPreferencePage extends
		PreferencePage implements
		IWorkbenchPreferencePage {
	public final static String ID = "com.cubrid.common.ui.preference.general";
	private GeneralPreferenceComposite container;

	/**
	 * The constructor
	 */
	public GeneralPreferencePage() {
		super(Messages.prefTitleGeneral, null);
		noDefaultAndApplyButton();
	}

	/**
	 * Initializes this preference page for the given workbench.
	 * 
	 * @param workbench the workbench
	 */
	public void init(IWorkbench workbench) {
		//empty
	}

	/**
	 * 
	 * Load the preference page value
	 * 
	 */
	private void loadPreference() {
		container.loadPreference();
	}

	/**
	 * Notifies that the OK button of this page's container has been pressed.
	 * 
	 * @return <code>false</code> to abort the container's OK processing and
	 *         <code>true</code> to allow the OK to happen
	 */
	public boolean performOk() {
		if (!checkValues()) {
			return false;
		}
		if (container != null) {
			container.save();
		}
		return true;
	}

	/**
	 * check input value
	 * 
	 * @return <code>true</code> if check is valid;<code>false</code> otherwise
	 */
	private boolean checkValues() {
		return true;
	}

	/**
	 * Creates and returns the SWT control for the customized body of this
	 * preference page under the given parent composite.
	 * 
	 * @param parent the parent composite
	 * @return the new control
	 */
	protected Control createContents(Composite parent) {
		container = new GeneralPreferenceComposite(parent);
		loadPreference();
		return container;
	}

}
