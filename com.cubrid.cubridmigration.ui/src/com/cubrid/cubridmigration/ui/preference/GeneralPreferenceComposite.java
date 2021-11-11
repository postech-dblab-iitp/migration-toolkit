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

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;

import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * General perference page composite
 * 
 * @author pangqiren
 * @version 1.0 - 2009-12-23 created by pangqiren
 */
public class GeneralPreferenceComposite extends
		Composite {

	private Button maximizeWindowBtn = null;
	private Button checkNewInfoBtn = null;
	private Button singleClickButton = null;
	private Button doubleClickButton = null;
	private Button isAlwaysExitBtn = null;
	private Button isAutoCheckUpdateBtn = null;

	/**
	 * The constructor
	 * 
	 * @param parent
	 */
	public GeneralPreferenceComposite(Composite parent) {
		super(parent, SWT.NONE);
		createContent();
	}

	/**
	 * 
	 * Create the page content
	 * 
	 */
	private void createContent() {

		setLayout(new GridLayout());
		setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		maximizeWindowBtn = new Button(this, SWT.CHECK);
		final GridData maximizeWindowBtnGd = new GridData(SWT.FILL, SWT.CENTER, true, false);
		maximizeWindowBtn.setLayoutData(maximizeWindowBtnGd);
		maximizeWindowBtn.setText(Messages.maximizeWindowOnStartUp);

		isAlwaysExitBtn = new Button(this, SWT.CHECK);
		final GridData isAlwaysExitGd = new GridData(SWT.FILL, SWT.CENTER, true, false);
		isAlwaysExitBtn.setLayoutData(isAlwaysExitGd);
		isAlwaysExitBtn.setText(Messages.msgToggleExitConfirm);

		checkNewInfoBtn = new Button(this, SWT.CHECK);
		final GridData checkNewInfoPageGd = new GridData(SWT.FILL, SWT.CENTER, true, false);
		checkNewInfoBtn.setLayoutData(checkNewInfoPageGd);
		checkNewInfoBtn.setText(Messages.btnCheckNewInfo);

		isAutoCheckUpdateBtn = new Button(this, SWT.CHECK);
		final GridData autoCheckUpdateBtnGd = new GridData(SWT.FILL, SWT.CENTER, true, false);
		isAutoCheckUpdateBtn.setLayoutData(autoCheckUpdateBtnGd);
		isAutoCheckUpdateBtn.setText(Messages.auoCheckUpdate);

		final Group openItemWayGrp = new Group(this, SWT.NONE);
		final GridData openItemWayGrpGd = new GridData(SWT.FILL, SWT.CENTER, true, false);
		openItemWayGrp.setLayoutData(openItemWayGrpGd);
		openItemWayGrp.setLayout(new GridLayout());
		openItemWayGrp.setText(Messages.grpOpenMode);
		//Hide this option, it is unused.
		openItemWayGrp.setVisible(false);

		singleClickButton = new Button(openItemWayGrp, SWT.RADIO);
		final GridData singleClickButtonGd = new GridData(SWT.FILL, SWT.CENTER, true, false);
		singleClickButton.setLayoutData(singleClickButtonGd);
		singleClickButton.setText(Messages.btnUseSingleClick);

		doubleClickButton = new Button(openItemWayGrp, SWT.RADIO);
		final GridData doubleClickButtonGd = new GridData(SWT.FILL, SWT.CENTER, true, false);
		doubleClickButton.setLayoutData(doubleClickButtonGd);
		doubleClickButton.setText(Messages.btnUseDoubleClick);
	}

	/**
	 * load query option from preference store
	 */
	public void loadPreference() {
		boolean isMax = GeneralPreference.isMaximizeWindowOnStartUp();
		maximizeWindowBtn.setSelection(isMax);

		boolean isAlwaysExit = GeneralPreference.isAlwaysExit();
		isAlwaysExitBtn.setSelection(isAlwaysExit);

		boolean isShowWelcomePage = GeneralPreference.isCheckNewInfoOnStartUp();
		checkNewInfoBtn.setSelection(isShowWelcomePage);

		boolean isAutoCheckUpdate = GeneralPreference.isAutoCheckUpdate();
		isAutoCheckUpdateBtn.setSelection(isAutoCheckUpdate);

		boolean isUseClickOnce = GeneralPreference.isUseClickOnce();
		singleClickButton.setSelection(isUseClickOnce);
		doubleClickButton.setSelection(!isUseClickOnce);
	}

	/**
	 * 
	 * Save options
	 */
	public void save() {
		boolean isMax = maximizeWindowBtn.getSelection();
		GeneralPreference.setMaximizeWindowOnStartUp(isMax);

		boolean isAlwaysExit = isAlwaysExitBtn.getSelection();
		GeneralPreference.setAlwaysExit(isAlwaysExit);

		boolean isShowWelcomePage = checkNewInfoBtn.getSelection();
		GeneralPreference.setCheckNewInfoOnStartUp(isShowWelcomePage);

		boolean isAutoCheckUpdate = isAutoCheckUpdateBtn.getSelection();
		GeneralPreference.setAutoCheckUpdate(isAutoCheckUpdate);

		boolean isUseClickOnce = singleClickButton.getSelection();
		GeneralPreference.setUseClickOnce(isUseClickOnce);

	}
}
