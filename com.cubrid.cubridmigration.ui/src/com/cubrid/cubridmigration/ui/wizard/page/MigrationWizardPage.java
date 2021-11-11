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
package com.cubrid.cubridmigration.ui.wizard.page;

import org.eclipse.core.runtime.IStatus;
import org.eclipse.jface.dialogs.IPageChangedListener;
import org.eclipse.jface.dialogs.IPageChangingListener;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.widgets.Composite;

import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;

/**
 * MigrationWizardPage is base class of all migration wizard pages classes.
 * 
 * @author Kevin Cao fulei
 * @version 1.0 - 2011-6-30 created by Kevin Cao
 */
public class MigrationWizardPage extends
		WizardPage implements
		IPageChangedListener,
		IPageChangingListener {

	protected boolean isFirstVisible = true;

	protected MigrationWizardPage(String pageName) {
		super(pageName);
	}

	public MigrationWizardPage(String pageName, String title,
			ImageDescriptor titleImage) {
		super(pageName, title, titleImage);
	}

	/**
	 * Retrieves the migration wizard object.
	 * 
	 * @return MigrationWizard
	 */
	public MigrationWizard getMigrationWizard() {
		return ((MigrationWizard) getWizard());
	}

	/**
	 * Handle migration wizard page changed.
	 * 
	 * @param event PageChangedEvent
	 */
	public void pageChanged(PageChangedEvent event) {
		if (event.getSelectedPage() == this) {
			afterShowCurrentPage(event);
		}
	}

	/**
	 * Handle migration wizard page changing.
	 * 
	 * @param event PageChangingEvent
	 */
	public void handlePageChanging(PageChangingEvent event) {
		if (!event.doit) {
			return;
		}
		if (event.getCurrentPage() == this) {
			handlePageLeaving(event);
		}
	}

	/**
	 * When migration wizard displayed current page.
	 * 
	 * @param event PageChangedEvent
	 */
	protected void afterShowCurrentPage(PageChangedEvent event) {
		//Default is doing nothing.
	}

	/**
	 * When migration wizard will show next page or previous page.
	 * 
	 * @param event PageChangingEvent
	 */
	protected void handlePageLeaving(PageChangingEvent event) {
		//Default is doing nothing.
	}

	/**
	 * Retrieves that is in go to next page process.
	 * 
	 * @param event the PageChangingEvent that is fired.
	 * @return true:go to next page.false:go to previous page.
	 */
	protected boolean isGotoNextPage(PageChangingEvent event) {
		return getWizard().getNextPage(this) == event.getTargetPage();
	}

	/**
	 * Create the control of wizard page.
	 * 
	 * @param parent Composite of control.
	 */
	public void createControl(Composite parent) {
		//Do nothing.		
	}

	/**
	 * fire page changed
	 * 
	 * @param status IStatus
	 */
	protected void firePageStatusChanged(IStatus status) {
		if (status == null) {
			return;
		}
		if (status.getSeverity() == IStatus.INFO) {
			setErrorMessage(null);
			setMessage(status.getMessage());
			setPageComplete(true);
		} else {
			setErrorMessage(status.getMessage());
			setPageComplete(false);
		}
	}

	/**
	 * Save options.
	 * 
	 * @return true if update success.
	 */
	protected boolean updateMigrationConfig() {
		return true;
	}
}
