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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.wizard.IWizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;
import com.cubrid.cubridmigration.ui.wizard.page.view.SelectSrcTarTypesView;

/**
 * 
 * New wizard step 1. Select the type of data source and the type of the
 * destination
 * 
 * @author Kevin Cao
 */
public class SelectSrcTarTypesPage extends
		MigrationWizardPage {

	private static final Logger LOG = LogUtil.getLogger(SelectSrcTarTypesPage.class);
	private SelectSrcTarTypesView comSelection;

	public SelectSrcTarTypesPage(String pageName) {
		super(pageName);
		setTitle(Messages.msgSelectMigrationType);
		setDescription(Messages.msgSelectMigrationTypeDes);
	}

	/**
	 * When migration wizard displayed current page.
	 * 
	 * @param event PageChangedEvent
	 */
	protected void afterShowCurrentPage(PageChangedEvent event) {
		try {
			if (isFirstVisible) {
				final MigrationWizard wzd = getMigrationWizard();
				MigrationConfiguration cfg = wzd.getMigrationConfig();
				if (wzd.isLoadMigrationScript()) {
					comSelection.showCfg(cfg.getSourceType(), cfg.getDestType());
				}
				isFirstVisible = false;
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		}
	}

	/**
	 * Create contents of the wizard
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		Composite container = new Composite(parent, SWT.NONE);
		container.setLayout(new GridLayout());
		setControl(container);
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		comSelection = new SelectSrcTarTypesView(container);
		afterShowCurrentPage(null);
	}

	/**
	 * Save the configuration before goto next page
	 * 
	 * @return next page
	 */
	public IWizardPage getNextPage() {
		if (!updateMigrationConfig()) {
			return null;
		}
		return super.getNextPage();
	}

	/**
	 * Save user input (source database connection information) to export
	 * options.
	 * 
	 * @return true if update success.
	 */
	protected boolean updateMigrationConfig() {
		//Warning message : type changing will cause settings reset
		final MigrationWizard wzd = getMigrationWizard();
		if (!wzd.updateSrcTarType(comSelection.getSourceType(), comSelection.getTargetType())) {
			return false;
		}
		final String result = this.comSelection.save();
		if (StringUtils.isNotBlank(result)) {
			MessageDialog.openError(getShell(), Messages.msgError, result);
			return false;
		}
		return true;
	}
}
