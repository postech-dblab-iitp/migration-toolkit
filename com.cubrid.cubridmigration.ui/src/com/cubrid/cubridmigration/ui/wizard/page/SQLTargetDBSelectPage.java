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

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.database.JDBCConnectionMgrView;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;
import com.cubrid.cubridmigration.ui.wizard.page.view.AbstractDestinationView;

/**
 * 
 * Select target database connection for SQL
 * 
 * @author fulei caoyilin
 * @version 1.0 - 2011-09-28
 */
public class SQLTargetDBSelectPage extends
		MigrationWizardPage {

	/**
	 * OnlineTargetDBView provides settings exporting to a online CUBRID DB.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-10-9 created by Kevin Cao
	 */
	private class OnlineTargetDBView extends
			AbstractDestinationView {
		private final JDBCConnectionMgrView conMgrView;

		private Button btnWriteErrorRecords;

		private Button btnSingleThreadImporting;

		private OnlineTargetDBView() {
			conMgrView = new JDBCConnectionMgrView(MigrationWizard.getSupportedTarDBTypes(), null);
		}

		/**
		 * Create Controls
		 * 
		 * @param parent Composite
		 */
		public void createControls(Composite parent) {
			if (btnSingleThreadImporting != null) {
				return;
			}
			conMgrView.createControls(parent);

			Composite container2 = new Composite(conMgrView.getComposite(), SWT.NONE);
			container2.setLayout(new GridLayout());
			container2.setLayoutData(new GridData(SWT.FILL, SWT.BOTTOM, true, false));
			Composite container = new Composite(container2, SWT.BORDER);
			container.setLayout(new GridLayout());
			container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
			btnSingleThreadImporting = new Button(container, SWT.CHECK);
			btnSingleThreadImporting.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false,
					4, 1));
			btnSingleThreadImporting.setText(Messages.btnSingleThreadImporting);
			btnSingleThreadImporting.setToolTipText(Messages.tipSingleThreadImporting);

			btnWriteErrorRecords = new Button(container, SWT.CHECK);
			btnWriteErrorRecords.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false, 4,
					1));
			btnWriteErrorRecords.setText(Messages.btnWriteErrorRecords);
		}

		/**
		 * Hide view
		 */
		public void hide() {
			conMgrView.hide();
		}

		/**
		 * initial the page set which option is visiable and updateDialogStatus
		 */
		public void init() {
			setTitle(getMigrationWizard().getStepNoMsg(SQLTargetDBSelectPage.this)
					+ Messages.msgDestSelectOnlineCUBRIDDB);
			setDescription(Messages.msgDestSelectOnlineCUBRIDDBDes);

			final MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
			conMgrView.init(config.getTargetConParams(), null);
			btnWriteErrorRecords.setSelection(config.isWriteErrorRecords());
			btnSingleThreadImporting.setSelection(config.isCreateConstrainsBeforeData());
		}

		/**
		 * Save UI
		 * 
		 * @return true if saving successfully
		 */
		public boolean save() {
			if (conMgrView.getSelectedDCI() == null) {
				MessageDialog.openError(getShell(), Messages.msgError,
						Messages.sourceDBPageErrNoSelectedItem);
				return false;
			}
			Catalog catalog = conMgrView.getCatalog();
			if (null == catalog) {
				return false;
			}
			final MigrationWizard wzd = getMigrationWizard();
			wzd.setTargetCatalog(catalog);
			MigrationConfiguration config = wzd.getMigrationConfig();
			ConnParameters connParameters = catalog.getConnectionParameters();
			//connParameters.setTimeZone(onLineTimezoneCombo.getItem(onLineTimezoneCombo.getSelectionIndex()));
			config.setTargetConParams(connParameters);
			config.setCreateConstrainsBeforeData(btnSingleThreadImporting.getSelection());
			config.setWriteErrorRecords(btnWriteErrorRecords.getSelection());
			return true;

		}

		/**
		 * displayOnlineContainer
		 */
		public void show() {
			conMgrView.show();
		}

	}

	//private static final Logger LOG = LogUtil.getLogger(SQLTargetDBSelectPage.class);
	private OnlineTargetDBView onlineTargetDBView;
	private Composite container;

	/**
	 * Create the wizard
	 */
	public SQLTargetDBSelectPage(String pageName) {
		super(pageName);
	}

	/**
	 * When migration wizard displayed current page.
	 * 
	 * @param event PageChangedEvent
	 */
	protected void afterShowCurrentPage(PageChangedEvent event) {
		final OnlineTargetDBView crtDBView = getCrtDBView();
		crtDBView.createControls(container);
		crtDBView.init();
		if (isFirstVisible) {
			isFirstVisible = false;
		}
		crtDBView.show();
		container.layout();
	}

	/**
	 * Create contents of the wizard
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		container = new Composite(parent, SWT.NONE);
		final GridLayout gridLayoutRoot = new GridLayout();
		container.setLayout(gridLayoutRoot);
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		setControl(container);
		onlineTargetDBView = new OnlineTargetDBView();
	}

	/**
	 * Retrieves current target DB view
	 * 
	 * @return TargetDBView
	 */
	private OnlineTargetDBView getCrtDBView() {
		return onlineTargetDBView;
	}

	/**
	 * When migration wizard will show next page or previous page.
	 * 
	 * @param event PageChangingEvent
	 */
	protected void handlePageLeaving(PageChangingEvent event) {
		// If page is not complete, it should be go to previous page.
		if (!isPageComplete()) {
			return;
		}
		if (isGotoNextPage(event)) {
			event.doit = updateMigrationData();
		}
	}

	/**
	 * Save user input (source database connection information) to export
	 * options.
	 * 
	 * @return true if update success.
	 */
	protected boolean updateMigrationData() {
		return getCrtDBView().save();
	}
}
