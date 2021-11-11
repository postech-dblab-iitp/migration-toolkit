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
package com.cubrid.cubridmigration.ui.wizard.editor;

import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;

import com.cubrid.common.ui.swt.table.ObjectArrayRowTableLabelProvider;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.event.ExportSQLEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportSQLsEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.ui.history.SQLImportReportEditorPart;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.editor.controller.FileMigrationProgressUIController;

/**
 * MigrationProgressEditorPart responses to monitor the migration progress.
 * 
 * @author Kevin Cao
 */
public class SQLProgressEditorPart extends
		CSVProgressEditorPart {
	public static final String ID = SQLProgressEditorPart.class.getName();

	/**
	 * @param parent Composite
	 */
	protected void createProgressTableViewer(final Composite parent) {
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.colSQLFile, Messages.colExportedCount,
				Messages.colImportedCount});
		tvBuilder.setColumnWidths(new int[] {550, 150, 150});
		tvBuilder.setColumnStyles(new int[] {SWT.LEFT, SWT.RIGHT, SWT.RIGHT});
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new ObjectArrayRowTableLabelProvider());
		tvProgress = tvBuilder.buildTableViewer(parent, SWT.BORDER | SWT.FULL_SELECTION);
	}

	/**
	 * Initialize with FileMigrationProgressUIController
	 * 
	 * @param cf MigrationConfiguration
	 */
	protected void initUIController(MigrationConfiguration cf) {
		controller = new FileMigrationProgressUIController();
		controller.setConfig(cf);
		controller.setReportEditorPartId(SQLImportReportEditorPart.ID);
	}

	/**
	 * Update the export count of SQL file
	 * 
	 * @param event ImportSQLsEvent
	 */
	protected void updateExportedCountInTableViewer(MigrationEvent event) {
		ExportSQLEvent ere = (ExportSQLEvent) event;
		String[] item = controller.updateTableExpData(ere.getSourceSQLFile(), ere.getRecordCount());
		tvProgress.refresh(item);
	}

	/**
	 * Update import count of SQL file
	 * 
	 * @param event ImportSQLsEvent
	 */
	protected void updateImportedCountInTableViewer(MigrationEvent event) {
		ImportSQLsEvent ire = (ImportSQLsEvent) event;
		String[] item = controller.updateTableImpData(ire.getSqlFile(), ire.getRecordCount());
		tvProgress.refresh(item);
	}

	/**
	 * Update Total Imported Count Label
	 * 
	 * @param event ImportSQLsEvent
	 */
	protected void updateTotalImportedCount(MigrationEvent event) {
		ImportSQLsEvent ire = (ImportSQLsEvent) event;
		long imp = ire.getRecordCount();
		if (imp > 0) {
			lblTotalRecord.setText(Long.toString(Long.valueOf(lblTotalRecord.getText()) + imp));
		}
	}

}
