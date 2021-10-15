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
import com.cubrid.cubridmigration.core.engine.event.ExportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.ui.history.CSVImportReportEditorPart;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.editor.controller.FileMigrationProgressUIController;

/**
 * MigrationProgressEditorPart responses to monitor the migration progress.
 * 
 * @author Kevin Cao
 */
public class CSVProgressEditorPart extends
		MigrationProgressEditorPart {

	public static final String ID = CSVProgressEditorPart.class.getName();

	/**
	 * createProgressTableViewer
	 * 
	 * @param parent Composite
	 */
	protected void createProgressTableViewer(final Composite parent) {
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.colCSVFile, Messages.colExportedCount,
				Messages.colImportedCount});
		tvBuilder.setColumnWidths(new int[] {550, 150, 150});
		tvBuilder.setColumnStyles(new int[] {SWT.LEFT, SWT.RIGHT, SWT.RIGHT});
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new ObjectArrayRowTableLabelProvider());
		tvProgress = tvBuilder.buildTableViewer(parent, SWT.BORDER | SWT.FULL_SELECTION);
	}

	/**
	 * Initialize the UIController
	 * 
	 * @param cf MigrationConfiguration
	 */
	protected void initUIController(MigrationConfiguration cf) {
		controller = new FileMigrationProgressUIController();
		controller.setConfig(cf);
		controller.setReportEditorPartId(CSVImportReportEditorPart.ID);
	}

	/**
	 * Update the export count of CSV file
	 * 
	 * @param event ExportCSVEvent
	 */
	protected void updateExportedCountInTableViewer(MigrationEvent event) {
		ExportCSVEvent ere = (ExportCSVEvent) event;
		String[] item = controller.updateTableExpData(ere.getSourceCSV().getName(),
				ere.getRecordCount());
		tvProgress.refresh(item);
	}

	/**
	 * Update import count of CSV file
	 * 
	 * @param event ImportCSVEvent
	 */
	protected void updateImportedCountInTableViewer(MigrationEvent event) {
		ImportCSVEvent ire = (ImportCSVEvent) event;
		String[] item = controller.updateTableImpData(ire.getCsv().getName(), ire.getRecordCount());
		tvProgress.refresh(item);
	}

	/**
	 * Update the total imported label
	 * 
	 * @param event ImportCSVEvent
	 */
	protected void updateTotalImportedCount(MigrationEvent event) {
		ImportCSVEvent ire = (ImportCSVEvent) event;
		long imp = ire.getRecordCount();
		if (imp > 0) {
			lblTotalRecord.setText(Long.toString(Long.valueOf(lblTotalRecord.getText()) + imp));
		}
	}
}
