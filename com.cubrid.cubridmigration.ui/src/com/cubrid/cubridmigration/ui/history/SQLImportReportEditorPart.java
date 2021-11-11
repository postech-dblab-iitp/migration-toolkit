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
package com.cubrid.cubridmigration.ui.history;

import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorSite;
import org.eclipse.ui.PartInitException;

import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.ui.history.tableviewer.FileSourceMigrationResultOverviewLabelProvider;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * SQLImportReportEditorPart responses to monitor the migration progress.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-11-30 created by Kevin Cao
 */
public class SQLImportReportEditorPart extends
		CSVImportReportEditorPart {

	public static final String ID = SQLImportReportEditorPart.class.getName();

	private static final String[] TABLE_HEADER_SQL_OVERVIEW = new String[] {Messages.colSQLFile,
			Messages.colExpCount, Messages.colImpCount, Messages.colProgress};

	/**
	 * @param comOverview parent
	 */
	protected void createOverviewTableViewer(Composite comOverview) {
		//Create overview table viewer 
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(TABLE_HEADER_SQL_OVERVIEW);
		tvBuilder.setColumnStyles(COLUMN_STYLES_OVERVIEW);
		tvBuilder.setColumnWidths(COLUMN_WIDTHS_OVERVIEW);
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new FileSourceMigrationResultOverviewLabelProvider());
		tvOverview = tvBuilder.buildTableViewer(comOverview, SWT.BORDER | SWT.FULL_SELECTION);
	}

	/**
	 * @param site IEditorSite
	 * @param input IEditorInput
	 * @throws PartInitException ex
	 */
	public void init(IEditorSite site, IEditorInput input) throws PartInitException {
		super.init(site, input);
		controller.setOverviewTableViewerHeader(TABLE_HEADER_SQL_OVERVIEW);
	}

}
