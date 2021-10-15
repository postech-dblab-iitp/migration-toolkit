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
package com.cubrid.common.ui.swt.table.listener;

import java.util.Iterator;

import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.custom.TableCursor;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.cubridmigration.ui.common.CompositeUtils;

/**
 * Let the table's selection follow the table cursor's moving. And let the cell
 * go to editor mode when enter pressed.
 * 
 * @author Kevin Cao
 * 
 */
public class TableCursorSelectionListener extends
		SelectionAdapter {
	private TableViewer tableViewer;

	public TableCursorSelectionListener(TableViewer tableViewer) {
		this.tableViewer = tableViewer;
	}

	public void widgetSelected(SelectionEvent event) {
		TableCursor tc = (TableCursor) event.getSource();
		StructuredSelection sel = (StructuredSelection) tableViewer.getSelection();
		TableItem row = tc.getRow();
		Iterator<?> iterator = sel.iterator();
		while (iterator.hasNext()) {
			//If the table cursor row is in selection, do nothing
			if (row.getData() == iterator.next()) {
				return;
			}
		}
		//The table's selection should follow the table cursor's selection
		tableViewer.setSelection(new StructuredSelection(row.getData()));
		CompositeUtils.setTableLastSelection(tableViewer.getTable());
	}

	/**
	 * Press enter for edit the cell.
	 */
	public void widgetDefaultSelected(SelectionEvent event) {
		TableCursor tc = (TableCursor) event.getSource();
		TableItem row = tc.getRow();
		if (row != null) {
			tableViewer.editElement(row.getData(), tc.getColumn());
		}
	}
}