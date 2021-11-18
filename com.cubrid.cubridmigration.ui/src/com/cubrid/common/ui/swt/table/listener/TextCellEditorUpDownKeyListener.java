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

import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.TableCursor;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;

/**
 * If the cell is in editing mode, press down/up key, move the editing to
 * next/upper cell. Be careful, a table cursor instance should be set into the
 * table viewer's data with TABLE_CURSOR_KEY key, otherwise this class will not
 * work.
 * 
 * @author Kevin Cao
 * 
 */
public class TextCellEditorUpDownKeyListener extends
		KeyAdapter {
	public static final String TABLE_CURSOR_KEY = "tableCursor";

	private TableViewer tableViewer;

	public TextCellEditorUpDownKeyListener(TableViewer tableViewer) {
		this.tableViewer = tableViewer;
	}

	public void keyPressed(KeyEvent ke) {
		if (tableViewer.getData(TABLE_CURSOR_KEY) == null) {
			return;
		}
		int index;
		int selectionIndex = tableViewer.getTable().getSelectionIndex();
		if (ke.keyCode == SWT.ARROW_DOWN) {
			index = selectionIndex + 1;
			index = Math.min(index, tableViewer.getTable().getItemCount() - 1);
		} else if (ke.keyCode == SWT.ARROW_UP) {
			index = selectionIndex - 1;
			index = Math.max(index, 0);
		} else {
			return;
		}
		TableCursor tc = (TableCursor) tableViewer.getData(TABLE_CURSOR_KEY);
		tc.setSelection(index, tc.getColumn());
		Object elementAt = tableViewer.getElementAt(index);
		tableViewer.editElement(elementAt, tc.getColumn());
	}
}