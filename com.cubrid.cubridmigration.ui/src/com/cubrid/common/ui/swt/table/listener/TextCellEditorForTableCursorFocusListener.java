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

import org.eclipse.swt.custom.TableCursor;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.ui.common.CompositeUtils;

/**
 * If the table viewer is table cursor supported, the cell text editor should be
 * added this listener.
 * 
 * @author Kevin Cao
 * 
 */
public class TextCellEditorForTableCursorFocusListener implements
		FocusListener {

	private final int tableCursorColumnIndex;

	public TextCellEditorForTableCursorFocusListener(int columnIndex) {
		this.tableCursorColumnIndex = columnIndex;
	}

	public void focusLost(FocusEvent event) {
		Text editor = (Text) event.getSource();
		TableCursor tc = (TableCursor) CompositeUtils.getTableCursor((Table) editor.getParent());
		if (tc != null) {
			tc.setVisible(true);
		}
	}

	public void focusGained(FocusEvent event) {
		Text editor = (Text) event.getSource();
		Table parentTable = (Table) editor.getParent();
		TableCursor tc = (TableCursor) CompositeUtils.getTableCursor(parentTable);
		if (tc != null) {
			tc.setSelection(parentTable.getSelectionIndex(), tableCursorColumnIndex);
			tc.setVisible(false);
		}
	}
}