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

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.internal.SWTEventListener;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.TypedListener;

import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * If the table's style has CHECK option, add this listener to the column 0's
 * selection listener list.
 * 
 * @author Kevin Cao
 * 
 */
@SuppressWarnings("restriction")
public class CheckStyleTableSelectionListener extends
		SelectionAdapter {

	private boolean selectAll = true;

	public static void addListenerToCheckStyleTable(Table table, boolean initSelectionStatus) {
		CheckStyleTableSelectionListener checkedColumnListener = new CheckStyleTableSelectionListener(
				initSelectionStatus);
		table.getColumn(0).addSelectionListener(checkedColumnListener);
		refreshSelectAllStatus(table, initSelectionStatus);
	}

	private CheckStyleTableSelectionListener(boolean initSelectionStatus) {
		this.selectAll = initSelectionStatus;
	}

	public void widgetSelected(final SelectionEvent event) {
		TableColumn column = (TableColumn) event.getSource();
		selectAll = !selectAll;
		Table table = (Table) column.getParent();
		for (TableItem ti : table.getItems()) {
			ti.setChecked(selectAll);
		}
		refreshSelectAllStatus(table, selectAll);
	}

	/**
	 * Set the button's image by status of selectAll variable.
	 * 
	 * @param tv table view
	 * @param selectAll true to select all
	 */
	public static void refreshSelectAllStatus(Table table, boolean selectAll) {
		TableColumn column = table.getColumn(0);
		Image image = selectAll ? MigrationUIPlugin.getImage("icon/checked.gif")
				: MigrationUIPlugin.getImage("icon/unchecked.gif");
		column.setImage(image);
		Listener[] listeners = column.getListeners(SWT.Selection);
		if (listeners == null) {
			return;
		}
		for (Listener listener : listeners) {
			if (listener instanceof TypedListener) {
				SWTEventListener eventListener = ((TypedListener) listener).getEventListener();
				if (eventListener instanceof CheckStyleTableSelectionListener) {
					((CheckStyleTableSelectionListener) eventListener).selectAll = selectAll;
				}
			}
		}
		for (TableItem ti : table.getItems()) {
			ti.setChecked(selectAll);
		}
	}
}