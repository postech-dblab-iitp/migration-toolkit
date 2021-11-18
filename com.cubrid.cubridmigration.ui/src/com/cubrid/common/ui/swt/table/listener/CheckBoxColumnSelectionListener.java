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

import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.cubridmigration.ui.common.CompositeUtils;

/**
 * CheckBoxColumnSelectionListener
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-8-17 created by Kevin Cao
 */
public class CheckBoxColumnSelectionListener extends
		SelectionAdapter {

	private final int[] casColumns;
	private boolean followTrue;
	private boolean followFalse;

	public CheckBoxColumnSelectionListener() {
		casColumns = null;
	}

	/**
	 * When this cell was changed, some other cells will change according with
	 * this cell's value
	 * 
	 * @param casColumns the column index should be changed
	 * @param followTrue only the new value if true, update the casColumns
	 * @param followFalse only the new value if false, update the casColumns
	 */
	public CheckBoxColumnSelectionListener(int[] casColumns,
			boolean followTrue, boolean followFalse) {
		this.casColumns = casColumns;
		this.followTrue = followTrue;
		this.followFalse = followFalse;
	}

	/**
	 * When table's column clicked
	 * 
	 * @param ex SelectionEvent
	 */
	public void widgetSelected(SelectionEvent ex) {
		if (!(ex.getSource() instanceof TableColumn)) {
			return;
		}
		TableColumn tc = (TableColumn) ex.getSource();
		boolean flag = !CompositeUtils.CHECK_IMAGE.equals(tc.getImage());
		tc.setImage(CompositeUtils.getCheckImage(flag));
		updateCells(tc, flag);

		if (casColumns == null) {
			return;
		}
		if ((flag && followTrue) || (!flag && followFalse)) {
			for (int idx : casColumns) {
				TableColumn col = tc.getParent().getColumn(idx);
				col.setImage(tc.getImage());
				updateCells(col, flag);
			}
		}
	}

	/**
	 * Update the cells of this column
	 * 
	 * @param tc TableColumn
	 * @param checkstatus boolean
	 */
	protected void updateCells(TableColumn tc, boolean checkstatus) {
		int idx = tc.getParent().indexOf(tc);
		for (int i = 0; i < tc.getParent().getItemCount(); i++) {
			TableItem ti = tc.getParent().getItem(i);
			ti.setImage(idx, tc.getImage());
			if (!(ti.getData() instanceof Object[])) {
				continue;
			}
			Object[] obj = (Object[]) ti.getData();
			if (obj[idx] instanceof Boolean) {
				obj[idx] = checkstatus;
			}
		}
	}
}
