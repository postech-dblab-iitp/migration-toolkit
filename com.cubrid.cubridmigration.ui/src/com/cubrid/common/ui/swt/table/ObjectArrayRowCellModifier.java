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
package com.cubrid.common.ui.swt.table;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ComboBoxCellEditor;
import org.eclipse.jface.viewers.ICellEditorValidator;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.common.ui.listener.KeyStateMaskAdapter;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;

/**
 * ObjectArrayCellModifier Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-8-14 created by Kevin Cao
 */
public class ObjectArrayRowCellModifier implements
		ICellModifier {
	protected CellEditor[] cellEditors;
	protected ICellEditorValidator[] verifiers;
	protected KeyStateMaskAdapter stateMaskListener;

	public ObjectArrayRowCellModifier() {
	}

	/**
	 * Can modify
	 * 
	 * @param element to be edit. TableItem
	 * @param property column property
	 * @return true by default
	 */
	public boolean canModify(Object element, String property) {
		return true;
	}

	/**
	 * Retrieves the cell value
	 * 
	 * @param element the table item data
	 * @param property string of column index
	 * 
	 * @return value of column
	 */
	public Object getValue(Object element, String property) {
		Object[] setc = (Object[]) element;
		return setc[Integer.parseInt(property)];
	}

	/**
	 * Modify cell element and table item
	 * 
	 * @param element Object
	 * @param property String
	 * @param value Object
	 */
	public final void modify(Object element, String property, Object value) {
		TableItem ti = (TableItem) element;
		Object[] setc = (Object[]) ti.getData();
		final int idx = Integer.parseInt(property);
		if (verifiers != null && verifiers[idx] != null) {
			String error = verifiers[idx].isValid(value);
			if (StringUtils.isNotEmpty(error)) {
				return;
			}
		}
		final Object data = ti.getParent().getData(CompositeUtils.LAST_SELECTION);
		TableItem lastModified = data == null ? null : (TableItem) data;
		if (stateMaskListener == null || (!stateMaskListener.isShift() || lastModified == null)) {
			//lastModified = ti;
			modify(ti, setc, idx, value);
			updateColumnImage(value, ti, idx);
			return;
		}
		//If shift key pressed, batch update the items from last modified to current selected
		int start = ti.getParent().indexOf(lastModified);
		int end = ti.getParent().indexOf(ti);
		//lastModified = ti;
		if (start > end) {
			int temp = start;
			start = end;
			end = temp;
		}
		for (int i = start; i <= end; i++) {
			TableItem tempTi = ti.getParent().getItem(i);
			Object[] obj = (Object[]) tempTi.getData();
			modify(tempTi, obj, idx, value);
		}
		updateColumnImage(value, ti, idx);
	}

	/**
	 * Update the columns image
	 * 
	 * @param value Object
	 * @param ti TableItem
	 * @param idx int
	 */
	public void updateColumnImage(Object value, TableItem ti, final int idx) {
		if (!(value instanceof Boolean)) {
			return;
		}
		boolean flag = false;
		if ((Boolean) value) {
			flag = true;
		} else {
			for (TableItem item : ti.getParent().getItems()) {
				Object[] values = (Object[]) item.getData();
				if ((Boolean) values[idx]) {
					flag = true;
					break;
				}
			}
		}
		ti.getParent().getColumn(idx).setImage(CompositeUtils.getCheckImage(flag));
	}

	/**
	 * Modify cell element and table item
	 * 
	 * @param ti TableItem
	 * @param element Object[]
	 * @param columnIdx int
	 * @param value Object
	 * 
	 */
	protected void modify(TableItem ti, Object[] element, int columnIdx, Object value) {
		element[columnIdx] = value;
		if (value instanceof Boolean) {
			ti.setImage(columnIdx, CompositeUtils.getCheckImage((Boolean) value));
		} else if (value instanceof String) {
			ti.setText(columnIdx, value.toString());
		} else if (cellEditors[columnIdx] instanceof ComboBoxCellEditor) {
			ComboBoxCellEditor editor = (ComboBoxCellEditor) cellEditors[columnIdx];
			ti.setText(columnIdx, editor.getItems()[Integer.valueOf(value.toString())]);
		}
	}

	public void setStateMaskListener(KeyStateMaskAdapter stateMaskListener) {
		this.stateMaskListener = stateMaskListener;
	}

	public void setCellEditors(CellEditor[] cellEditors) {
		this.cellEditors = cellEditors;
	}

	public void setVerifiers(ICellEditorValidator[] verifiers) {
		this.verifiers = verifiers;
	}

}
