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

import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ComboBoxCellEditor;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.graphics.Image;

import com.cubrid.cubridmigration.ui.common.CompositeUtils;

/**
 * ObjectArrayTableLabelProvider Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-8-14 created by Kevin Cao
 */
public class ObjectArrayRowTableLabelProvider extends
		TableLabelProviderAdapter {

	protected TableViewer tableViewer;

	public ObjectArrayRowTableLabelProvider() {
	}

	/**
	 * Retrieves the column's text
	 * 
	 * @param element Object[]
	 * @param columnIndex int
	 * 
	 * @return text
	 */
	public String getColumnText(Object element, int columnIndex) {
		Object[] setc = (Object[]) element;
		if (setc[columnIndex] instanceof String) {
			return String.valueOf(setc[columnIndex]);
		}
		if (setc[columnIndex] instanceof Integer && tableViewer != null) {
			CellEditor[] cellEditors = tableViewer.getCellEditors();
			if (cellEditors != null && cellEditors[columnIndex] instanceof ComboBoxCellEditor) {
				ComboBoxCellEditor editor = (ComboBoxCellEditor) cellEditors[columnIndex];
				return editor.getItems()[(Integer) setc[columnIndex]];
			}
			return setc[columnIndex].toString();
		}
		return null;
	}

	/**
	 * Retrieves the column's image. If the value is boolean, the check/uncheck
	 * image will be returned
	 * 
	 * @param element Object[]
	 * @param columnIndex int
	 * 
	 * @return image
	 */
	public Image getColumnImage(Object element, int columnIndex) {
		Object[] setc = (Object[]) element;
		if (setc[columnIndex] instanceof Boolean) {
			return CompositeUtils.getCheckImage((Boolean) setc[columnIndex]);
		}
		return null;
	}

	public void setTableViewer(TableViewer tableViewer) {
		this.tableViewer = tableViewer;
	}

}
