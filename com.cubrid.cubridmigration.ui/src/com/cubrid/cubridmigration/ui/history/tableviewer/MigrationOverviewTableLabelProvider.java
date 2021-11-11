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
package com.cubrid.cubridmigration.ui.history.tableviewer;

import java.text.NumberFormat;

import org.eclipse.swt.graphics.Color;

import com.cubrid.common.ui.swt.table.BaseTableLabelProvider;
import com.cubrid.cubridmigration.core.engine.report.MigrationOverviewResult;

/**
 * Migration overview provider
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-10 created by Kevin Cao
 */
public class MigrationOverviewTableLabelProvider extends
		BaseTableLabelProvider {

	/**
	 * Retrieves the column's text by column index
	 * 
	 * @param element to be displayed.
	 * @param columnIndex is the index of column. Begin with 0.
	 * @return String to be filled in the column.
	 */
	public String getColumnText(Object element, int columnIndex) {
		MigrationOverviewResult mor = (MigrationOverviewResult) element;
		switch (columnIndex) {
		case 0:
			return mor.getObjType();
		case 1:
			return NumberFormat.getIntegerInstance().format(mor.getExpCount());
		case 2:
			return NumberFormat.getIntegerInstance().format(mor.getImpCount());
		case 3:
			long totalCount = Math.max(mor.getTotalCount(), mor.getExpCount());
			return NumberFormat.getIntegerInstance().format(totalCount - mor.getImpCount());
		case 4:
			totalCount = Math.max(mor.getTotalCount(), mor.getExpCount());
			return totalCount == 0 ? "100%" : (Long.toString(100
					* (mor.getExpCount() + mor.getImpCount()) / (totalCount * 2)) + "%");
		default:
			return null;
		}

	}

	/**
	 * Provides a background color for the given element at the specified index
	 * 
	 * @param element the element
	 * @param columnIndex the zero-based index of the column in which the color
	 *        appears
	 * @return the background color for the element, or <code>null</code> to use
	 *         the default background color
	 * 
	 */
	public Color getBackground(Object element, int columnIndex) {
		MigrationOverviewResult mor = (MigrationOverviewResult) element;
		if (mor.getExpCount() != mor.getImpCount() || mor.getTotalCount() != mor.getExpCount()) {
			return getErrorColor();
		}
		return null;
	}
}