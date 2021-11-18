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

import org.eclipse.jface.viewers.ITableColorProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;

/**
 * BaseTableLabelProvider is the abstract class of TableLabelProvider.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-10 created by Kevin Cao
 */
public class BaseTableLabelProvider extends
		LabelProvider implements
		ITableLabelProvider,
		ITableColorProvider {

	/**
	 * Retrieves the error color
	 * 
	 * @return red color
	 */
	protected Color getErrorColor() {
		return Display.getDefault().getSystemColor(SWT.COLOR_RED);
	}

	/**
	 * Default return null
	 * 
	 * @param element to be display.
	 * @param columnIndex is the index of column. Begin with 0.
	 * @return null
	 */
	public Image getColumnImage(Object element, int columnIndex) {
		return null;
	}

	/**
	 * Retrieves the column's text by column index
	 * 
	 * @param element to be displayed.
	 * @param columnIndex is the index of column. Begin with 0.
	 * @return String to be filled in the column.
	 */
	public String getColumnText(Object element, int columnIndex) {
		return null;
	}

	/**
	 * Provides a foreground color for the given element.
	 * 
	 * @param element the element
	 * @param columnIndex the zero-based index of the column in which the color
	 *        appears
	 * @return the foreground color for the element, or <code>null</code> to use
	 *         the default foreground color
	 */
	public Color getForeground(Object element, int columnIndex) {
		return null;
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
		return null;
	}

}
