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
package com.cubrid.cubridmigration.ui.preference;

import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerSorter;
import org.eclipse.swt.SWT;

import com.cubrid.cubridmigration.core.mapping.model.MapItem;

/**
 * 
 * DataTypeSorter
 * 
 * @author JessieHuang
 * @version 1.0 - 2010-01-22 created by JessieHuang
 */
public class DataTypeSorter extends
		ViewerSorter {

	public static final String SRC_DATATYPE_SORT = "srcDataType";
	public static final String TARGET_DATATYPE_SORT = "targetDataType";
	private final String column;
	private int dir = SWT.DOWN; //NOPMD

	/**
	 * @param column
	 * @param dir
	 */
	public DataTypeSorter(String column, int dir) {
		super();
		this.column = column;
		this.dir = dir;
	}

	/**
	 * compare
	 * 
	 * @param viewer Viewer
	 * @param e1 Object
	 * @param e2 Object
	 * @return int
	 */
	public int compare(Viewer viewer, Object e1, Object e2) {
		int returnValue = 0;

		if (this.column.equalsIgnoreCase(SRC_DATATYPE_SORT)) {
			returnValue = ((MapItem) e1).getSource().getDatatype().compareTo(
					((MapItem) e2).getSource().getDatatype());
		} else if (this.column.equalsIgnoreCase(TARGET_DATATYPE_SORT)) {
			returnValue = ((MapItem) e1).getTarget().getDatatype().compareTo(
					((MapItem) e2).getTarget().getDatatype());
		}

		if (this.dir == SWT.DOWN) {
			returnValue = returnValue * -1;
		}

		return returnValue;
	}

}
