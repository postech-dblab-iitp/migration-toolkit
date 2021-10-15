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

import java.util.Comparator;

import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerSorter;
import org.eclipse.swt.SWT;

import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;

/**
 * BriefComparator compares names and start time.
 * 
 * @author Kevin Cao
 */
public class MigrationHistoryBriefComparator extends
		ViewerSorter implements
		Comparator<Object> {

	private int columnIndex;
	private int sortMode;

	/**
	 * compare
	 * 
	 * @param o1 Object
	 * @param o2 Object
	 * @return compare result
	 */
	public int compare(Object o1, Object o2) {
		MigrationBriefReport m1 = (MigrationBriefReport) o1;
		MigrationBriefReport m2 = (MigrationBriefReport) o2;
		int mode = sortMode == SWT.UP ? 1 : -1;
		if (columnIndex == 0) {
			return mode * m1.getScriptName().compareTo(m2.getScriptName());
		} else if (columnIndex == 1) {
			if (m1.getStartTime() == m2.getStartTime()) {
				return 0;
			}
			int temp = m1.getStartTime() < m2.getStartTime() ? -1 : 1;
			return (mode * temp);
		}
		return 0;
	}

	/**
	 * compare
	 * 
	 * @param viewer Viewer
	 * @param e1 Object
	 * @param e2 Object
	 * @return compare result
	 */
	public int compare(Viewer viewer, Object e1, Object e2) {
		return compare(e1, e2);
	}

	public void setColumnIndex(int ci) {
		this.columnIndex = ci;
	}

	public void setSortMode(int sm) {
		this.sortMode = sm;
	}
}