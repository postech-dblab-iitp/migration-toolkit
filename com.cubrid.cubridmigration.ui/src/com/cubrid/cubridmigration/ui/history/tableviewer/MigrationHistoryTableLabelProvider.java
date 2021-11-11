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

import java.util.Date;

import com.cubrid.common.ui.swt.table.BaseTableLabelProvider;
import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * MigrationHistory Table Viewer Label Provider
 * 
 * @author Kevin Cao
 * 
 */
public class MigrationHistoryTableLabelProvider extends
		BaseTableLabelProvider {

	/**
	 * @param element Object
	 * @param columnIndex integer
	 * 
	 * @return column text
	 */
	public String getColumnText(Object element, int columnIndex) {
		MigrationBriefReport mbr = (MigrationBriefReport) element;
		if (columnIndex == 0) {
			return mbr.getScriptName();
		}
		if (columnIndex == 1) {
			return CUBRIDTimeUtil.defaultFormatDateTime(new Date(mbr.getStartTime()));
		}
		if (columnIndex == 2) {
			return CUBRIDTimeUtil.defaultFormatDateTime(new Date(mbr.getEndTime()));
		}
		if (columnIndex == 3) {
			return getMigrationStatusLabel(mbr.getStatus());
		}
		return mbr.getHistoryFile();
	}

	/**
	 * @param status 0:accomplished;1:Not accomplished;other:canceled;
	 * @return migration status display label
	 */
	protected String getMigrationStatusLabel(int status) {
		return status == 0 ? Messages.msgAccomplished : (status == 1 ? Messages.msgNotAccomplished
				: Messages.msgCanceled);
	}
}