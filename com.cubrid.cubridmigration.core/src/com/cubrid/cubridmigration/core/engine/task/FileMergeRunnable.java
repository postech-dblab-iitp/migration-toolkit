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
package com.cubrid.cubridmigration.core.engine.task;

import java.io.File;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;

/**
 * Merge template files to output files in thread.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-10-19 created by Kevin Cao
 */
public class FileMergeRunnable implements
		Runnable,
		IMigrationTask {
	private final String sourceFile;
	private final String targetFile;
	private final String targetCharset;
	private final boolean deleteFile;
	private final boolean isTextFile;
	private final RunnableResultHandler listener;

	public FileMergeRunnable(String sourceFile, String targetFile,
			String targetCharset, RunnableResultHandler listener,
			boolean deleteFile, boolean isTextFile) {
		this.sourceFile = sourceFile;
		this.listener = listener;
		this.targetFile = targetFile;
		this.deleteFile = deleteFile;
		this.isTextFile = isTextFile;
		this.targetCharset = targetCharset;
	}

	/**
	 * Get port and start transport file to server.
	 */
	public void run() {
		try {
			if (isTextFile) {
				CUBRIDIOUtils.mergeFile(sourceFile, targetFile);
			} else {
				mergeXLSFile();
			}
			if (listener != null) {
				listener.success();
			}
		} catch (Throwable ex) {
			if (listener != null) {
				listener.failed(ex.getMessage());
			}
		}
		if (deleteFile) {
			PathUtils.deleteFile(new File(sourceFile));
		}
	}

	/**
	 * 
	 * Merge XLS files
	 * 
	 * @throws Exception ex0
	 */
	private void mergeXLSFile() throws Exception {
		Workbook srcWB = null;
		WritableWorkbook tarWW = null;
		Workbook tarWB = null;
		try {
			srcWB = Workbook.getWorkbook(new File(sourceFile));
			Sheet sheet = srcWB.getSheet(0);

			WorkbookSettings tarWS = new WorkbookSettings();
			tarWS.setEncoding(targetCharset);
			final File tarFile = new File(targetFile);
			WritableSheet tarSheet;
			if (tarFile.exists() && tarFile.length() > 0) {
				tarWB = Workbook.getWorkbook(tarFile, tarWS);
				tarWW = Workbook.createWorkbook(tarFile, tarWB);
				tarSheet = tarWW.getSheet(0);
			} else {
				tarWW = Workbook.createWorkbook(tarFile, tarWS);
				tarSheet = tarWW.createSheet(sheet.getName(), 0);
			}
			int total = tarSheet.getRows();
			for (int i = 0; i < sheet.getRows(); i++) {
				Cell[] values = sheet.getRow(i);
				for (int j = 0; j < values.length; j++) {
					tarSheet.addCell(new jxl.write.Label(j, total,
							values[j].getContents()));
				}
				total++;
			}
			tarWW.write();
		} finally {
			if (srcWB != null) {
				srcWB.close();
			}
			if (tarWW != null) {
				tarWW.close();
			}
			if (tarWB != null) {
				tarWB.close();
			}
		}
	}
}
