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
package com.cubrid.cubridmigration.core.engine.report;

import java.io.File;
import java.io.IOException;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;

/**
 * MigrationReportFileUtils
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-7-2 created by Kevin Cao
 */
public abstract class MigrationReportFileUtils {

	/**
	 * extract report temporary directory
	 * 
	 * @param mhFile String
	 * @return temporary directory
	 */
	private static String getReportTempDir(String mhFile) {
		File mh = new File(mhFile);
		return PathUtils.getBaseTempDir() + mh.getName().replaceAll("\\.", "_")
				+ File.separator;
	}

	/**
	 * extract *.Brief file from *.mh file
	 * 
	 * @param mhFile *.mh
	 * @return *.Brief
	 * @throws IOException ex
	 */
	public static String extractBrief(String mhFile) throws IOException {
		return CUBRIDIOUtils.extractFromZip(PathUtils.getReportDir() + mhFile,
				MigrationBriefReport.EX_BRIEF, getReportTempDir(mhFile));
	}

	/**
	 * extract *.log file from *.mh file
	 * 
	 * @param mhFile *.mh
	 * @return *.log
	 * @throws IOException ex
	 */
	public static String extractLog(String mhFile) throws IOException {
		return CUBRIDIOUtils.extractFromZip(PathUtils.getReportDir() + mhFile,
				IMigrationReporter.LOG_FILE_EX, getReportTempDir(mhFile));
	}

	/**
	 * extract *.txt file from *.mh file
	 * 
	 * @param mhFile *.mh
	 * @return *.txt
	 * @throws IOException ex
	 */
	public static String extractNonSupport(String mhFile) throws IOException {
		return CUBRIDIOUtils.extractFromZip(PathUtils.getReportDir() + mhFile,
				IMigrationReporter.TXT_FILE_EX, getReportTempDir(mhFile));
	}

	/**
	 * extract *.report file from *.mh file
	 * 
	 * @param mhFile *.mh
	 * @return *.report
	 * @throws IOException ex
	 */
	public static String extractReport(String mhFile) throws IOException {
		return CUBRIDIOUtils.extractFromZip(PathUtils.getReportDir() + mhFile,
				IMigrationReporter.REPORT_FILE_EX, getReportTempDir(mhFile));
	}

	/**
	 * extract *.script file from *.mh file
	 * 
	 * @param mhFile *.mh
	 * @return *.script
	 * @throws IOException ex
	 */
	public static String extractScript(String mhFile) throws IOException {
		return CUBRIDIOUtils.extractFromZip(PathUtils.getReportDir() + mhFile,
				IMigrationReporter.SCRIPT_FILE_EX, getReportTempDir(mhFile));
	}

}
