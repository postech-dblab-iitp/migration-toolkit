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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.template.MigrationTemplateParser;

/**
 * MigrationBriefReport Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-7-1 created by Kevin Cao
 */
public class MigrationBriefReport {

	private static final Logger LOG = LogUtil.getLogger(MigrationBriefReport.class);

	public static final String EX_BRIEF = ".brief";
	//Migration status: success, has error, canceled
	public static final int MS_SUCCESS = 0;
	public static final int MS_FAILED = 1;
	public static final int MS_CANCELED = 2;

	//Started by user or reservation
	public static final int SM_USER = 0;
	public static final int SM_RESERVATION = 1;

	private String historyFile;
	private String scriptName;
	private long startTime;
	private long endTime;
	private int status;
	private String outputDir;
	private int startMode;
	private int sourceType = MigrationConfiguration.SOURCE_TYPE_CUBRID;

	public long getEndTime() {
		return endTime;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public String getHistoryFile() {
		return historyFile;
	}

	public String getScriptName() {
		return scriptName;
	}

	public int getStartMode() {
		return startMode;
	}

	public long getStartTime() {
		return startTime;
	}

	public int getStatus() {
		return status;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

	public void setHistoryFile(String reportFile) {
		this.historyFile = reportFile;
	}

	public void setScriptName(String scriptName) {
		this.scriptName = scriptName;
	}

	public void setStartMode(int startMode) {
		this.startMode = startMode;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	/**
	 * Load information from history file, the file must be in the directory
	 * "report"
	 * 
	 * @param hisFile name of the history file, not the full name.
	 * @throws FileNotFoundException ex
	 * @throws IOException ex
	 */
	public void loadFromHistoryFile(String hisFile) throws FileNotFoundException, IOException {
		File hf = new File(PathUtils.getReportDir() + hisFile);
		if (!hf.exists() || hf.isDirectory()) {
			throw new IllegalArgumentException("Invalid migration history file.");
		}
		setHistoryFile(hisFile);
		String briefFile = MigrationReportFileUtils.extractBrief(hisFile);
		//If brief file is exists in the ZIP file
		if (briefFile != null) {
			//Extract brief file from report file
			loadFromBriefFile(briefFile);
			return;
		}
		//If is old version report file: Transfer old version to new version
		transferOld2New(hisFile);
	}

	/**
	 * 
	 * Transform old migration history file to new migration history file
	 * 
	 * @param hisFile migration history file
	 * @throws IOException ex
	 */
	public void transferOld2New(String hisFile) throws IOException {
		File hf = new File(hisFile);
		if (!hf.exists() || hf.isDirectory()) {
			throw new IllegalArgumentException("Invalid migration history file.");
		}
		List<File> files = CUBRIDIOUtils.unzip(hisFile, PathUtils.getBaseTempDir());
		List<String> newFiles = new ArrayList<String>();
		String rfName = "";
		String scriptFile = "";
		for (File ff : files) {
			final int endIndex = ff.getName().indexOf(".");
			String name = PathUtils.getBaseTempDir()
					+ ff.getName().substring(0, endIndex > 0 ? endIndex : ff.getName().length());
			if (ff.getName().startsWith("migration_log_")) {
				name = name + IMigrationReporter.LOG_FILE_EX;
			} else if (ff.getName().startsWith("migration_report_")) {
				name = name + IMigrationReporter.REPORT_FILE_EX;
				rfName = name;
			} else if (ff.getName().startsWith("migration_nonsupport_")) {
				name = name + IMigrationReporter.TXT_FILE_EX;
			} else if (ff.getName().startsWith("migration_script_")) {
				name = name + IMigrationReporter.SCRIPT_FILE_EX;
				scriptFile = name;
			} else {
				continue;
			}
			File nf = new File(name);
			if (nf.exists() && !name.equals(ff.getCanonicalPath())) {
				PathUtils.deleteFile(nf);
				CUBRIDIOUtils.mergeFile(ff.getCanonicalPath(), nf.getCanonicalPath());
			}
			newFiles.add(name);
		}
		setScriptName("migration");
		setOutputDir(null);
		if (StringUtils.isNotBlank(scriptFile)) {
			try {
				MigrationConfiguration config = MigrationTemplateParser.parse(scriptFile);
				if (config.targetIsFile()) {
					setOutputDir(config.getFileRepositroyPath());
				}
			} catch (Exception ex) {
				LOG.error("", ex);
			}
		}
		setStartMode(SM_USER);
		MigrationReport report = MigrationReport.loadFromReportFile(rfName);
		setStatus(report.hasError() ? MS_FAILED : MS_SUCCESS);
		setStartTime(report.getTotalStartTime());
		setEndTime(report.getTotalEndTime());

		File bf = new File(PathUtils.getBaseTempDir()
				+ hf.getName().substring(0, hf.getName().indexOf('.') - 1) + EX_BRIEF);
		save2BriefFile(bf.getCanonicalPath());
		newFiles.add(bf.getCanonicalPath());
		PathUtils.deleteFile(hf);
		CUBRIDIOUtils.zip(hf.getCanonicalPath(), newFiles.toArray(new String[newFiles.size()]),
				true);
	}

	/**
	 * Load brief information from .brief file
	 * 
	 * @param briefFile *.brief file
	 * @throws FileNotFoundException ex
	 * @throws IOException ex
	 */
	public void loadFromBriefFile(String briefFile) throws FileNotFoundException, IOException {
		Properties pro = new Properties();
		final FileInputStream inStream = new FileInputStream(briefFile);
		try {
			pro.load(inStream);
			setScriptName(pro.getProperty("name"));
			setOutputDir(pro.getProperty("output"));
			//setReportFile(pro.getProperty("file"));
			setStartMode(Integer.parseInt(pro.getProperty("launcher")));
			setStatus(Integer.parseInt(pro.getProperty("status")));
			setStartTime(Long.parseLong(pro.getProperty("start")));
			setEndTime(Long.parseLong(pro.getProperty("end")));
			setSourceType(Integer.parseInt(pro.getProperty("source")));
		} finally {
			inStream.close();
		}
	}

	/**
	 * Save information in to a .brief file
	 * 
	 * @param file *.brief
	 * @throws IOException ex
	 */
	public void save2BriefFile(String file) throws IOException {
		final File fullBriefFile = new File(file);
		PathUtils.createFile(fullBriefFile);
		Properties pro = new Properties();
		pro.put("name", getScriptName());
		pro.put("start", Long.toString(getStartTime()));
		pro.put("end", Long.toString(getEndTime()));
		//pro.put("file", getHistoryFile());
		pro.put("status", Long.toString(getStatus()));
		pro.put("launcher", Long.toString(getStartMode()));
		pro.put("source", Long.toString(sourceType));
		pro.put("output", getOutputDir() == null ? "" : getOutputDir());
		OutputStream os = new FileOutputStream(fullBriefFile);
		try {
			pro.store(os, null);
		} finally {
			os.close();
		}
	}

	public int getSourceType() {
		return sourceType;
	}

	public void setSourceType(int sourceType) {
		this.sourceType = sourceType;
	}
}
