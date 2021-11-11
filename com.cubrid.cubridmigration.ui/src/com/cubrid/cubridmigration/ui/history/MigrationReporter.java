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
package com.cubrid.cubridmigration.ui.history;

import java.io.File;

import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IPersistableElement;

import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.exception.BreakMigrationException;
import com.cubrid.cubridmigration.core.engine.report.DefaultMigrationReporter;
import com.cubrid.cubridmigration.core.engine.report.MigrationReport;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.page.CSVImportConfirmPage;
import com.cubrid.cubridmigration.ui.wizard.page.ConfirmationPage;
import com.cubrid.cubridmigration.ui.wizard.page.SQLMigrationConfirmPage;

/**
 * MigrationReporter can be an editor input.
 * 
 * @author Kevin Cao
 */
public class MigrationReporter extends
		DefaultMigrationReporter implements
		IEditorInput {

	public MigrationReporter(MigrationConfiguration config, int startMode) {
		super(config, startMode);
	}

	public MigrationReporter(File file) {
		super(file);
	}

	/**
	 * Get summary
	 */
	protected void getSummary() {
		//Summary
		if (config.sourceIsSQL()) {
			report.setConfigSummary(SQLMigrationConfirmPage.getConfigSummary(config));
		} else if (config.sourceIsCSV()) {
			report.setConfigSummary(CSVImportConfirmPage.getConfigSummary(config));
		} else {
			report.setConfigSummary(ConfirmationPage.getConfigSummary(config, null));
		}
	}

	/**
	 * getAdapter
	 * 
	 * @param adapter MigrationConfiguration or MigrationReport
	 * @return Object
	 */
	@SuppressWarnings("rawtypes")
	public Object getAdapter(Class adapter) {
		if (adapter.equals(MigrationConfiguration.class)) {
			return config;
		} else if (adapter.equals(MigrationReport.class)) {
			return report;
		}
		return null;
	}

	/**
	 * default is false
	 * 
	 * @return false
	 */
	public boolean exists() {
		return false;
	}

	public ImageDescriptor getImageDescriptor() {
		return null;
	}

	public String getName() {
		return Messages.editorMigrationReport;
	}

	public IPersistableElement getPersistable() {
		return null;
	}

	public String getToolTipText() {
		return getName();
	}

	/**
	 * @param obj to be compared
	 * 
	 * @return if has same report, the reporter object are same
	 */
	public boolean equals(Object obj) {
		if (obj instanceof MigrationReporter) {
			MigrationReporter refer = (MigrationReporter) obj;
			return fileName.equalsIgnoreCase(refer.fileName);
		}
		return false;
	}

	/**
	 * @return fileName.hashCode()
	 */
	public int hashCode() {
		return fileName.hashCode();
	}

	/**
	 * Is the report is a SQL importing report.
	 * 
	 * @return true if it is a SQL importing report
	 */
	public boolean isSQLReport() {
		if (report == null) {
			throw new BreakMigrationException("Migration Report was not specified.");
		}
		return !report.getDataFileResults().isEmpty();
	}
}
