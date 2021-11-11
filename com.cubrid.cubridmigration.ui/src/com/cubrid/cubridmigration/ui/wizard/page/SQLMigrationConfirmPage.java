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
package com.cubrid.cubridmigration.ui.wizard.page;

import java.util.List;

import org.eclipse.jface.dialogs.PageChangedEvent;

import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.common.UIConstant;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.preference.MigrationConfigPage;

/**
 * New wizard step 3. Confirm Migration Settings
 * 
 * @author caoyilin
 * @version 1.0 - 2012-11-26
 */
public class SQLMigrationConfirmPage extends
		BaseConfirmationPage {
	/**
	 * Get the migration configuration summary
	 * 
	 * @param migration MigrationConfiguration
	 * @return String summary text
	 */
	public static String getConfigSummary(MigrationConfiguration migration) {
		String lineSeparator = System.getProperty("line.separator");
		String tabSeparator = "\t";
		StringBuffer text = new StringBuffer();
		//source db
		text.append(Messages.confirmSettingsSourceDatabase).append(lineSeparator).append(
				tabSeparator).append(Messages.confirmSettingsType).append("SQL(s)").append("\r\n");
		text.append(Messages.msgSQLFilesList).append(":\r\n");
		List<String> sqls = migration.getSqlFiles();
		for (String sql : sqls) {
			text.append(tabSeparator).append(sql).append("\r\n");
		}
		//target db
		text.append(Messages.confirmSettingsTargetDatabase).append(lineSeparator).append(
				tabSeparator).append(Messages.confirmSettingsType);
		if (migration.targetIsOnline()) {
			ConnParameters targetConnParameters = migration.getTargetConParams();
			text.append("Online").append(lineSeparator).append(tabSeparator);
			text.append(Messages.confirmHostIP).append(targetConnParameters.getHost()).append(
					lineSeparator).append(tabSeparator).append(Messages.confirmDatabaseName).append(
					targetConnParameters.getDbName()).append(lineSeparator).append(tabSeparator).append(
					Messages.confirmPort).append(targetConnParameters.getPort()).append(
					lineSeparator).append(tabSeparator).append(Messages.confirmCharset).append(
					targetConnParameters.getCharset()).append(lineSeparator).append(tabSeparator).append(
					Messages.confirmTimezone);
			String timeZone = targetConnParameters.getTimeZone();
			timeZone = timeZone == null ? UIConstant.DEFAULT_TIME_ZONE : timeZone;
			int length = timeZone.length() > 9 ? 9 : timeZone.length();
			text.append(timeZone.substring(0, length)).append(lineSeparator);
		}
		return text.toString();
	}

	public SQLMigrationConfirmPage(String pageName) {
		super(pageName);
	}

	/**
	 * When migration wizard displayed current page.
	 * 
	 * @param event PageChangedEvent
	 */
	protected void afterShowCurrentPage(PageChangedEvent event) {
		try {
			setTitle(getMigrationWizard().getStepNoMsg(SQLMigrationConfirmPage.this)
					+ Messages.confirmMigrationPageTile);
			setDescription(Messages.confirmMigrationPageDescription);
			//set thread count before finish
			if (isFirstVisible && !getMigrationWizard().isLoadMigrationScript()) {
				MigrationConfiguration cfg = getMigrationWizard().getMigrationConfig();
				cfg.setExportThreadCount(MigrationConfigPage.getDefaultExportThreadCount());
				cfg.setImportThreadCount(MigrationConfigPage.getDefaultImpportThreadCountEachTable());
				cfg.setCommitCount(MigrationConfigPage.getCommitCount());
				cfg.setImplicitEstimate(false);
			}
			postMigrationData();
		} catch (RuntimeException e) {
			throw e;
		} finally {
			isFirstVisible = false;
		}
	}

	/**
	 * postMigrationData
	 */
	protected void postMigrationData() {
		MigrationConfiguration migration = getMigrationWizard().getMigrationConfig();
		txtSummary.setText(getConfigSummary(migration));
	}
}
