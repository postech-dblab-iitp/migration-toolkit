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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ToolItem;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceFKConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceIndexConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceViewConfig;
import com.cubrid.cubridmigration.mysql.trans.MySQL2CUBRIDMigParas;
import com.cubrid.cubridmigration.ui.SWTResourceConstents;
import com.cubrid.cubridmigration.ui.common.UIConstant;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.preference.MigrationConfigPage;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;

public class GraphConfirmationPage extends
		BaseConfirmationPage {
	private static final Logger LOG = LogUtil.getLogger(GraphConfirmationPage.class);

	/**
	 * Get the migration configuration summary
	 * 
	 * @param migration MigrationConfiguration
	 * @param styleRanges List<StyleRange> displayed in StyleText
	 * @return String summary text
	 */
	public static String getConfigSummary(MigrationConfiguration migration,
			List<StyleRange> styleRanges) {
		String lineSeparator = System.getProperty("line.separator");
		String tabSeparator = "\t";
		StringBuffer text = new StringBuffer();
		//source db
		text.append(Messages.confirmSettingsSourceDatabase).append(lineSeparator).append(
				tabSeparator).append(Messages.confirmSettingsType);
		if (migration.sourceIsOnline()) {
			ConnParameters srcConnParameters = migration.getSourceConParams();
			text.append("Online").append(lineSeparator).append(tabSeparator);
			text.append(Messages.confirmHostIP).append("  ").append(srcConnParameters.getHost()).append(
					lineSeparator).append(tabSeparator).append(Messages.confirmDatabaseName).append(
					"  ").append(srcConnParameters.getDbName()).append(lineSeparator).append(
					tabSeparator).append(Messages.confirmPort).append("  ").append(
					srcConnParameters.getPort()).append(lineSeparator).append(tabSeparator).append(
					Messages.confirmUserName).append("  ").append(srcConnParameters.getConUser()).append(
					lineSeparator).append(tabSeparator).append(Messages.confirmCharset).append("  ").append(
					srcConnParameters.getCharset()).append(lineSeparator).append(tabSeparator).append(
					Messages.confirmTimezone).append("  ");
			//cubrid source doesn't read time zone
			if (srcConnParameters.getTimeZone() == null) {
				text.append(Messages.msgDefault).append(lineSeparator);
			} else {
				int length = srcConnParameters.getTimeZone().length() > 9 ? 9
						: srcConnParameters.getTimeZone().length();
				text.append(srcConnParameters.getTimeZone().substring(0, length)).append(
						lineSeparator);
			}

		} else {
			text.append("MYSQL XML Dump file").append(lineSeparator).append(tabSeparator);
			text.append(Messages.confirmFile).append(migration.getSourceFileName()).append(
					lineSeparator).append(tabSeparator).append(Messages.confirmCharset).append(
					migration.getSourceFileEncoding()).append(lineSeparator).append(tabSeparator).append(
					Messages.confirmTimezone);
			int length = migration.getSourceFileTimeZone().length() > 9 ? 9
					: migration.getSourceFileTimeZone().length();
			text.append(migration.getSourceFileTimeZone().substring(0, length)).append(
					lineSeparator);

		}
		//target db
		text.append(Messages.confirmSettingsTargetDatabase).append(lineSeparator).append(
				tabSeparator).append(Messages.confirmSettingsType);
		if (migration.targetIsOnline()) {
			ConnParameters tcp = migration.getTargetConParams();
			text.append("Online").append(lineSeparator).append(tabSeparator);
			text.append(Messages.confirmHostIP).append("  ").append(tcp.getHost()).append(
					lineSeparator).append(tabSeparator).append(Messages.confirmDatabaseName).append(
					"  ").append(tcp.getDbName()).append(lineSeparator).append(tabSeparator).append(
					Messages.confirmPort).append("  ").append(tcp.getPort()).append(lineSeparator).append(
					tabSeparator).append(Messages.confirmCharset).append("  ").append(
					tcp.getCharset()).append(lineSeparator).append(tabSeparator).append(
					Messages.confirmTimezone).append("  ");
			String timeZone = tcp.getTimeZone();
			timeZone = timeZone == null ? UIConstant.DEFAULT_TIME_ZONE : timeZone;
			int length = timeZone.length() > 9 ? 9 : timeZone.length();
			text.append(timeZone.substring(0, length)).append(lineSeparator);
		} else if (migration.targetIsFile()) {
			text.append(Messages.confirmFileRepository).append(" (");
			if (styleRanges != null) {
				styleRanges.add(new StyleRange(text.length(),
						migration.getTargetDataFileFormatLabel().length(),
						SWTResourceConstents.COLOR_BLUE, null));
			}
			text.append(migration.getTargetDataFileFormatLabel());
			text.append(")").append(lineSeparator).append(tabSeparator);

			text.append(Messages.confirmPath).append("  ");
			if (styleRanges != null) {
				styleRanges.add(new StyleRange(text.length(),
						migration.getFileRepositroyPath().length(),
						SWTResourceConstents.COLOR_BLUE, null));
			}
			text.append(migration.getFileRepositroyPath());
			text.append(lineSeparator).append(tabSeparator);

			text.append(Messages.confrimSchema).append("  ");
			if (styleRanges != null) {
				styleRanges.add(new StyleRange(text.length(),
						migration.getTargetSchemaFileName().length(),
						SWTResourceConstents.COLOR_BLUE, null));
			}
			text.append(migration.getTargetSchemaFileName());
			text.append(lineSeparator).append(tabSeparator);

			text.append(Messages.confrimIndex).append("  ");
			if (styleRanges != null) {
				styleRanges.add(new StyleRange(text.length(),
						migration.getTargetIndexFileName().length(),
						SWTResourceConstents.COLOR_BLUE, null));
			}
			text.append(migration.getTargetIndexFileName());
			text.append(lineSeparator).append(tabSeparator);

			text.append(Messages.confrimData).append("  ");
			int oldLength = text.length();
			if (migration.isOneTableOneFile()) {
				text.append(Messages.btnOneTableOneFile).append(lineSeparator).append(tabSeparator);
			} else {
				if (migration.getDestType() == MigrationConfiguration.DEST_DB_UNLOAD
						|| migration.getDestType() == MigrationConfiguration.DEST_SQL) {
					text.append(migration.getTargetDataFileName());
				} else {
					text.append(migration.getFileRepositroyPath());
					//text.append("data").append(File.separator);
					text.append(migration.getTargetFilePrefix()).append(
							Messages.lblConfirmDataFormat);
					text.append(migration.getDataFileExt());
				}
				text.append(lineSeparator);
				text.append(tabSeparator);
			}
			if (styleRanges != null) {
				styleRanges.add(new StyleRange(oldLength, text.length() - oldLength,
						SWTResourceConstents.COLOR_BLUE, null));
			}

			int length = migration.getTargetFileTimeZone().length() > 9 ? 9
					: migration.getTargetFileTimeZone().length();
			text.append(Messages.confirmTimezone).append(" ").append(
					migration.getTargetFileTimeZone().substring(0, length)).append(lineSeparator);
		}

		return text.toString();
	}

	private boolean isScriptSaved;
	private Button btnSaveSchema;

	private final List<StyleRange> styleRanges = new ArrayList<StyleRange>();

	public GraphConfirmationPage(String pageName) {
		super(pageName);
	}

	/**
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		super.createControl(parent);
		new ToolItem(tbTools, SWT.SEPARATOR);
		btnSaveSchema = new Button(comRoot, SWT.CHECK);
		btnSaveSchema.setText("Save Source Catalog");
		btnSaveSchema.setToolTipText("Save Source Catalog to Script");
		btnSaveSchema.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				MigrationWizard wzd = getMigrationWizard();
				wzd.setSaveSchema(btnSaveSchema.getSelection());
			}
		});
	}

	/**
	 * When migration wizard displayed current page.
	 * 
	 * @param event PageChangedEvent
	 */
	protected void afterShowCurrentPage(PageChangedEvent event) {
		try {
			MigrationWizard wzd = getMigrationWizard();
			setTitle(wzd.getStepNoMsg(this) + Messages.confirmMigrationPageTile);
			setDescription(Messages.confirmMigrationPageDescription);
			isScriptSaved = false;
			MigrationConfiguration cfg = wzd.getMigrationConfig();
			btnSaveSchema.setSelection(cfg.getOfflineSrcCatalog() != null);
			wzd.setSaveSchema(btnSaveSchema.getSelection());
			if (isFirstVisible && !wzd.isLoadMigrationScript()) {
				cfg.setExportThreadCount(MigrationConfigPage.getDefaultExportThreadCount());
				cfg.setImportThreadCount(MigrationConfigPage.getDefaultImpportThreadCountEachTable());
				cfg.setCommitCount(MigrationConfigPage.getCommitCount());
				cfg.setPageFetchCount(MigrationConfigPage.getPageFetchingCount());
				cfg.setMaxCountPerFile(MigrationConfigPage.getFileMaxSize());
				cfg.setImplicitEstimate(false);
			}
			postMigrationData();
		} catch (RuntimeException e) {
			LOG.error(LogUtil.getExceptionString(e));
			throw e;
		} finally {
			isFirstVisible = false;
		}
	}

	/**
	 * Handle page leaving
	 * 
	 * @param event PageChangingEvent
	 */
	protected void handlePageLeaving(PageChangingEvent event) {
		if (!isGotoNextPage(event) && isScriptSaved) {
			this.getMigrationWizard().getMigrationConfig().buildConfigAndTargetSchema(false);
		}
		super.handlePageLeaving(event);
	}

	/**
	 * postMigrationData
	 */
	protected void postMigrationData() {
	    MigrationWizard mw = getMigrationWizard();
		MigrationConfiguration cfg = mw.getMigrationConfig();
		
		styleRanges.clear();
		txtSummary.setText(getConfigSummary(cfg, styleRanges) + cfg.getGraphDictionary().getPrintVertexAndEdgeInfo());
		for (StyleRange sr : styleRanges) {
			txtSummary.setStyleRange(sr);
		}
		
		setDDLText();
		switchText(false);
	}

	/**
	 * Prepare for saving migration script.
	 * 
	 */
	protected void prepare4SaveScript() {
		isScriptSaved = true;
		super.prepare4SaveScript();
	}

	protected boolean isSaveSchema() {
		return btnSaveSchema.getSelection();
	}
}
