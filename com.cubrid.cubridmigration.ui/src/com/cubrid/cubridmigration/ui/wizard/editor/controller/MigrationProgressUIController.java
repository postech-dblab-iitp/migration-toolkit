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
package com.cubrid.cubridmigration.ui.wizard.editor.controller;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.ISaveablePart2;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.part.EditorPart;

import com.cubrid.common.ui.swt.ProgressMonitorDialogRunner;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.IMigrationMonitor;
import com.cubrid.cubridmigration.core.engine.MigrationProcessManager;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.CreateObjectEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportSQLEvent;
import com.cubrid.cubridmigration.core.engine.event.ExportGraphRecordEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportCSVEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportGraphRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportSQLsEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.ui.database.SchemaFetcherWithProgress;
import com.cubrid.cubridmigration.ui.history.MigrationReporter;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * @author Kevin Cao
 * 
 */
public class MigrationProgressUIController {
	protected static final String NA_STRING = "--";
	protected final static Logger LOG = LogUtil.getLogger(MigrationProgressUIController.class);

	protected ProgressMonitorDialogRunner progressMonitorDialogRunner = new ProgressMonitorDialogRunner();

	protected MigrationConfiguration config;
	protected MigrationReporter reporter;
	protected MigrationProcessManager mpm;

	protected String[][] tableItems;

	protected int expCountCache = 0;

	protected int impCountCache = 0;

	protected String reportEditorPartId;

	/**
	 * Add an message to the text area.
	 * 
	 * @param txtProgress Date
	 * @param eventDate message
	 * @param msg String
	 * @param isError whether the message is a error message
	 */
	public void addMessage2Text(StyledText txtProgress, Date eventDate, String msg, boolean isError) {
		if (!txtProgress.getVisible()) {
			return;
		}
		int length = txtProgress.getText().length();
		txtProgress.append(CUBRIDTimeUtil.defaultFormatMilin(eventDate));
		txtProgress.setStyleRange(new StyleRange(length, 23,
				txtProgress.getDisplay().getSystemColor(SWT.COLOR_BLUE), null));
		txtProgress.append(" ");
		String message = msg == null ? " " : msg;
		txtProgress.append(message);
		txtProgress.append("\n");
		Color color = getLogTextColor(isError);
		txtProgress.setStyleRange(new StyleRange(length + 24, message.length(), color, null));
		txtProgress.setSelection(length);
		if (txtProgress.getLineCount() > 8000) {
			txtProgress.getContent().replaceTextRange(0,
					txtProgress.getContent().getOffsetAtLine(1), "");
		}
	}

	/**
	 * @return ISaveablePart2.YES if migration is finished
	 */
	public int canBeClosed() {
		return isMigrationRunning() ? ISaveablePart2.CANCEL : ISaveablePart2.YES;
	}

	/**
	 * @param startMode started by user or scheduler
	 * @return MigrationReporter used in progress monitor
	 */
	public MigrationReporter createMigrationReporter(int startMode) {
		reporter = new MigrationReporter(config, startMode);
		return reporter;
	}

	/**
	 * Get table cell long value
	 * 
	 * @param svalue "-" or a long value
	 * @return long value, 0 if "-"
	 */
	protected long getCellValue(String svalue) {
		if (StringUtils.isBlank(svalue)) {
			return 0;
		}
		return Long.parseLong(NA_STRING.equals(svalue) ? "0" : svalue);
	}

	/**
	 * 
	 * @return Migration Configuration's commit count.
	 */
	public int getCommitCount() {
		return config.getCommitCount();
	}

	/**
	 * @param isError red will be returned.
	 * @return red or green
	 */
	protected Color getLogTextColor(boolean isError) {
		Color color;
		if (isError) {
			color = Display.getDefault().getSystemColor(SWT.COLOR_RED);
		} else {
			color = Display.getDefault().getSystemColor(SWT.COLOR_GREEN);
		}
		return color;
	}

	/**
	 * 
	 * @param event MigrationEvent
	 * @return how much the progress bar should grow up when the event received.
	 */
	public int getProgressBarProgressValue(MigrationEvent event) {
		if (event instanceof CreateObjectEvent) {
			CreateObjectEvent ev = (CreateObjectEvent) event;
			return ev.isSuccess() ? 1 : 0;
		}
		int commitCount = getCommitCount();
		if (event instanceof ExportRecordsEvent) {
			ExportRecordsEvent ere = (ExportRecordsEvent) event;
			if (ere.getRecordCount() <= commitCount) {
				expCountCache = expCountCache + ere.getRecordCount();
			}
			if (expCountCache >= commitCount) {
				int factor = expCountCache / commitCount;
				expCountCache = expCountCache % commitCount;
				return factor;
			}
		} else if (event instanceof ImportRecordsEvent) {
			ImportRecordsEvent ire = (ImportRecordsEvent) event;
			if (ire.isSuccess()) {
				if (ire.getRecordCount() <= commitCount) {
					impCountCache = impCountCache + ire.getRecordCount();
				}
				if (impCountCache >= commitCount) {
					int factor = impCountCache / commitCount;
					impCountCache = impCountCache % commitCount;
					return factor;
				}
			}
		} else if (event instanceof ImportGraphRecordsEvent) {
			ImportGraphRecordsEvent ire = (ImportGraphRecordsEvent) event;
			if (ire.isSuccess()) {
				if (ire.getRecordCount() <= commitCount) {
					impCountCache = impCountCache + ire.getRecordCount();
				}
				if (impCountCache >= commitCount) {
					int factor = impCountCache / commitCount;
					impCountCache = impCountCache % commitCount;
					return factor;
				}
			}
		}
		return 0;
	}

	/**
	 * 
	 * @return the progress bar's style according to the
	 *         config.isImplicitEstimate
	 */
	public int getProgressBarStyle() {
		return config.isImplicitEstimate() ? SWT.INDETERMINATE : SWT.NONE;
	}

	/**
	 * 
	 * @return the progress table viewer's input date
	 */
	public String[][] getProgressTableInput() {
		//For GraphDB
		if (config.getDestType() == MigrationConfiguration.DEST_GRAPH) {
			return getGraphProgressTableInput();
		}
		
		List<SourceTableConfig> expStcs = new ArrayList<SourceTableConfig>();
		expStcs.addAll(config.getExpEntryTableCfg());
		expStcs.addAll(config.getExpSQLCfg());
		int index = 0;
		tableItems = new String[expStcs.size()][6];
		for (SourceTableConfig stc : expStcs) {
			Table tbl = config.getSrcTableSchema(stc.getOwner(), stc.getName());
			if (config.isImplicitEstimate()) {
				tableItems[index] = new String[] {stc.getName(), NA_STRING, NA_STRING, NA_STRING,
						NA_STRING, stc.getOwner()};
			} else if (tbl == null || tbl.getTableRowCount() == 0) {
				tableItems[index] = new String[] {stc.getName(), NA_STRING, NA_STRING, NA_STRING,
						NA_STRING, stc.getOwner()};
			} else {
				tableItems[index] = new String[] {stc.getName(),
						String.valueOf(tbl.getTableRowCount()), "0", "0", "0%", stc.getOwner()};
			}
			index++;
		}
		return tableItems;
	}
	
	private String[][] getGraphProgressTableInput() {
		List<Vertex> expStcs = new ArrayList<Vertex>();
		expStcs.addAll(config.getGraphDictionary().getMigratedVertexList());
		int index = 0;
		tableItems = new String[expStcs.size()][6];
		for (Vertex v : expStcs) {
			Table tbl = config.getSrcTableSchema(v.getOwner(), v.getVertexLabel());
			if (config.isImplicitEstimate()) {
				tableItems[index] = new String[] {v.getVertexLabel(), NA_STRING, NA_STRING, NA_STRING,
						NA_STRING, v.getOwner()};
			} else if (tbl == null || tbl.getTableRowCount() == 0) {
				tableItems[index] = new String[] {v.getVertexLabel(), NA_STRING, NA_STRING, NA_STRING,
						NA_STRING, v.getOwner()};
			} else {
				tableItems[index] = new String[] {v.getVertexLabel(),
						String.valueOf(tbl.getTableRowCount()), "0", "0", "0%", v.getOwner()};
			}
			index++;
		}
		
		return tableItems;
	}

	/**
	 * @return the progress bar's total progress value
	 */
	public int getTotalProgress() {
		int value = config.getExpObjCount();
		List<SourceEntryTableConfig> allExportTables = config.getExpEntryTableCfg();
		int commitCount = config.getCommitCount();
		for (SourceTableConfig stc : allExportTables) {
			final Table st = config.getSrcTableSchema(stc.getOwner(), stc.getName());
			if (st == null) {
				continue;
			}
			long count = st.getTableRowCount();
			int inc = (int) (count / commitCount);
			int inc2 = (int) (count / commitCount);
			if (count % commitCount > 0) {
				inc++;
			}
			if (count % commitCount > 0) {
				inc2++;
			}
			value = value + inc + inc2;
		}
		List<Table> exportSQLTables = config.getSrcSQLSchema2Exp();
		for (Table stc : exportSQLTables) {
			long count = stc.getTableRowCount();
			int inc = (int) (count / commitCount);
			int inc2 = (int) (count / commitCount);
			if (count % commitCount > 0) {
				inc++;
			}
			if (count % commitCount > 0) {
				inc2++;
			}
			value = value + inc + inc2;
		}
		return value;
	}

	/**
	 * 
	 * @param event MigrationEvent
	 * @return true if the event has error
	 */
	public boolean ifEventHasError(MigrationEvent event) {
		if (event instanceof CreateObjectEvent) {
			CreateObjectEvent ev = (CreateObjectEvent) event;
			return !ev.isSuccess();
		} else if (event instanceof ImportRecordsEvent) {
			ImportRecordsEvent ire = (ImportRecordsEvent) event;
			return !ire.isSuccess();
		} else if (event instanceof ImportCSVEvent) {
			ImportCSVEvent ire = (ImportCSVEvent) event;
			return !ire.isSuccess();
		} else if (event instanceof ImportSQLsEvent) {
			ImportSQLsEvent ire = (ImportSQLsEvent) event;
			return !ire.isSuccess();
		} else if (event instanceof ImportGraphRecordsEvent) {
			ImportGraphRecordsEvent ire = (ImportGraphRecordsEvent) event;
			return !ire.isSuccess();
		}
		return event instanceof MigrationErrorEvent;
	}

	/**
	 * When the monitor received migration event, judge if the monitor should
	 * update the export status
	 * 
	 * @param event MigrationEvent
	 * @return true if export status should be updated.
	 */
	public boolean ifShouldUpdateExportStatus(MigrationEvent event) {
		return (event instanceof ExportRecordsEvent) || (event instanceof ExportCSVEvent)
				|| (event instanceof ExportSQLEvent) || (event instanceof ExportGraphRecordEvent);
	}

	/**
	 * When the monitor received migration event, judge if the monitor should
	 * update the import status
	 * 
	 * @param event MigrationEvent
	 * @return true if import status should be updated.
	 */
	public boolean ifShouldUpdateImportStatus(MigrationEvent event) {
		if (event instanceof ImportCSVEvent) {
			return ((ImportCSVEvent) event).isSuccess();
		}
		if (event instanceof ImportSQLsEvent) {
			return ((ImportSQLsEvent) event).isSuccess();
		}
		if (event instanceof ImportRecordsEvent) {
			return ((ImportRecordsEvent) event).isSuccess();
		}
		if (event instanceof ImportGraphRecordsEvent) {
			return ((ImportGraphRecordsEvent) event).isSuccess();
		}
		return false;
	}

	/**
	 * 
	 * @return if the migration is running.
	 */
	public boolean isMigrationRunning() {
		return mpm != null;
	}

	/**
	 * 
	 */
	public void migrationFinished() {
		mpm = null;
		if (!config.targetIsOnline()) {
			return;
		}
		//auto refresh target schema
		try {
			Catalog catalog = SchemaFetcherWithProgress.fetch(config.getTargetConParams());
			if (catalog == null) {
				LOG.error("Refresh target DB schema failed.");
			}
		} catch (Exception ignored) {
			LOG.error("", ignored);
		}
	}

	/**
	 * @param oldEditorPart to be closed.
	 */
	public void openMigrationReport(EditorPart oldEditorPart) {
		try {
			PlatformUI.getWorkbench().getActiveWorkbenchWindow().getActivePage().openEditor(
					reporter, reportEditorPartId);
			oldEditorPart.getSite().getPage().closeEditor(oldEditorPart, false);
		} catch (PartInitException e1) {
			MessageDialog.openError(PlatformUI.getWorkbench().getDisplay().getActiveShell(),
					Messages.msgError, e1.getMessage());
		}
	}

	public void setConfig(MigrationConfiguration config) {
		this.config = config;
	}
	
	public void setProgressMonitorDialogRunner(
			ProgressMonitorDialogRunner progressMonitorDialogRunner) {
		this.progressMonitorDialogRunner = progressMonitorDialogRunner;
	}

	public void setReportEditorPartId(String reportEditorPartId) {
		this.reportEditorPartId = reportEditorPartId;
	}

	/**
	 * Start migration process
	 * 
	 * @param monitor migration monitor
	 * @param startMode start by user or scheduler
	 */
	public void startMigration(IMigrationMonitor monitor, int startMode) {
		expCountCache = 0;
		impCountCache = 0;
		config.cleanNoUsedConfigForStart();
		final MigrationReporter reporter = createMigrationReporter(startMode);
		mpm = MigrationProcessManager.getInstance(config, monitor, reporter);
		mpm.startMigration();
	}

	/**
	 * Stop migration immediately.
	 */
	public void stopMigrationNow() {
		if (mpm == null) {
			return;
		}
		if (!MessageDialog.openConfirm(Display.getDefault().getActiveShell(),
				Messages.msgConfirmation, Messages.msgConfirmStopMigration)) {
			return;
		}
		//Stop migration with progress dialog.
		try {
			progressMonitorDialogRunner.run(true, false, new IRunnableWithProgress() {

				public void run(IProgressMonitor monitor) throws InvocationTargetException,
						InterruptedException {
					monitor.beginTask(Messages.msgStoppingMigration, IProgressMonitor.UNKNOWN);
					mpm.interruptMigration();
					monitor.done();
				}
			});
		} catch (Exception e) {
			LOG.error("", e);
		}
	}

	/**
	 * Update the export count
	 * 
	 * @param tableName to be updated
	 * @param exp count
	 * 
	 * @return row data updated
	 */
	public String[] updateTableExpData(String tableName, long exp) {
		if (exp <= 0) {
			return new String[] {};
		}
		for (String[] item : tableItems) {
			if (item[0].equals(tableName)) {
				return getItemForExpData(exp, item);
			}
		}
		return new String[] {};
	}

	public String[] updateTableExpData(String owner, String tableName, long exp) {
		if (exp <= 0) {
			return new String[] {};
		}
		for (String[] item : tableItems) {
			
			// for Single Schema 
			if (item[5] == null || "null".equalsIgnoreCase(item[5])) {
				return updateTableExpData(tableName, exp);
			}
			
			if (item[0].equals(tableName) && item[5].equalsIgnoreCase(owner)) {
				return getItemForExpData(exp, item);
			}
		}
		return new String[] {};
	}

	private String[] getItemForExpData(long exp, String[] item) {
		long newExp = getCellValue(item[2]) + exp;
		item[2] = String.valueOf(newExp);
		if (!config.isImplicitEstimate()) {
			long oldimp = getCellValue(item[3]);
			item[4] = String.valueOf(Math.round(100 * (newExp + oldimp)
					/ (2 * getCellValue(item[1]))))
					+ "%";
		}
		return item;
	}
	
	/**
	 * Update import count of table
	 * 
	 * @param tableName to be updated
	 * @param imp count
	 * 
	 * @return row data updated
	 */
	public String[] updateTableImpData(String owner, String tableName, long imp) {
		
		for (String[] item : tableItems) {
			// for Single Schema
			if (item[5] == null || "null".equalsIgnoreCase(item[5])) {
				return updateTableImpData(tableName, imp);
			}
			if (item[0].equals(tableName) && item[5].equalsIgnoreCase(owner)) {
				return getItemForImpData(imp, item);
			}
		}
		return new String[] {};
	}
	
	public String[] updateTableImpData(String tableName, long imp) {
		for (String[] item : tableItems) {
			if (item[0].equals(tableName)) {
				return getItemForImpData(imp, item);
			}
		}
		return new String[] {};
	}

	private String[] getItemForImpData(long imp, String[] item) {
		long newImp = getCellValue(item[3]) + imp;
		item[3] = String.valueOf(newImp);
		if (!config.isImplicitEstimate()) {
			long oldexp = getCellValue(item[2]);
			item[4] = String.valueOf(Math.round(100 * (oldexp + newImp)
					/ (2 * getCellValue(item[1]))))
					+ "%";
		}
		return item;
	}

	/**
	 * Update the table's row count in a progress dialog.
	 */
	public void updateTableRowCount() {
		progressMonitorDialogRunner.run(true, false, new IRunnableWithProgress() {

			public void run(IProgressMonitor monitor) throws InvocationTargetException,
					InterruptedException {
				monitor.beginTask(Messages.msgPrepare4Start, IProgressMonitor.UNKNOWN);
				try {
					config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
				} finally {
					monitor.done();
				}
			}
		});
	}
}
