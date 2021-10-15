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

import java.util.List;

import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IMenuListener;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerFilter;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import com.cubrid.common.ui.swt.EditorPartProvider;
import com.cubrid.common.ui.swt.ProgressMonitorDialogRunner;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.editor.MigrationBaseEditorPart;
import com.cubrid.cubridmigration.ui.history.controller.MigrationHistoryUIController;
import com.cubrid.cubridmigration.ui.history.tableviewer.MigrationHistoryBriefComparator;
import com.cubrid.cubridmigration.ui.history.tableviewer.MigrationHistoryTableLabelProvider;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * MigrationHistoryEditorPart responses to show the migration history of local.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-11-11 created by Kevin Cao
 */
public class MigrationHistoryEditorPart extends
		MigrationBaseEditorPart {

	/**
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private final class SortTableItemsByColumnSelectionListener extends
			SelectionAdapter {
		private boolean up;

		SortTableItemsByColumnSelectionListener(boolean upStatus) {
			this.up = upStatus;
		}

		/**
		 * @param event Table viewer column SelectionEvent
		 */
		public void widgetSelected(SelectionEvent event) {
			up = !up;
			sortColumn((TableColumn) event.getSource(), up);
		}
	}

	public final static String ID = MigrationHistoryEditorPart.class.getName();

	private MigrationHistoryUIController delegate = new MigrationHistoryUIController();

	private TableViewer tableViewer;

	private Action actRefresh = new Action() {

		public String getText() {
			return Messages.menuRefresh;
		}

		public void run() {
			refreshHistoryTable();
		}

	};

	private Action actImport = new Action() {

		public String getText() {
			return Messages.menuImportHistory;
		}

		public void run() {
			importHistory();
		}

	};

	private Action actDelete = new Action() {

		public String getText() {
			return Messages.menuDelete;
		}

		public void run() {
			deleteHistory();
		}

	};

	private Action actReStartMigration = new Action() {

		public String getText() {
			return Messages.menuOpenWithWizard;
		}

		public void run() {
			delegate.reopenWizard(getSelectedHistory());
		}

	};

	private Action actOpenReport = new Action() {

		public String getText() {
			return Messages.menuOpenReport;
		}

		public void run() {
			delegate.showMigrationReport(getSelectedHistory());
		}

	};

	private MigrationHistoryBriefComparator comparator = new MigrationHistoryBriefComparator();
	private Text txtFileter;

	/**
	 * @param table to be created context menu.
	 */
	private void createContextMenu(final Table table) {
		final MenuManager mm = new MenuManager();
		mm.addMenuListener(new IMenuListener() {

			public void menuAboutToShow(IMenuManager manager) {
				boolean empty = tableViewer.getSelection().isEmpty();
				actOpenReport.setEnabled(!empty);
				actReStartMigration.setEnabled(!empty);
				actDelete.setEnabled(!empty);
			}
		});
		mm.add(actOpenReport);
		mm.add(actReStartMigration);
		mm.add(new Separator());
		mm.add(actDelete);
		mm.add(new Separator());
		mm.add(actImport);
		mm.add(new Separator());
		mm.add(actRefresh);
		Menu menu = mm.createContextMenu(table);
		table.setMenu(menu);
	}

	/**
	 * Create part controls
	 * 
	 * @param parent of the controls.
	 */
	public void createPartControl(Composite parent) {
		delegate.setProgressMonitorDialogRunner(new ProgressMonitorDialogRunner());
		delegate.setEditorPartProvider(new EditorPartProvider());

		this.setTitleImage(MigrationUIPlugin.getImage("icon/db/log_view.png"));
		parent.setLayout(new GridLayout());
		parent.setLayoutData(new GridData(SWT.FILL, SWT.FILL, false, false));
		createToolbar(parent);
		createTableViewer(parent);
		refreshHistoryTable();
		//Default sorted by start time
		sortColumn(tableViewer.getTable().getColumn(1), true);
	}

	/**
	 * Create table viewer
	 * 
	 * @param parent Composite
	 */
	private void createTableViewer(Composite parent) {
		//Create table viewer of migration history
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.colMigrationName, Messages.colStartTime,
				Messages.colEndTime, Messages.colStatus, Messages.colReportFile});
		tvBuilder.setColumnWidths(new int[] {200, 150, 150, 120, 400});
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new MigrationHistoryTableLabelProvider());

		tableViewer = tvBuilder.buildTableViewer(parent, SWT.BORDER | SWT.FULL_SELECTION
				| SWT.MULTI);

		tableViewer.addDoubleClickListener(new IDoubleClickListener() {

			public void doubleClick(DoubleClickEvent event) {
				delegate.showMigrationReport(getSelectedHistory());
			}
		});

		final Table table = tableViewer.getTable();
		table.getColumn(0).addSelectionListener(new SortTableItemsByColumnSelectionListener(false));
		table.getColumn(1).addSelectionListener(new SortTableItemsByColumnSelectionListener(true));
		tableViewer.setSorter(comparator);

		createContextMenu(table);
	}

	/**
	 * Create tool bar
	 * 
	 * @param parent Composite
	 */
	private void createToolbar(Composite parent) {
		//create tool bar and buttons.
		Composite topCom = new Composite(parent, SWT.NONE);
		topCom.setLayout(new GridLayout(3, false));
		topCom.setLayoutData(new GridData(SWT.FILL, SWT.TOP, false, false));

		ToolBar tbHistory = new ToolBar(topCom, SWT.WRAP | SWT.FLAT | SWT.RIGHT);
		tbHistory.setLayout(new GridLayout());
		tbHistory.setLayoutData(new GridData(SWT.FILL, SWT.TOP, false, false));

		final ToolItem delButton = new ToolItem(tbHistory, SWT.PUSH);
		delButton.setToolTipText(Messages.btnDelete);
		delButton.setImage(MigrationUIPlugin.getImage("icon/delete.gif"));
		delButton.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				deleteHistory();
			}

		});
		final ToolItem openButton = new ToolItem(tbHistory, SWT.PUSH);
		openButton.setToolTipText(Messages.btnOpenHistory);
		openButton.setImage(MigrationUIPlugin.getImage("icon/file_open.png"));
		openButton.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				importHistory();
			}
		});
		new ToolItem(tbHistory, SWT.SEPARATOR);
		final ToolItem refButton = new ToolItem(tbHistory, SWT.PUSH);
		refButton.setToolTipText(Messages.btnRefresh);
		refButton.setImage(MigrationUIPlugin.getImage("icon/refresh.gif"));
		refButton.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				refreshHistoryTable();
			}
		});
		new ToolItem(tbHistory, SWT.SEPARATOR);
		final ToolItem btnOpenWizard = new ToolItem(tbHistory, SWT.PUSH);
		btnOpenWizard.setText(Messages.btnStartMigrationBySelectedHistory);
		btnOpenWizard.setImage(MigrationUIPlugin.getImage("icon/tb/mnu_script_wizard.png"));
		btnOpenWizard.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				delegate.reopenWizard(getSelectedHistory());
			}
		});
		new ToolItem(tbHistory, SWT.SEPARATOR);

		//Text filter
		Label lblFilter = new Label(topCom, SWT.NONE);
		lblFilter.setText(Messages.lblMigrationNameFilter);

		txtFileter = new Text(topCom, SWT.BORDER);
		txtFileter.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));

		txtFileter.addModifyListener(new ModifyListener() {

			final ViewerFilter viewerFilter = new ViewerFilter() {

				public boolean select(Viewer viewer, Object parentElement, Object element) {
					MigrationBriefReport mbr = (MigrationBriefReport) element;
					String text = txtFileter.getText();
					text = text.replaceAll("\\*", ".*") + ".*";
					if (mbr.getScriptName().matches(text)) {
						return true;
					}
					return false;
				}
			};
			final ViewerFilter[] filters = new ViewerFilter[] {viewerFilter};

			public void modifyText(ModifyEvent me) {
				tableViewer.setFilters(filters);
			}
		});

	}

	/**
	 * Sort table items by clicking column
	 * 
	 * @param tc column of the table
	 * @param up up or down
	 */
	private void sortColumn(TableColumn tc, boolean up) {
		final Table table = tableViewer.getTable();
		int columnIndex = table.indexOf(tc);
		comparator.setColumnIndex(columnIndex);
		final int sm = (up ? SWT.UP : SWT.DOWN);
		comparator.setSortMode(sm);
		table.setSortColumn(table.getColumn(columnIndex));
		table.setSortDirection(sm);
		tableViewer.refresh();
	}

	/**
	 * Get the selection
	 * 
	 * @return IStructuredSelection
	 */
	private IStructuredSelection getSelectedHistory() {
		return (IStructuredSelection) tableViewer.getSelection();
	}

	/**
	 * Delete the selected history and refresh.
	 * 
	 */
	public void deleteHistory() {
		delegate.deleteHistory(getSelectedHistory());
		refreshHistoryTable();
	}

	/**
	 * Import and refresh
	 */
	public void importHistory() {
		delegate.importHistory();
		refreshHistoryTable();
	}

	/**
	 * 
	 * Refresh the history table view.
	 * 
	 */
	public void refreshHistoryTable() {
		List<MigrationBriefReport> briefs = delegate.getAllLocalHistory();
		txtFileter.setText("");
		tableViewer.setInput(briefs);
		if (!briefs.isEmpty()) {
			tableViewer.getTable().setSelection(0);
		}
	}
}
