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
package com.cubrid.cubridmigration.ui.preference;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerFilter;
import org.eclipse.jface.viewers.ViewerSorter;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.table.TableLabelProviderAdapter;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.connection.JDBCData;
import com.cubrid.cubridmigration.core.connection.JDBCDriverManager;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.database.JDBCConfigDataManager;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * JDBCDriverManagePage is response for managing local JDBC driver files
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-10-29 created by Kevin Cao
 */
public class JDBCDriverManagePage extends
		PreferencePage implements
		IWorkbenchPreferencePage {
	public final static String ID = JDBCDriverManagePage.class.getName();
	private TableViewer tvDrivers;
	private Combo cbTypes;

	private TableViewerComparator<JDBCData> comparator = new TableViewerComparator<JDBCData>();
	private Button btnDelete;

	/**
	 * Init
	 * 
	 * @param workbench IWorkbench
	 */
	public void init(IWorkbench workbench) {
		noDefaultAndApplyButton();
	}

	/**
	 * Create contents
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createContents(Composite parent) {
		//Composite com = new Composite(parent, SWT.NONE);
		parent.setLayout(new GridLayout());
		parent.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Composite com = new Composite(parent, SWT.NONE);
		com.setLayout(new GridLayout(2, false));
		com.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));
		Label lblDbType = new Label(com, SWT.NONE);
		lblDbType.setText(Messages.lblDBType);
		lblDbType.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		cbTypes = new Combo(com, SWT.BORDER | SWT.READ_ONLY);
		cbTypes.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		cbTypes.add(Messages.btnAll);
		cbTypes.select(0);
		for (DatabaseType dt : DatabaseType.getAllTypes()) {
			cbTypes.add(dt.getName());
		}
		cbTypes.addModifyListener(new ModifyListener() {

			public void modifyText(ModifyEvent ev) {
				ViewerFilter viewerFilter = new ViewerFilter() {

					public boolean select(Viewer viewer, Object parentElement, Object element) {
						if (cbTypes.getSelectionIndex() == 0) {
							return true;
						}
						JDBCData jd = (JDBCData) element;
						return jd.getDatabaseType().getName().equalsIgnoreCase(cbTypes.getText());
					}
				};
				tvDrivers.setFilters(new ViewerFilter[] {viewerFilter});
			}
		});
		createTableViewer(parent);
		createButtons(parent);
		return parent;
	}

	/**
	 * Create buttons
	 * 
	 * @param com Composite
	 */
	private void createButtons(Composite com) {
		Composite parent = new Composite(com, SWT.NONE);
		parent.setLayout(new GridLayout(3, false));
		parent.setLayoutData(new GridData(SWT.RIGHT, SWT.BOTTOM, false, false));
		Button btnNew = new Button(parent, SWT.NONE);
		btnNew.setText(Messages.btnAdd);
		btnNew.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnNew.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				addDriver();
			}

		});

		btnDelete = new Button(parent, SWT.NONE);
		btnDelete.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnDelete.setText(Messages.btnDelete);
		btnDelete.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				deleteDriver();
			}

		});

		Button btnRefresh = new Button(parent, SWT.NONE);
		btnRefresh.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnRefresh.setText(Messages.btnDownload);
		btnRefresh.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {

				CompositeUtils.runMethodInProgressBar(true, false, new IRunnableWithProgress() {

					public void run(IProgressMonitor monitor) throws InvocationTargetException,
							InterruptedException {
						JDBCDriverDownloadTask task = new JDBCDriverDownloadTask(
								PathUtils.getJDBCLibDir(), Messages.msgStartDownloadDrivers,
								Messages.msgDriverDownloaded);
						task.execute(monitor);
					}
				});
				JDBCConfigDataManager.loadJdbc();
				refreshDrivers();
			}

		});

	}

	/**
	 * Delete driver from manager.
	 * 
	 */
	private void deleteDriver() {
		if (tvDrivers.getSelection().isEmpty()) {
			return;
		}
		if (!MessageDialog.openConfirm(getShell(), Messages.msgConfirmation,
				Messages.msgConfirmToDeleteJDBCDrivers)) {
			return;
		}
		TableItem ti = tvDrivers.getTable().getSelection()[0];
		JDBCData jd = (JDBCData) ti.getData();
		jd.getDatabaseType().removeJDBCData(jd);
		JDBCConfigDataManager.saveJdbcData();
		refreshDrivers();
	}

	/**
	 * addDriver
	 */
	private void addDriver() {
		final FileDialog fileDialog = new FileDialog(Display.getDefault().getActiveShell(),
				SWT.SINGLE);
		fileDialog.setFilterPath(".");
		fileDialog.setFilterExtensions(new String[] {"*.jar", "*.zip", "*.*"});
		fileDialog.setFilterNames(new String[] {"*.jar", "*.zip", "*.*"});
		final String firstFile = fileDialog.open();
		if (firstFile == null) {
			return;
		}
		if (JDBCDriverManager.getInstance().isDriverDuplicated(firstFile)) {
			UICommonTool.openErrorBox(Display.getDefault().getActiveShell(),
					Messages.msgDuplicatedJdbcDriverFile);
			return;
		}
		if (!JDBCDriverManager.getInstance().addDriver(firstFile, false)) {
			UICommonTool.openErrorBox(Display.getDefault().getActiveShell(),
					Messages.msgInvalidJdbcJar);
			return;
		}
		JDBCConfigDataManager.saveJdbcData();
		refreshDrivers();
	}

	/**
	 * Create table viewer
	 * 
	 * 
	 * @param com Composite
	 */
	private void createTableViewer(Composite com) {
		Composite parent = new Composite(com, SWT.NONE);
		parent.setLayout(new GridLayout());
		parent.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.colType, Messages.colVersion,
				Messages.colFile});
		tvBuilder.setColumnWidths(new int[] {70, 75, 420});
		tvBuilder.setContentProvider(new StructuredContentProviderAdaptor());
		tvBuilder.setLabelProvider(new JDBCDriverTableLabelProvider());
		tvDrivers = tvBuilder.buildTableViewer(parent, SWT.BORDER | SWT.FULL_SELECTION);

		tvDrivers.setSorter(comparator);
		tvDrivers.getTable().getColumn(0).addSelectionListener(new SelectionAdapter() {

			boolean up = false;

			public void widgetSelected(SelectionEvent event) {
				up = !up;
				sortColumn(0, up);
			}

		});
		tvDrivers.getTable().getColumn(1).addSelectionListener(new SelectionAdapter() {

			boolean up = true;

			public void widgetSelected(SelectionEvent event) {
				up = !up;
				sortColumn(1, up);
			}

		});
		tvDrivers.getTable().getColumn(2).addSelectionListener(new SelectionAdapter() {

			boolean up = true;

			public void widgetSelected(SelectionEvent event) {
				up = !up;
				sortColumn(2, up);
			}

		});

		tvDrivers.addSelectionChangedListener(new ISelectionChangedListener() {

			@Override
			public void selectionChanged(SelectionChangedEvent event) {
				if (event.getSelection().isEmpty()) {
					btnDelete.setEnabled(false);
					return;
				}
				Object firstElement = ((StructuredSelection) (event.getSelection())).getFirstElement();
				JDBCData jd = (JDBCData) firstElement;
				File driverFileParentPath = new File(jd.getJdbcDriverPath()).getParentFile();
				File defaultDriverDir = new File(PathUtils.getJDBCLibDir());
				btnDelete.setEnabled(!driverFileParentPath.getAbsolutePath().equals(
						defaultDriverDir.getAbsolutePath()));

			}
		});
		refreshDrivers();
		sortColumn(2, true);
	}

	/**
	 * Sort table items by clicking column
	 * 
	 * @param columnIndex column index of the table
	 * @param up up or down
	 */
	private void sortColumn(int columnIndex, boolean up) {
		comparator.setColumnIndex(columnIndex);
		final int sm = (up ? SWT.UP : SWT.DOWN);
		comparator.setSortMode(sm);
		final Table table = tvDrivers.getTable();
		table.setSortColumn(table.getColumn(columnIndex));
		table.setSortDirection(sm);
		tvDrivers.refresh();
	}

	/**
	 * Refresh
	 * 
	 */
	private void refreshDrivers() {
		final List<JDBCData> jdbcDatas = new ArrayList<JDBCData>();
		for (DatabaseType dt : DatabaseType.getAllTypes()) {
			jdbcDatas.addAll(dt.getJDBCDatas());
		}
		tvDrivers.setInput(jdbcDatas);
	}

	/**
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private final class JDBCDriverTableLabelProvider extends
			TableLabelProviderAdapter {

		/**
		 * @param element in the table viewer
		 * @param columnIndex of the table viewer
		 * @return column text
		 */
		public String getColumnText(Object element, int columnIndex) {
			JDBCData jd = (JDBCData) element;
			switch (columnIndex) {
			case 0:
				return jd.getDatabaseType().getName();
			case 1:
				return jd.getVersion();
			case 2:
				return jd.getJdbcDriverPath();
			default:
				return "";
			}
		}
	}

	/**
	 * TableViewerComparator compares names and start time.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-7-11 created by Kevin Cao
	 * @param <T> class
	 */
	private class TableViewerComparator<T> extends
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
			JDBCData m1 = (JDBCData) o1;
			JDBCData m2 = (JDBCData) o2;
			int mode = sortMode == SWT.UP ? 1 : -1;
			if (columnIndex == 0) {
				return mode
						* m1.getDatabaseType().getName().compareTo(m2.getDatabaseType().getName());
			} else if (columnIndex == 1) {
				String ver1 = m1.getVersion();
				String ver2 = m2.getVersion();
				return (int) (mode * ver1.compareTo(ver2));
			} else if (columnIndex == 2) {
				return (int) mode * m1.getJdbcDriverPath().compareTo(m2.getJdbcDriverPath());
			}
			return 0;
		}

		void setColumnIndex(int ci) {
			this.columnIndex = ci;
		}

		void setSortMode(int sm) {
			this.sortMode = sm;
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
	}
}
