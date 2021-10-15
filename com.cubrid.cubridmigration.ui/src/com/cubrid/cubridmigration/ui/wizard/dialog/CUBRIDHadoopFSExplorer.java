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
package com.cubrid.cubridmigration.ui.wizard.dialog;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerSorter;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.table.BaseTableLabelProvider;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.hadoop.HadoopStreamFactory;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * CUBRIDHadoopFSExplorer
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-8-30 created by Kevin Cao
 */
public class CUBRIDHadoopFSExplorer extends
		Dialog {

	/**
	 * Hadoop file list table viewer label provider
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private static class FilesTableViewerLabelProvider extends
			BaseTableLabelProvider {

		/**
		 * @param element Table data
		 * @param columnIndex column index
		 * 
		 * @return image
		 */
		public Image getColumnImage(Object element, int columnIndex) {
			FileStatus fs = (FileStatus) element;
			if (columnIndex > 0) {
				return null;
			}
			return fs.isDir() ? MigrationUIPlugin.getImage("icon/folder.png")
					: MigrationUIPlugin.getImage("icon/file.png");
		}

		/**
		 * @param element Table data
		 * @param columnIndex column index
		 * 
		 * @return column text
		 */
		public String getColumnText(Object element, int columnIndex) {
			FileStatus fs = (FileStatus) element;
			switch (columnIndex) {
			case 0:
				return fs.getPath().getName();
			case 1:
				return CUBRIDTimeUtil.defaultFormatDateTime(new Date(fs.getModificationTime()));
			case 2:
				return fs.isDir() ? Messages.msgDirectory : (PathUtils.extracFileExt(
						fs.getPath().getName()).toLowerCase(Locale.ENGLISH)
						+ " " + Messages.msgFile);
			case 3:
				return fs.isDir() ? "" : PathUtils.getFileKBSize(fs.getLen());
			default:
				return "";
			}
		}
	}

	/**
	 * ColumnSelectionSortListener
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-9-4 created by Kevin Cao
	 */
	private class ColumnSelectionSortListener extends
			SelectionAdapter {
		private boolean up = true;

		/**
		 * Sort table items by clicking column
		 * 
		 * @param col column index of the table
		 */
		public void sortColumn(TableColumn col) {
			comparator.setColIndex(tvFiles.getTable().indexOf(col));
			final int sm = (up ? SWT.UP : SWT.DOWN);
			comparator.setSortMode(sm);
			final Table table = tvFiles.getTable();
			table.setSortColumn(col);
			table.setSortDirection(sm);
			tvFiles.refresh();
		}

		/**
		 * @param event SelectionEvent
		 * 
		 */
		public void widgetSelected(SelectionEvent event) {
			up = !up;
			sortColumn((TableColumn) event.getSource());
		}
	}

	/**
	 * BriefComparator compares names and start time.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-7-11 created by Kevin Cao
	 * @param <T> class
	 */
	private static class FileStatusComparator<T> extends
			ViewerSorter implements
			Comparator<Object> {

		private int colIndex;
		private int sortMode;

		/**
		 * compare
		 * 
		 * @param o1 Object
		 * @param o2 Object
		 * @return compare result
		 */
		public int compare(Object o1, Object o2) {
			FileStatus m1 = (FileStatus) o1;
			FileStatus m2 = (FileStatus) o2;
			int mode = sortMode == SWT.UP ? 1 : -1;
			final int typeFactor = m1.isDir() == m2.isDir() ? 1 : -1;
			if (typeFactor < 0) {
				return typeFactor * mode;
			}
			long baseFactor;
			if (colIndex == 0) {
				baseFactor = m1.getPath().getName().compareTo(m2.getPath().getName());
			} else if (colIndex == 1) {
				baseFactor = m1.getModificationTime() - m2.getModificationTime();
			} else if (colIndex == 2) {
				String ext1 = PathUtils.extracFileExt(m1.getPath().getName()).toLowerCase(
						Locale.ENGLISH);
				String ext2 = PathUtils.extracFileExt(m2.getPath().getName()).toLowerCase(
						Locale.ENGLISH);
				baseFactor = ext1.compareTo(ext2);
				if (baseFactor == 0) {
					baseFactor = m1.getPath().getName().compareTo(m2.getPath().getName());
				}
			} else if (colIndex == 3) {
				baseFactor = m1.getLen() - m2.getLen();
			} else {
				baseFactor = 0;
			}
			return baseFactor == 0 ? 0 : (mode * ((int) (baseFactor / Math.abs(baseFactor))));
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

		/**
		 * Change column index
		 * 
		 * @param ci column index
		 */
		void setColIndex(int ci) {
			this.colIndex = ci;
		}

		/**
		 * Change sort mode
		 * 
		 * @param sm SWT.UP or SWT.DOWN
		 */
		void setSortMode(int sm) {
			this.sortMode = sm;
		}
	}

	private static final String KEY_SORT = "sort";

	private static String lastHost = "";

	private final FileStatusComparator<FileStatus> comparator = new FileStatusComparator<FileStatus>();

	private FileStatus currentRootPath;
	private final ModifyListener fileTxtModifyListener = new ModifyListener() {

		public void modifyText(ModifyEvent ev) {
			tvFiles.setSelection(null);
		}
	};
	private final List<String> hdfsFiles = new ArrayList<String>();
	private final FocusAdapter restoreRootPathListener = new FocusAdapter() {

		public void focusGained(FocusEvent ev) {
			if (currentRootPath == null || txtRootPath == null) {
				return;
			}
			txtRootPath.setText(currentRootPath.getPath().toString());
		}

	};
	private TableViewer tvFiles;
	private Text txtPath;
	private Text txtRootPath;

	private Button btnUp;

	private Button btnGO;

	public CUBRIDHadoopFSExplorer(Shell parentShell) {
		super(parentShell);
	}

	/**
	 * Create button
	 * 
	 * @param parent Composite
	 * @param id int
	 * @param label String
	 * @param defaultButton boolean
	 * @return Button
	 */
	protected Button createButton(Composite parent, int id, String label, boolean defaultButton) {
		final Button btn = super.createButton(parent, id, label, false);
		btn.addFocusListener(restoreRootPathListener);
		return btn;
	}

	/**
	 * @param parent of the composites
	 * @return dialog area.
	 */
	protected Control createDialogArea(Composite parent) {
		this.getShell().setText(Messages.titleAddCSVFromHDFS);
		Composite rootCom = new Composite(parent, SWT.NONE);
		rootCom.setLayout(new GridLayout());
		rootCom.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		createPathArea(rootCom);
		createTableViewer(rootCom);
		createFileArea(rootCom);

		if (StringUtils.isNotEmpty(lastHost)) {
			gotoPath();
		}
		return rootCom;
	}

	/**
	 * createFileArea
	 * 
	 * @param rootCom Composite
	 */
	private void createFileArea(Composite rootCom) {
		Composite com = new Composite(rootCom, SWT.NONE);
		com.setLayout(new GridLayout(2, false));
		com.setLayoutData(new GridData(SWT.FILL, SWT.BOTTOM, true, false));
		Label lblPath = new Label(com, SWT.NONE);
		lblPath.setText(Messages.lblHDFSFile);

		txtPath = new Text(com, SWT.BORDER);
		txtPath.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtPath.setText("");
		txtPath.addModifyListener(fileTxtModifyListener);
		txtPath.addFocusListener(restoreRootPathListener);
		txtPath.addKeyListener(new KeyAdapter() {

			public void keyPressed(KeyEvent ev) {
				if (ev.character != '\r') {
					return;
				}
				okPressed();
			}

		});
	}

	/**
	 * createPathArea
	 * 
	 * @param rootCom Composite
	 */
	private void createPathArea(Composite rootCom) {
		Composite com1 = new Composite(rootCom, SWT.NONE);
		com1.setLayout(new GridLayout(4, false));
		com1.setLayoutData(new GridData(SWT.FILL, SWT.BOTTOM, true, false));

		Label lblRootPath = new Label(com1, SWT.NONE);
		lblRootPath.setText(Messages.lblHDFSPath);

		txtRootPath = new Text(com1, SWT.BORDER);
		txtRootPath.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtRootPath.setText(lastHost);
		txtRootPath.addKeyListener(new KeyAdapter() {

			public void keyPressed(KeyEvent ev) {
				if (ev.character != '\r') {
					return;
				}
				gotoPath();
			}
		});

		txtRootPath.addModifyListener(new ModifyListener() {

			public void modifyText(ModifyEvent ev) {
				btnGO.setEnabled(StringUtils.isNotBlank(txtRootPath.getText()));
			}
		});

		btnGO = new Button(com1, SWT.NONE);
		btnGO.setImage(MigrationUIPlugin.getImage("icon/file_open.png"));
		btnGO.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				gotoPath();
			}
		});
		btnUp = new Button(com1, SWT.NONE);
		btnUp.setImage(MigrationUIPlugin.getImage("icon/up.png"));
		btnUp.addFocusListener(restoreRootPathListener);
		btnUp.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				goUp();
			}
		});

		btnGO.setEnabled(StringUtils.isNotBlank(txtRootPath.getText()));
		btnUp.setEnabled(false);
	}

	/**
	 * Create table viewer
	 * 
	 * @param rootCom Composite
	 */
	private void createTableViewer(Composite rootCom) {
		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.colFileName, Messages.colFileDate,
				Messages.colFileType, Messages.colFileSize});
		tvBuilder.setColumnWidths(new int[] {220, 140, 100, 120});
		tvBuilder.setContentProvider(new StructuredContentProviderAdaptor());
		tvBuilder.setLabelProvider(new FilesTableViewerLabelProvider());

		tvFiles = tvBuilder.buildTableViewer(rootCom, SWT.BORDER | SWT.MULTI | SWT.FULL_SELECTION);

		tvFiles.getTable().setLinesVisible(false);

		tvFiles.getTable().addFocusListener(restoreRootPathListener);

		tvFiles.addSelectionChangedListener(new ISelectionChangedListener() {

			public void selectionChanged(SelectionChangedEvent event) {
				if (event.getSelection().isEmpty()) {
					return;
				}
				TableItem[] tis = tvFiles.getTable().getSelection();
				if (tis.length == 1 && ((FileStatus) tis[0].getData()).isDir()) {
					return;
				}
				StringBuffer names = new StringBuffer();
				for (TableItem ti : tis) {
					FileStatus fs = (FileStatus) ti.getData();
					if (fs.isDir()) {
						continue;
					}
					names.append("").append(fs.getPath().getName()).append(" ");
				}
				txtPath.removeModifyListener(fileTxtModifyListener);
				txtPath.setText(names.toString().trim());
				txtPath.addModifyListener(fileTxtModifyListener);
			}
		});
		tvFiles.addDoubleClickListener(new IDoubleClickListener() {

			public void doubleClick(DoubleClickEvent event) {
				TableItem[] tis = tvFiles.getTable().getSelection();
				if (tis.length == 0) {
					return;
				}

				if (tis.length == 1 && ((FileStatus) tis[0].getData()).isDir()) {
					gotoPath(((FileStatus) tis[0].getData()));
					return;
				}
				for (TableItem ti : tis) {
					FileStatus fs = (FileStatus) ti.getData();
					if (fs.isDir()) {
						continue;
					}
					hdfsFiles.add(fs.getPath().toString());
				}
				if (hdfsFiles.isEmpty()) {
					gotoPath(((FileStatus) tis[0].getData()));
					return;
				}
				superOK();
			}
		});
		tvFiles.getTable().addKeyListener(new KeyAdapter() {

			public void keyPressed(KeyEvent ev) {
				if (ev.keyCode != 8) {
					return;
				}
				goUp();
			}

		});

		tvFiles.setSorter(comparator);
		for (int i = 0; i < tvFiles.getTable().getColumnCount(); i++) {
			final ColumnSelectionSortListener listener = new ColumnSelectionSortListener();
			final TableColumn column = tvFiles.getTable().getColumn(i);
			column.addSelectionListener(listener);
			column.setData(KEY_SORT, listener);
		}
	}

	/**
	 * getFileStatus
	 * 
	 * @param hdfs String
	 * @return FileStatus
	 * @throws Exception ex
	 */
	private FileStatus getFileStatusWithProgress(final String hdfs) throws Exception {
		final Object[] fss = new Object[2];
		CompositeUtils.runMethodInProgressBar(true, false, new IRunnableWithProgress() {

			public void run(IProgressMonitor monitor) throws InvocationTargetException,
					InterruptedException {
				try {
					monitor.beginTask(Messages.msgConnectingHDFS, IProgressMonitor.UNKNOWN);
					fss[0] = HadoopStreamFactory.getFileStatus(hdfs);
				} catch (Exception ex) {
					fss[1] = ex;
				}
				monitor.done();
			}
		});
		if (fss[1] != null) {
			throw (Exception) fss[1];
		}
		return (FileStatus) fss[0];
	}

	/**
	 * 
	 * @param hdfs string
	 * @return hdfs://.....
	 */
	private String getFullPath(String hdfs) {
		if (StringUtils.isBlank(hdfs)) {
			return "";
		}
		if (!hdfs.toLowerCase(Locale.ENGLISH).startsWith("hdfs://")) {
			hdfs = "hdfs://" + hdfs;
		}
		//Auto add '/' to the address
		if (hdfs.lastIndexOf('/') == 6) {
			hdfs = hdfs + "/";
		}
		return hdfs;
	}

	/**
	 * Hdfs Path
	 * 
	 * @return HDFS path
	 */
	public List<String> getHdfsFiles() {
		return new ArrayList<String>(hdfsFiles);
	}

	/**
	 * @return dialog size
	 */
	protected Point getInitialSize() {
		return new Point(640, 480);
	}

	/**
	 * Refresh root path text
	 * 
	 */
	private void gotoPath() {
		final String hdfs = getFullPath(txtRootPath.getText().trim());
		if (StringUtils.isEmpty(hdfs)) {
			return;
		}
		try {
			FileStatus fs = getFileStatusWithProgress(hdfs);
			gotoPath(fs);
		} catch (Exception e) {
			MessageDialog.openError(getShell(), Messages.msgError, e.getMessage());
		}
	}

	/**
	 * Go to a HDFS path and list children
	 * 
	 * @param rootFS FileStatus
	 */
	private void gotoPath(FileStatus rootFS) {
		try {
			if (rootFS == null) {
				return;
			}
			if (!rootFS.isDir()) {
				rootFS = HadoopStreamFactory.getFileStatus(rootFS.getPath().getParent().toString());
			}
			if (rootFS == null) {
				return;
			}
			currentRootPath = rootFS;
			txtRootPath.setText(rootFS.getPath().toString());
			FileStatus[] fss = HadoopStreamFactory.getChidrenFileStatus(rootFS.getPath().toString());
			tvFiles.setInput(fss);
			tvFiles.getTable().setFocus();
			TableColumn sortColumn = tvFiles.getTable().getSortColumn();
			if (sortColumn == null) {
				sortColumn = tvFiles.getTable().getColumn(0);
			}
			((ColumnSelectionSortListener) sortColumn.getData(KEY_SORT)).sortColumn(sortColumn);
			lastHost = currentRootPath.getPath().toString();
			btnUp.setEnabled(currentRootPath.getPath().getParent() != null);
		} catch (Exception e) {
			MessageDialog.openError(getShell(), Messages.msgError, e.getMessage());
		}
	}

	/**
	 * Go to parent path
	 * 
	 */
	private void goUp() {
		try {
			if (currentRootPath == null) {
				return; //Invalid path
			}
			final Path parent2 = currentRootPath.getPath().getParent();
			if (parent2 == null) {
				return;
			}
			txtRootPath.setText(parent2.toString());
			gotoPath();
		} catch (Exception e) {
			MessageDialog.openError(getShell(), Messages.msgError, e.getMessage());
		}
	}

	/**
	 * Resizable
	 * 
	 * @return true
	 */
	protected boolean isResizable() {
		return true;
	}

	/**
	 * OK button pressed
	 */
	protected void okPressed() {
		try {
			if (tvFiles.getSelection().isEmpty()) {
				final String filesStr = txtPath.getText().trim();
				String[] fileNames = filesStr.split("\\s");
				for (String fn : fileNames) {
					final String fullFN;
					if (fn.indexOf('/') >= 0) {
						fullFN = fn;
					} else {
						fullFN = (currentRootPath == null ? ""
								: currentRootPath.getPath().toString()) + "/" + fn;
					}
					String hdfs = getFullPath(fullFN);
					if (StringUtils.isEmpty(hdfs)) {
						continue;
					}
					FileStatus rootFS = getFileStatusWithProgress(hdfs);
					if (rootFS.isDir()) {
						if (fileNames.length == 1) {
							gotoPath(rootFS);
							return;
						}
						continue;
					}
					hdfsFiles.add(rootFS.getPath().toString());
				}
				if (hdfsFiles.isEmpty()) {
					return;
				}
			} else {
				TableItem[] tis = tvFiles.getTable().getSelection();
				for (TableItem ti : tis) {
					FileStatus fs = (FileStatus) ti.getData();
					if (fs.isDir()) {
						continue;
					}
					hdfsFiles.add(fs.getPath().toString());
				}
				if (hdfsFiles.isEmpty()) {
					tvFiles.setSelection(null);
					return;
				}
			}
			superOK();
		} catch (Exception e) {
			MessageDialog.openError(getShell(), Messages.msgError, e.getMessage());
		}
	}

	/**
	 * super.okPressed.
	 * 
	 */
	private void superOK() {
		super.okPressed();
	}

}
