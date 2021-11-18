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

import java.io.File;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TableViewer;
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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.table.TableLabelProviderAdapter;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.cubridmigration.core.common.CharsetUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * new wizard step 1. choose SQL files to be executed.
 * 
 * @author caoyilin
 * @version 1.0 - 2012-11-27
 */
public class SQLSelectPage extends
		MigrationWizardPage {

	/**
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private final class SQLFilesTableLabelProvider extends
			TableLabelProviderAdapter {

		/**
		 * @param element data
		 * @param columnIndex of table viewer
		 * 
		 * @return text of column
		 */
		public String getColumnText(Object element, int columnIndex) {
			String fileName = element.toString();
			if (columnIndex == 0) {
				return fileName;
			} else if (columnIndex == 1) {
				File file = new File(fileName);
				if (file.exists() && file.isFile()) {
					return file.length() >= 1000 ? (NumberFormat.getIntegerInstance(Locale.US).format(file.length() / 1000))
							: "1";
				} else {
					return Messages.errInvalidSQLFile;
				}
			}
			return null;
		}
	}

	/**
	 * 
	 * Add SQL file(s) in the table view.
	 * 
	 * @author caoyilin
	 * 
	 */
	private class AddAction extends
			Action {
		public AddAction() {
			setText(Messages.btnAdd);
		}

		/**
		 * run
		 */
		public void run() {
			FileDialog dialog = new FileDialog(getShell(), SWT.OPEN | SWT.MULTI);
			dialog.setFilterExtensions(new String[] {"*.sql", "*.txt", "*.*"});
			dialog.setFilterNames(new String[] {"*.sql", "*.txt", "*.*"});
			if (dialog.open() == null) {
				return;
			}
			int count = 0;
			for (String file : dialog.getFileNames()) {
				String fullName = dialog.getFilterPath() + File.separator + file;
				if (new File(fullName).isFile() && (tvInput.indexOf(fullName) < 0)) {
					tvInput.add(fullName);
					count++;
				}
			}
			int[] selection = new int[count];
			for (int i = 0; i < count; i++) {
				selection[i] = tvInput.size() - i - 1;
			}
			refresh(selection);
		}
	}

	/**
	 * 
	 * Move the SQL file(s) to the bottom of the table view.
	 * 
	 * @author caoyilin
	 * 
	 */
	private class BottomAction extends
			Action {
		public BottomAction() {
			setText(Messages.btnBottom);
		}

		/**
		 * run
		 */
		public void run() {
			TableItem[] tis = tableViewer.getTable().getSelection();
			if (tis.length == 0) {
				return;
			}
			List<String> tempList = new ArrayList<String>();
			for (TableItem ti : tis) {
				tempList.add(ti.getText());
			}
			Iterator<String> it = tvInput.iterator();
			while (it.hasNext()) {
				if (tempList.indexOf(it.next()) >= 0) {
					it.remove();
				}
			}
			tvInput.addAll(tempList);
			int[] selection = new int[tempList.size()];
			for (int i = 0; i < tempList.size(); i++) {
				selection[i] = tvInput.size() - i - 1;
			}
			refresh(selection);
		}
	}

	/**
	 * 
	 * Delete SQL file(s) in the table view.
	 * 
	 * @author caoyilin
	 * 
	 */
	private class DeleteAction extends
			Action {
		public DeleteAction() {
			setText(Messages.removeButtonLabel);
		}

		/**
		 * run
		 */
		public void run() {
			TableItem[] tis = tableViewer.getTable().getSelection();
			if (tis.length == 0) {
				return;
			}
			int idx1 = tableViewer.getTable().getSelectionIndex();
			for (TableItem ti : tis) {
				int idx = tvInput.indexOf(ti.getText());
				if (idx >= 0) {
					tvInput.remove(idx);
				}
			}
			if (idx1 >= tvInput.size()) {
				idx1 = tvInput.size() - 1;
			}
			refresh(new int[] {idx1});
		}
	}

	/**
	 * 
	 * Move down the SQL file(s) in the table view.
	 * 
	 * @author caoyilin
	 * 
	 */
	private class DownAction extends
			Action {
		public DownAction() {
			setText(Messages.btnDown);
		}

		/**
		 * run
		 */
		public void run() {
			if (tableViewer.getSelection().isEmpty()) {
				return;
			}
			int idx = tableViewer.getTable().getSelectionIndex();
			int newIdx = idx + 1;
			if (newIdx == tvInput.size()) {
				return;
			}
			String temp = tvInput.get(newIdx);
			tvInput.set(newIdx, tvInput.get(idx));
			tvInput.set(idx, temp);
			refresh(new int[] {newIdx});
		}
	}

	/**
	 * 
	 * Move the SQL file(s) to the top of the table view.
	 * 
	 * @author caoyilin
	 * 
	 */
	private class TopAction extends
			Action {
		public TopAction() {
			setText(Messages.btnTop);
		}

		/**
		 * run
		 */
		public void run() {
			if (tableViewer.getSelection().isEmpty()) {
				return;
			}
			TableItem[] tis = tableViewer.getTable().getSelection();
			List<String> tempList = new ArrayList<String>();
			for (TableItem ti : tis) {
				tempList.add(ti.getText());
			}
			Iterator<String> it = tvInput.iterator();
			while (it.hasNext()) {
				if (tempList.indexOf(it.next()) >= 0) {
					it.remove();
				}
			}
			tvInput.addAll(0, tempList);

			int[] selection = new int[tempList.size()];
			for (int i = 0; i < tempList.size(); i++) {
				selection[i] = i;
			}
			refresh(selection);
		}
	}

	/**
	 * 
	 * Move up the SQL file(s) in the table view.
	 * 
	 * @author caoyilin
	 * 
	 */
	private class UpAction extends
			Action {
		public UpAction() {
			setText(Messages.btnUp);
		}

		/**
		 * run
		 */
		public void run() {
			if (tableViewer.getSelection().isEmpty()) {
				return;
			}
			int idx = tableViewer.getTable().getSelectionIndex();
			int newIdx = idx - 1;
			if (newIdx < 0) {
				return;
			}
			String temp = tvInput.get(newIdx);
			tvInput.set(newIdx, tvInput.get(idx));
			tvInput.set(idx, temp);
			refresh(new int[] {newIdx});
		}
	}

	private static final Logger LOG = LogUtil.getLogger(SQLSelectPage.class);

	private TableViewer tableViewer;
	private Button btnAdd;
	private Button btnDelete;

	private Button btnTop;
	private Button btnUp;
	private Button btnDown;
	private Button btnBottom;

	private final List<String> tvInput = new ArrayList<String>();

	private final AddAction addAction;
	private final DeleteAction deleteAction;
	private final TopAction topAction;
	private final UpAction upAction;
	private final DownAction downAction;
	private final BottomAction bottomAction;

	private Combo cbFileCharset;

	public SQLSelectPage(String pageName) {
		super(pageName);
		addAction = new AddAction();
		deleteAction = new DeleteAction();
		topAction = new TopAction();
		upAction = new UpAction();
		downAction = new DownAction();
		bottomAction = new BottomAction();
	}

	/**
	 * When migration wizard displayed current page.
	 * 
	 * @param event PageChangedEvent
	 */

	protected void afterShowCurrentPage(PageChangedEvent event) {
		try {
			setTitle(getMigrationWizard().getStepNoMsg(SQLSelectPage.this)
					+ Messages.titleWizardPageSelectSQL);
			setDescription(Messages.msgSelectSQL);
			tvInput.clear();
			tvInput.addAll(getMigrationWizard().getMigrationConfig().getSqlFiles());
			tableViewer.setInput(tvInput);
			refresh(new int[] {});
		} catch (Exception ex) {
			LOG.error("", ex);
		}
	}

	/**
	 * 
	 * Create buttons
	 * 
	 * @param group Composite
	 */
	private void createButtons(Composite group) {
		Composite buttonContainer = new Composite(group, SWT.NONE);
		GridData buttonGd = new GridData(SWT.LEFT, SWT.FILL, false, true);
		buttonGd.minimumWidth = 70;
		buttonContainer.setLayoutData(buttonGd);
		buttonContainer.setLayout(new GridLayout());

		btnAdd = new Button(buttonContainer, SWT.NONE);
		btnAdd.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnAdd.setText(Messages.btnAdd);
		btnAdd.setToolTipText(Messages.tipAddSQL);
		btnAdd.setAlignment(SWT.CENTER);
		btnAdd.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				addAction.run();
			}
		});

		btnDelete = new Button(buttonContainer, SWT.NONE);
		btnDelete.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnDelete.setText(Messages.removeButtonLabel);
		btnDelete.setToolTipText(Messages.tipRemoveSQL);
		btnDelete.setAlignment(SWT.CENTER);
		btnDelete.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				deleteAction.run();
			}
		});

		btnTop = new Button(buttonContainer, SWT.NONE);
		btnTop.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnTop.setText(Messages.btnTop);
		btnTop.setToolTipText(Messages.tipTopMove);
		btnTop.setAlignment(SWT.CENTER);
		btnTop.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				topAction.run();
			}
		});

		btnUp = new Button(buttonContainer, SWT.NONE);
		btnUp.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnUp.setText(Messages.btnUp);
		btnUp.setToolTipText(Messages.tipUpMove);
		btnUp.setAlignment(SWT.CENTER);
		btnUp.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				upAction.run();
			}
		});

		btnDown = new Button(buttonContainer, SWT.NONE);
		btnDown.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnDown.setText(Messages.btnDown);
		btnDown.setToolTipText(Messages.tipDownMove);
		btnDown.setAlignment(SWT.CENTER);
		btnDown.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				downAction.run();
			}
		});

		btnBottom = new Button(buttonContainer, SWT.NONE);
		btnBottom.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		btnBottom.setText(Messages.btnBottom);
		btnBottom.setToolTipText(Messages.tipBottomMove);
		btnBottom.setAlignment(SWT.CENTER);
		btnBottom.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				bottomAction.run();
			}
		});

	}

	/**
	 * Create contents of the wizard
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		Composite container = new Composite(parent, SWT.NONE);
		final GridLayout gridLayoutRoot = new GridLayout();
		container.setLayout(gridLayoutRoot);
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		setControl(container);

		Group group = new Group(container, SWT.SHADOW_ETCHED_IN);
		group.setLayout(new GridLayout(2, false));
		GridData groupGridData = new GridData(SWT.FILL, SWT.FILL, true, true);
		group.setLayoutData(groupGridData);

		createSQLTableViewer(group);
		createButtons(group);
		createSettings(container);
		//Initialize
		//afterShowCurrentPage(null);
	}

	/**
	 * Create other settings area
	 * 
	 * @param container Parent
	 */
	private void createSettings(Composite container) {
		Group group = new Group(container, SWT.NONE);
		group.setLayout(new GridLayout(2, false));
		group.setLayoutData(new GridData(SWT.FILL, SWT.BOTTOM, true, false));

		Label charsetLabel = new Label(group, SWT.NONE);
		charsetLabel.setText("File Charset: ");
		charsetLabel.setLayoutData(new GridData(SWT.END, SWT.FILL, false, true));
		cbFileCharset = new Combo(group, SWT.READ_ONLY);
		final GridData gdCharsetCombo = new GridData(SWT.FILL, SWT.CENTER, true, false);
		cbFileCharset.setLayoutData(gdCharsetCombo);
		cbFileCharset.setItems(CharsetUtils.getCharsets());
		cbFileCharset.select(1);
		cbFileCharset.addModifyListener(new ModifyListener() {

			public void modifyText(ModifyEvent e1) {
				//updateDialogStatus(null);
			}
		});

	}

	/**
	 * JDBC source database configuration area
	 * 
	 * @param parent Composite
	 */
	protected void createSQLTableViewer(Composite parent) {
		Composite dbTableContainer = new Composite(parent, SWT.NONE);
		dbTableContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		dbTableContainer.setLayout(new GridLayout());

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {Messages.colSQLFile, Messages.colFileSize});
		tvBuilder.setColumnWidths(new int[] {550, 150});
		tvBuilder.setContentProvider(new StructuredContentProviderAdaptor());
		tvBuilder.setLabelProvider(new SQLFilesTableLabelProvider());
		tableViewer = tvBuilder.buildTableViewer(dbTableContainer, SWT.BORDER | SWT.MULTI
				| SWT.FULL_SELECTION);

		tableViewer.addSelectionChangedListener(new ISelectionChangedListener() {

			public void selectionChanged(SelectionChangedEvent event) {
				setButtonsStatus();
			}
		});

		createTableViewMenus(tableViewer.getTable());
	}

	/**
	 * Create table pop menus
	 * 
	 * @param table Table
	 */
	private void createTableViewMenus(Table table) {
		MenuManager menuManager = new MenuManager();
		menuManager.add(addAction);
		menuManager.add(deleteAction);
		menuManager.add(new Separator());
		menuManager.add(topAction);
		menuManager.add(upAction);
		menuManager.add(downAction);
		menuManager.add(bottomAction);
		Menu menu = menuManager.createContextMenu(table);
		table.setMenu(menu);
	}

	/**
	 * When migration wizard will show next page or previous page.
	 * 
	 * @param event PageChangingEvent
	 */
	protected void handlePageLeaving(PageChangingEvent event) {
		// If page is not complete, it should be go to previous page.
		if (!isPageComplete()) {
			return;
		}
		if (!updateMigrationConfig() && isGotoNextPage(event)) {
			event.doit = false;
		}
	}

	/**
	 * Refresh table viewer.
	 * 
	 * @param selection index of items selected.
	 */
	private void refresh(int[] selection) {
		tableViewer.refresh();
		tableViewer.getTable().setSelection(selection);
		setButtonsStatus();
		if (tvInput.isEmpty()) {
			setErrorMessage(Messages.errNoSQLSelected);
		} else {
			setErrorMessage(null);
		}
		setPageComplete(!tvInput.isEmpty());
	}

	/**
	 * Update buttons and actions status
	 * 
	 */
	private void setButtonsStatus() {
		boolean isEmpty = !tableViewer.getSelection().isEmpty();
		boolean isSingle = tableViewer.getTable().getSelectionCount() == 1;
		boolean hasHead = false;
		boolean hasEnd = false;
		int last = -1;
		boolean broken = false;
		for (int idx : tableViewer.getTable().getSelectionIndices()) {
			if (last == -1) {
				last = idx;
			} else if (idx != last + 1) {
				broken = true;
			} else {
				last = idx;
			}
			if (idx == 0) {
				hasHead = true;
			}
			if (idx == tvInput.size() - 1) {
				hasEnd = true;
			}
		}

		btnDelete.setEnabled(isEmpty);
		btnTop.setEnabled(isEmpty && !(hasHead && !broken));
		btnUp.setEnabled(isEmpty && isSingle && !hasHead);
		btnDown.setEnabled(isEmpty && isSingle && !hasEnd);
		btnBottom.setEnabled(isEmpty && !(hasEnd && !broken));

		deleteAction.setEnabled(btnDelete.getEnabled());
		topAction.setEnabled(btnTop.getEnabled());
		upAction.setEnabled(btnUp.getEnabled());
		downAction.setEnabled(btnDown.getEnabled());
		bottomAction.setEnabled(btnBottom.getEnabled());
	}

	/**
	 * Save user input (source database connection information) to export
	 * options.
	 * 
	 * @return true if update success.
	 */
	protected boolean updateMigrationConfig() {
		if (tvInput.isEmpty()) {
			return false;
		}
		List<String> sqls = new ArrayList<String>();
		for (TableItem ti : tableViewer.getTable().getItems()) {
			File file = new File(ti.getText());
			if (sqls.indexOf(ti.getText()) < 0 && file.exists() && file.isFile()) {
				sqls.add(ti.getText());
			}
		}
		if (sqls.isEmpty()) {
			setErrorMessage(Messages.errNoValidSQLFile);
			return false;
		} else {
			setErrorMessage(null);
		}
		MigrationConfiguration config = getMigrationWizard().getMigrationConfig();
		config.setSqlFiles(sqls);
		config.setSourceFileEncoding(cbFileCharset.getText());
		config.setSourceType(MigrationConfiguration.SQL);
		return true;
	}
}
