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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.TitleAreaDialog;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.ComboBoxCellEditorFactory;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * A dialog which can make changing the length of the columns which data type
 * are char more conveniently.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-03-28 created by Kevin Cao
 */
public class RenameSchemaDialog extends
		TitleAreaDialog {

	private static final int[] COLUMNS_WIDTHS = new int[] {250, 250};
	private static final int[] COLUMN_STYLES = new int[] {SWT.LEFT, SWT.LEFT};
	private TableViewer tvSchemas;
	private final Map<String, String> renameResult = new HashMap<String, String>();
	private final List<String> oldSchemas = new ArrayList<String>();
	private final List<String> newSchemas = new ArrayList<String>();

	/**
	 * Rename schemas, If old size is only 1 and new size is only, the old will
	 * be set to new without any dialog.
	 * 
	 * @param oldSchemas old schema names to be renamed
	 * @param newSchemas new schema names
	 * @return old to new Mapping
	 */
	public static Map<String, String> renameSchemas(List<String> oldSchemas, List<String> newSchemas) {
		Map<String, String> result = new HashMap<String, String>();
		if (oldSchemas.size() == 1 && newSchemas.size() == 1) {
			result.put(oldSchemas.get(0), newSchemas.get(0));
			return result;
		}
		RenameSchemaDialog dlg = new RenameSchemaDialog(Display.getDefault().getActiveShell(),
				oldSchemas, newSchemas);
		if (dlg.open() != IDialogConstants.OK_ID) {
			return null;
		}
		result.putAll(dlg.renameResult);
		return result;
	}

	public RenameSchemaDialog(Shell parentShell, List<String> oldSchemas, List<String> newSchemas) {
		super(parentShell);
		this.oldSchemas.addAll(oldSchemas);
		this.newSchemas.addAll(newSchemas);
	}

	/**
	 * No help.
	 * 
	 * @return false
	 */
	public boolean isHelpAvailable() {
		return false;
	}

	/**
	 * createDialogArea
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		final Composite composite = new Composite(parent, SWT.NONE);
		composite.setLayout(new GridLayout());
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));

		createTable1(composite);
		createButtons(composite);

		fillTable();
		setTitle(Messages.titleRenameSchema);
		setMessage(Messages.msgRenameSchema);
		return parent;
	}

	/**
	 * Fill the data into table view
	 * 
	 */
	private void fillTable() {
		Object[][] ip = new Object[oldSchemas.size()][2];
		int i = 0;
		//Combobox items index: default is 0 or only one schema
		int defaultIdx = 0;
		if (newSchemas.size() == 1) {
			defaultIdx = 1;
		}
		for (String schema : oldSchemas) {
			ip[i][0] = schema;
			ip[i][1] = defaultIdx;
			i++;
		}
		tvSchemas.setInput(ip);
	}

	/**
	 * Create control buttons
	 * 
	 * @param composite of the buttons
	 */
	private void createButtons(Composite composite) {
		final Composite comButtons = new Composite(composite, SWT.RIGHT);
		comButtons.setLayout(new GridLayout(3, false));
		comButtons.setLayoutData(new GridData(SWT.FILL, SWT.BOTTOM, true, false));

	}

	/**
	 * Create the char table view
	 * 
	 * @param parent of the table view
	 */
	private void createTable1(Composite parent) {
		final Composite composite = new Composite(parent, SWT.NONE);
		composite.setLayout(new GridLayout());
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(getTableHeaders());
		tvBuilder.setColumnStyles(COLUMN_STYLES);
		tvBuilder.setColumnWidths(COLUMNS_WIDTHS);

		List<String> comboInput = new ArrayList<String>();
		comboInput.add("");
		comboInput.addAll(newSchemas);
		ComboBoxCellEditorFactory comboBoxCellEditorFactory = new ComboBoxCellEditorFactory();
		comboBoxCellEditorFactory.setItems(comboInput.toArray(new String[] {}));
		comboBoxCellEditorFactory.setReadOnly(true);
		comboBoxCellEditorFactory.setDefaultValue(0);
		tvBuilder.setCellEditorClasses(new CellEditorFactory[] {null, comboBoxCellEditorFactory});
		tvBuilder.setContentProvider(new ArrayContentProvider());

		tvSchemas = tvBuilder.buildTableViewer(composite, SWT.BORDER | SWT.FULL_SELECTION);

		createContextMenus();
	}

	/**
	 * createContextMenus
	 */
	private void createContextMenus() {
		//Add batch processing menus.
		MenuManager mm = new MenuManager();
		mm.add(new Action() {

			public void run() {
				TableItem[] items = tvSchemas.getTable().getItems();
				for (TableItem ti : items) {
					Object[] objs = (Object[]) ti.getData();
					objs[1] = 0;
				}
				tvSchemas.refresh();
			}

			public String getText() {
				return Messages.btnRemoveSchema;
			}
		});
		mm.add(new Separator());
		MenuManager mm2 = new MenuManager(Messages.btnResetAllSchemaAs);
		int idx = 1;
		for (String tt : newSchemas) {
			final String mmTT = tt;
			final int fidx = idx;
			mm2.add(new Action() {

				public void run() {
					TableItem[] items = tvSchemas.getTable().getItems();
					for (TableItem ti : items) {
						Object[] objs = (Object[]) ti.getData();
						objs[1] = fidx;
					}
					tvSchemas.refresh();
				}

				public String getText() {
					return mmTT;
				}
			});
			idx++;
		}
		mm.add(mm2);
		Menu menu = mm.createContextMenu(tvSchemas.getTable());
		tvSchemas.getTable().setMenu(menu);
	}

	/**
	 * Retrieves the table headers string array.
	 * 
	 * @return string array
	 */
	private String[] getTableHeaders() {
		return new String[] {Messages.colOldSchema, Messages.colNewSchema};
	}

	/**
	 * constrainShellSize
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setSize(540, 405);
		getShell().setText(Messages.msgSchemaInformation);
	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, false);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
	}

	/**
	 * @return shell style
	 */
	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE;
	}

	/**
	 * OK button pressed.
	 */
	protected void okPressed() {
		TableItem[] items = tvSchemas.getTable().getItems();
		boolean isNoMapping = true;
		for (TableItem ti : items) {
			isNoMapping = isNoMapping && StringUtils.isEmpty(ti.getText(1));
			renameResult.put(ti.getText(0), ti.getText(1));
		}
		if (isNoMapping) {
			MessageDialog.openError(getShell(), Messages.msgError, Messages.msgErrNoSchemaMapped);
			return;
		}
		super.okPressed();
	}
}
