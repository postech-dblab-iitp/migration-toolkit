/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
 * Solution.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */
package com.cubrid.cubridmigration.ui.wizard.dialog;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.ObjectArrayRowCellModifier;
import com.cubrid.common.ui.swt.table.ObjectArrayRowTableLabelProvider;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.CheckboxCellEditorFactory;
import com.cubrid.common.ui.swt.table.listener.CheckBoxColumnSelectionListener;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * This dialog show about message about CUBRID Manager
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-4-1 created by Kevin Cao
 */
public class SelectSchemaDialog extends
		Dialog {

	private TableViewer tvSchemas;
	private List<String> allSchemas = new ArrayList<String>();
	private List<String> selectedSchemas = new ArrayList<String>();
	private String result = "";
	private List<Object[]> tvInput;

	public SelectSchemaDialog(Shell parentShell, List<String> allSchemas, String schemas) {
		super(parentShell);
		this.allSchemas.addAll(allSchemas);
		if (schemas != null) {
			String[] scs = schemas.split(",");
			for (String sc : scs) {
				if (StringUtils.isBlank(sc)) {
					continue;
				}
				selectedSchemas.add(sc.trim());
			}
		}

	}

	/**
	 * Shell style
	 * 
	 * @return style
	 */
	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE;
	}

	/**
	 * Configures the given shell in preparation for opening this window in it.
	 * 
	 * @param newShell the shell
	 */
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setText("Select Schemas");
		newShell.setSize(320, 400);
		//Adjust dialog position. 
		if (newShell.getParent() != null) {
			Point location = newShell.getParent().getLocation();
			newShell.setLocation(location.x + 10, location.y + 10);
		}
	}

	/**
	 * Create buttons in button bar
	 * 
	 * @param parent the parent composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, true);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
	}

	/**
	 * Creates the page content
	 * 
	 * @param parent the parent composite to contain the dialog area
	 * @return the dialog area control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite comResult = new Composite(parent, SWT.NONE);
		comResult.setLayout(new GridLayout());
		comResult.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {"", "Schemas"});
		tvBuilder.setColumnWidths(new int[] {30, 250});
		tvBuilder.setCellEditorClasses(new CellEditorFactory[] {new CheckboxCellEditorFactory(),
				null});
		tvBuilder.setCellModifier(new ObjectArrayRowCellModifier());
		tvBuilder.setContentProvider(new ArrayContentProvider());
		tvBuilder.setLabelProvider(new ObjectArrayRowTableLabelProvider());

		tvSchemas = tvBuilder.buildTableViewer(comResult, SWT.BORDER | SWT.FULL_SELECTION);

		//		tvSchemas = CompositeUtils.createTableViewer(comResult, SWT.BORDER | SWT.FULL_SELECTION,
		//				new String[] {"", "Schemas"}, new int[] {SWT.NONE, SWT.NONE});
		//CompositeUtils.setTableColumnsWidth(tvSchemas, new int[] {30, 250});
		//		final CellEditor[] cellEditors = new CellEditor[] {
		//				new CheckboxCellEditor(tvSchemas.getTable()), null};
		//		ObjectArrayCellModifier cellModifier = new ObjectArrayCellModifier();
		//		cellModifier.setCellEditors(cellEditors);
		//		CompositeUtils.setTableViewEditors(tvSchemas, cellEditors, cellModifier);
		CompositeUtils.setTableColumnSelectionListener(tvSchemas, new SelectionListener[] {
				new CheckBoxColumnSelectionListener(new int[] {0}, false, false), null});

		tvInput = new ArrayList<Object[]>();
		for (String sc : allSchemas) {
			tvInput.add(new Object[] {selectedSchemas.isEmpty() || selectedSchemas.contains(sc), sc});
		}
		tvSchemas.setInput(tvInput);
		return comResult;
	}

	/**
	 * Schema names
	 * 
	 * @return split by ','
	 */
	public String getSchemaNames() {
		return result;
	}

	/**
	 * OK pressed.
	 */
	protected void okPressed() {
		StringBuffer sb = new StringBuffer();
		int i = 0;
		for (Object[] it : tvInput) {
			if ((Boolean) it[0]) {
				if (i > 0) {
					sb.append(",");
				}
				sb.append(it[1].toString());
				i++;
			}
		}
		if (StringUtils.isBlank(sb.toString())) {
			return;
		}
		result = sb.toString();
		super.okPressed();
	}
}