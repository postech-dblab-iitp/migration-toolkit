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
import java.util.List;

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.ObjectArrayRowCellModifier;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.CheckboxCellEditorFactory;
import com.cubrid.common.ui.swt.table.listener.CheckBoxColumnSelectionListener;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceIndexConfig;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * TableSelectorDialog Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-23 created by Kevin Cao
 */
public class TableIndexSelectorDialog extends
		Dialog {

	/**
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private final class IndexSelectionTableContentProvider extends
			StructuredContentProviderAdaptor {

		/**
		 * @param inputElement input of the viewer
		 * 
		 * @return data list in the viewer
		 */
		@SuppressWarnings("unchecked")
		public Object[] getElements(Object inputElement) {
			List<Object> data = new ArrayList<Object>();
			for (SourceIndexConfig sc : (List<SourceIndexConfig>) inputElement) {
				//Add the SourceConfig to the end of the object array.
				String schema = sc.getParent().getOwner() == null ? "" : sc.getParent().getOwner();
				data.add(new Object[] {schema, sc.getParent().getName(), sc.getName(),
						sc.isCreate(), sc.isReplace(), sc});
			}
			return super.getElements(data);
		}
	}

	/**
	 * 
	 * @author Kevin Cao
	 * 
	 */
	private final class IndexSelectionCellModifier extends
			ObjectArrayRowCellModifier {

		/**
		 * Modify cell element and table item
		 * 
		 * @param ti TableItem
		 * @param element Object[]
		 * @param columnIdx int
		 * @param value Object
		 * 
		 */
		protected void modify(TableItem ti, Object[] element, int columnIdx, Object value) {
			Object[] obj = (Object[]) ti.getData();
			if (value instanceof Boolean) {

				boolean bv = (Boolean) value;
				if (columnIdx == IDX_CREATE) {
					obj[IDX_REPLACE] = bv;
					ti.setImage(IDX_REPLACE, CompositeUtils.getCheckImage(bv));
					updateColumnImage(value, ti, columnIdx + 1);
				} else {
					obj[IDX_CREATE] = (Boolean) obj[IDX_CREATE] || bv;
					ti.setImage(IDX_CREATE, CompositeUtils.getCheckImage((Boolean) obj[IDX_CREATE]));
					updateColumnImage(value, ti, columnIdx - 1);
				}
			}
			super.modify(ti, element, columnIdx, value);
		}
	}

	private TableViewer tvTableIndexes;
	private final MigrationConfiguration cfg;

	private static final int IDX_CREATE = 3;
	private static final int IDX_REPLACE = IDX_CREATE + 1;
	private static final int IDX_OBJ = IDX_REPLACE + 1;

	public TableIndexSelectorDialog(Shell parentShell, MigrationConfiguration cfg) {
		super(parentShell);
		this.cfg = cfg;
	}

	/**
	 * Create dialog area
	 * 
	 * @param parent Composite
	 * @return control
	 */
	protected Control createDialogArea(Composite parent) {
		final Composite composite = new Composite(parent, SWT.NONE);
		composite.setLayout(new GridLayout());
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));

		TableViewerBuilder tvBuilder = new TableViewerBuilder();
		tvBuilder.setColumnNames(new String[] {"Schema", Messages.lblSourceTableName,
				Messages.lblIndexName, Messages.lblCreate, Messages.lblReplace});
		tvBuilder.setTableCursorSupported(true);
		int wdSchema = cfg.getSourceDBType().isSupportMultiSchema() ? 150 : 0;
		tvBuilder.setColumnWidths(new int[] {wdSchema, 150, 150, 80, 90});
		tvBuilder.setCellEditorClasses(new CellEditorFactory[] {null, null, null,
				new CheckboxCellEditorFactory(), new CheckboxCellEditorFactory()});
		tvBuilder.setCellModifier(new IndexSelectionCellModifier());
		tvBuilder.setContentProvider(new IndexSelectionTableContentProvider());
		tvTableIndexes = tvBuilder.buildTableViewer(composite, SWT.BORDER | SWT.FULL_SELECTION);

		CompositeUtils.setTableColumnSelectionListener(tvTableIndexes, new SelectionListener[] {
				null, null, null,
				new CheckBoxColumnSelectionListener(new int[] {IDX_REPLACE}, true, true),
				new CheckBoxColumnSelectionListener(new int[] {IDX_CREATE}, true, false)});

		fillTable();
		return parent;
	}

	/**
	 * Fill data to table viewer
	 */
	private void fillTable() {
		List<SourceIndexConfig> list = new ArrayList<SourceIndexConfig>();
		for (SourceEntryTableConfig setc : cfg.getExpEntryTableCfg()) {
			if (!setc.isCreateNewTable()) {
				continue;
			}
			list.addAll(setc.getIndexConfigList());
		}
		tvTableIndexes.setInput(list);
		CompositeUtils.initTableViewerCheckColumnImage(tvTableIndexes);
	}

	/**
	 * constrainShellSize
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setSize(720, 540);
		getShell().setText(Messages.lblSelectIndexes);
	}

	/**
	 * createButtonsForButtonBar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, false);
		getButton(IDialogConstants.OK_ID).setEnabled(true);
	}

	/**
	 * @return shell style
	 */
	protected int getShellStyle() {
		return super.getShellStyle() | SWT.RESIZE | SWT.MAX;
	}

	/**
	 * Don't support help
	 * 
	 * @return false
	 */
	public boolean isHelpAvailable() {
		return false;
	}

	/**
	 * Button pressed
	 * 
	 * @param buttonId OK or not
	 */
	protected void buttonPressed(int buttonId) {
		if (buttonId == Dialog.OK) {
			for (TableItem ti : tvTableIndexes.getTable().getItems()) {
				Object[] obj = (Object[]) ti.getData();
				SourceIndexConfig sic = (SourceIndexConfig) obj[IDX_OBJ];
				sic.setCreate((Boolean) obj[IDX_CREATE]);
				sic.setReplace((Boolean) obj[IDX_REPLACE]);
			}
		}
		super.buttonPressed(buttonId);
	}

}
