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
package com.cubrid.cubridmigration.ui.wizard.page.view;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.ComboBoxCellEditorFactory;
import com.cubrid.common.ui.swt.table.celleditor.TextCellEditorFactory;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.SourceIndexConfig;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.navigator.node.IndexNode;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * IndexMappingView responses to display index mapping
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-26 created by Kevin Cao
 */
public class IndexMappingView extends
		AbstractMappingView {
	private static final String[] ITEMS = new String[] {"ASC", "DESC"};

	private Composite container;
	private IndexView grpSource;
	private IndexView grpTarget;
	private SourceIndexConfig sic;

	private Button btnCreate;
	private Button btnReplace;

	public IndexMappingView(Composite parent) {
		super(parent);
	}

	/**
	 * Hide UI
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(container, true);
	}

	/**
	 * Show UI
	 */
	public void show() {
		CompositeUtils.hideOrShowComposite(container, false);
	}

	/**
	 * Create controls
	 * 
	 * @param parent Composite
	 */
	protected void createControl(Composite parent) {
		container = new Composite(parent, SWT.NONE);
		GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
		gd.exclude = true;
		container.setLayoutData(gd);
		container.setVisible(false);
		container.setLayout(new GridLayout(2, false));

		btnCreate = new Button(container, SWT.CHECK);
		btnCreate.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		btnCreate.setText(Messages.lblCreate);
		btnCreate.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				btnReplace.setEnabled(btnCreate.getSelection());
				btnReplace.setSelection(btnCreate.getSelection());
				grpTarget.setEditable(btnCreate.getSelection());
			}
		});
		btnReplace = new Button(container, SWT.CHECK);
		btnReplace.setText(Messages.lblReplace);
		btnReplace.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

		createSourcePart(container);
		createTargetPart(container);

	}

	/**
	 * Create source part
	 * 
	 * @param parent Composite
	 */
	private void createSourcePart(Composite parent) {
		grpSource = new IndexView(parent, Messages.lblSource);
		grpSource.setEditable(false);
	}

	/**
	 * Create target part
	 * 
	 * @param parent Composite
	 */
	private void createTargetPart(Composite parent) {
		grpTarget = new IndexView(parent, Messages.lblTarget);
	}

	/**
	 * Display index node
	 * 
	 * @param obj index node
	 */
	public void showData(Object obj) {
		super.showData(obj);
		if (!(obj instanceof IndexNode)) {
			return;
		}
		btnCreate.setEnabled(false);
		btnReplace.setEnabled(false);
		grpTarget.setEditable(false);

		Index srcIndex = ((IndexNode) obj).getIndex();
		grpSource.setIndex(srcIndex);

		sic = config.getExpIdxCfg(srcIndex.getTable().getOwner(), srcIndex.getTable().getName(),
				srcIndex.getName());
		if (sic == null) {
			return;
		}
		Table tt = config.getTargetTableSchema(sic.getParent().getTarget());
		if (tt == null) {
			return;
		}
		Index tidx = tt.getIndexByName(sic.getTarget());
		if (tidx == null) {
			return;
		}
		grpTarget.setIndex(tidx);
		btnCreate.setEnabled(sic.getParent().isCreateNewTable());
		btnCreate.setSelection(sic.getParent().isCreateNewTable() && sic.isCreate());
		btnReplace.setEnabled(btnCreate.getSelection());
		btnReplace.setSelection(btnCreate.getSelection() && sic.isReplace());
		grpTarget.setEditable(btnCreate.getSelection());
	}

	/**
	 * Save. If create option is not checked, the other information will not be
	 * saved.
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		if (sic == null) {
			return super.save();
		}
		sic.setCreate(btnCreate.getSelection());
		sic.setReplace(btnReplace.getSelection());
		if (!sic.isCreate()) {
			return super.save();
		}
		VerifyResultMessages result = grpTarget.save();
		if (!result.hasError()) {
			sic.setTarget(grpTarget.idx.getName());
		}
		return result;
	}

	/**
	 * IndexView responses to display a index object
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-8-22 created by Kevin Cao
	 */
	private class IndexView {

		private Text txtTableName;
		private Text txtIndexName;
		private Button btnReverse;
		private Button btnUnique;
		private TableViewer tvColumns;

		private Index idx;
		private Button btnAddIdx;
		private Button btnRemoveIdx;

		private final List<Object[]> columnsData = new ArrayList<Object[]>();

		IndexView(Composite parent, String name) {
			Group grp = new Group(parent, SWT.NONE);
			grp.setLayout(new GridLayout(2, false));
			GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
			gd.widthHint = PART_WIDTH;
			grp.setLayoutData(gd);
			grp.setText(name);

			Label lblTableName = new Label(grp, SWT.NONE);
			lblTableName.setText(Messages.lblTableName);

			txtTableName = new Text(grp, SWT.BORDER);
			txtTableName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtTableName.setTextLimit(CUBRIDDataTypeHelper.DB_OBJ_NAME_MAX_LENGTH);
			txtTableName.setText("");
			txtTableName.setEditable(false);

			Label lblIndexName = new Label(grp, SWT.NONE);
			lblIndexName.setText(Messages.lblIndexName);

			txtIndexName = new Text(grp, SWT.BORDER);
			txtIndexName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtIndexName.setTextLimit(CUBRIDDataTypeHelper.DB_OBJ_NAME_MAX_LENGTH);
			txtIndexName.setText("");

			btnReverse = new Button(grp, SWT.CHECK);
			btnReverse.setText("REVERSE");

			btnUnique = new Button(grp, SWT.CHECK);
			btnUnique.setText("UNIQUE");

			Composite comColumns = new Composite(grp, SWT.NONE);
			comColumns.setLayout(new GridLayout(2, false));
			comColumns.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true, 2, 1));

			Composite comColumns2 = new Composite(comColumns, SWT.NONE);
			comColumns2.setLayout(new GridLayout());
			comColumns2.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

			TableViewerBuilder tvBuilder = new TableViewerBuilder();
			tvBuilder.setColumnNames(new String[] {Messages.tabTitleColumn, Messages.tabTitleOrder});
			tvBuilder.setColumnWidths(new int[] {180, 50});
			ComboBoxCellEditorFactory comboBoxCellEditorFactory = new ComboBoxCellEditorFactory();
			comboBoxCellEditorFactory.setItems(ITEMS);
			comboBoxCellEditorFactory.setReadOnly(true);
			tvBuilder.setCellEditorClasses(new CellEditorFactory[] {new TextCellEditorFactory(),
					comboBoxCellEditorFactory});
			tvBuilder.setContentProvider(new StructuredContentProviderAdaptor());
			//tvBuilder.setLabelProvider();
			//tvBuilder.setCellModifier();
			tvBuilder.setTableCursorSupported(true);
			tvColumns = tvBuilder.buildTableViewer(comColumns2, SWT.BORDER | SWT.FULL_SELECTION);

			//Buttons
			Composite comIndexBtns = new Composite(comColumns, SWT.NONE);
			comIndexBtns.setLayout(new GridLayout());
			comIndexBtns.setLayoutData(new GridData(SWT.RIGHT, SWT.TOP, false, false));

			btnAddIdx = new Button(comIndexBtns, SWT.NONE);
			btnAddIdx.setText(" + ");
			btnAddIdx.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			btnAddIdx.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent ev) {
					columnsData.add(new Object[] {"", 0});
					tvColumns.refresh();
				}

			});
			btnRemoveIdx = new Button(comIndexBtns, SWT.NONE);
			btnRemoveIdx.setText(" - ");
			btnRemoveIdx.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			btnRemoveIdx.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent ev) {
					final int si = tvColumns.getTable().getSelectionIndex();
					if (si < 0) {
						return;
					}
					columnsData.remove(si);
					tvColumns.refresh();
					final int itemCount = columnsData.size();
					if (itemCount == 0) {
						return;
					}
					if (si < itemCount) {
						tvColumns.getTable().setSelection(si);
					} else {
						tvColumns.getTable().setSelection(itemCount - 1);
					}
				}

			});
		}

		/**
		 * Save UI to Index
		 * 
		 * @return VerifyResultMessages
		 */
		VerifyResultMessages save() {
			idx.setName(txtIndexName.getText());
			idx.setReverse(btnReverse.getSelection());
			idx.setUnique(btnUnique.getSelection());
			//Clear old column settings
			idx.setIndexColumns(null);
			//Add new column
			for (int i = 0; i < columnsData.size(); i++) {
				Object[] obj = (Object[]) tvColumns.getElementAt(i);
				final String column = (String) obj[0];
				if (StringUtils.isNotBlank(column)) {
					idx.addColumn(column, (Integer) obj[1] == 0);
				}
			}
			return new VerifyResultMessages();
		}

		/**
		 * Set the view's read only status
		 * 
		 * @param editable false if read only
		 */
		void setEditable(boolean editable) {
			txtIndexName.setEditable(editable);
			btnReverse.setEnabled(editable);
			btnUnique.setEnabled(editable);
			tvColumns.getTable().setEnabled(editable);
			btnAddIdx.setEnabled(editable);
			btnRemoveIdx.setEnabled(editable);
		}

		/**
		 * Set the index object to be displayed
		 * 
		 * @param idx Index
		 */
		void setIndex(Index idx) {
			this.idx = idx;
			txtTableName.setText(idx.getTable().getName());
			txtIndexName.setText(idx.getName());
			btnUnique.setSelection(idx.isUnique());
			btnReverse.setSelection(idx.isReverse());
			//Set column table viewer input
			columnsData.clear();
			Map<String, Boolean> columns = idx.getIndexColumns();
			for (Entry<String, Boolean> idxCol : columns.entrySet()) {
				Boolean rule = idxCol.getValue();
				rule = (rule == null) ? false : rule;
				columnsData.add(new Object[] {idxCol.getKey(), rule ? 0 : 1});
			}
			tvColumns.setInput(columnsData);
		}
	}
}
