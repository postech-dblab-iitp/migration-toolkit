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

import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceFKConfig;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.navigator.node.FKNode;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * FKMappingView responses to mapping foreign keys
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-26 created by Kevin Cao
 */
public class FKMappingView extends
		AbstractMappingView {
	private static final String[] FK_ACTIONS = new String[] {"CASCADE", "RESTRICT", "SET NULL",
			"NO ACTION"};

	private Composite container;
	private FKView grpSource;
	private FKView grpTarget;

	private Button btnCreate;
	private Button btnReplace;

	private SourceFKConfig sfkc;

	public FKMappingView(Composite parent) {
		super(parent);
	}

	/**
	 * Hide this view
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(container, true);

	}

	/**
	 * Show this view
	 */
	public void show() {
		CompositeUtils.hideOrShowComposite(container, false);

	}

	/**
	 * Create controls
	 * 
	 * @param parent of the controls
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
	 * Create source foreign key UI
	 * 
	 * @param parent Composite
	 */
	private void createSourcePart(Composite parent) {
		grpSource = new FKView(parent, Messages.lblSource);
		grpSource.setEditable(false);
	}

	/**
	 * Create target foreign key UI
	 * 
	 * @param parent Composite
	 */
	private void createTargetPart(Composite parent) {
		grpTarget = new FKView(parent, Messages.lblTarget);
	}

	/**
	 * Show foreign key node
	 * 
	 * @param obj FKNode
	 */
	public void showData(Object obj) {
		super.showData(obj);
		if (!(obj instanceof FKNode)) {
			return;
		}
		btnCreate.setEnabled(false);
		btnReplace.setEnabled(false);
		grpTarget.setEditable(false);

		FKNode fknode = (FKNode) obj;
		FK srcFK = fknode.getFk();
		grpSource.setFK(srcFK);

		SourceEntryTableConfig setc = config.getExpEntryTableCfg(srcFK.getTable().getOwner(),
				srcFK.getTable().getName());
		if (setc == null) {
			return;
		}
		sfkc = setc.getFKConfig(srcFK.getName());
		if (sfkc == null) {
			return;
		}

		Table tt = config.getTargetTableSchema(setc.getTarget());
		if (tt == null) {
			return;
		}
		FK tfk = tt.getFKByName(sfkc.getTarget());
		if (tfk == null) {
			return;
		}
		grpTarget.setFK(tfk);
		grpTarget.setEditable(true);

		btnCreate.setEnabled(setc.isCreateNewTable());
		btnCreate.setSelection(setc.isCreateNewTable() && sfkc.isCreate());
		btnReplace.setEnabled(btnCreate.getSelection());
		btnReplace.setSelection(btnCreate.getSelection() && sfkc.isReplace());
		grpTarget.setEditable(btnCreate.getSelection());

	}

	/**
	 * Save
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		if (sfkc == null) {
			return super.save();
		}
		sfkc.setCreate(btnCreate.getSelection());
		sfkc.setReplace(btnReplace.getSelection());
		if (!sfkc.isCreate()) {
			return super.save();
		}

		VerifyResultMessages result = grpTarget.save();
		if (!result.hasError()) {
			sfkc.setTarget(grpTarget.fk.getName());
		}
		return result;
	}

	/**
	 * Show foreign key information
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-8-22 created by Kevin Cao
	 */
	private class FKView {
		private Group grp;
		private Text txtTableName;
		private Text txtFKName;
		private Text txtTableColumns;
		private Text txtRefTableName;
		private Text txtRefTableColumns;
		private Combo cboUpdateAction;
		private Combo cboDeleteAction;
		//private Text cboCacheObjAction;

		private FK fk;

		FKView(Composite parent, String name) {
			grp = new Group(parent, SWT.NONE);
			grp.setLayout(new GridLayout(2, false));
			GridData gd = new GridData(SWT.LEFT, SWT.FILL, false, true);
			gd.widthHint = PART_WIDTH + 50;
			grp.setLayoutData(gd);
			grp.setText(name);

			//Table Name
			Label lblTableName = new Label(grp, SWT.NONE);
			lblTableName.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblTableName.setText(Messages.lblTableName);

			txtTableName = new Text(grp, SWT.BORDER);
			txtTableName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtTableName.setEditable(false);
			txtTableName.setText("");
			//FK name
			Label lblFKName = new Label(grp, SWT.NONE);
			lblFKName.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblFKName.setText(Messages.lblFKName);

			txtFKName = new Text(grp, SWT.BORDER);
			txtFKName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtFKName.setText("");
			//Table columns
			Label lblTableColumns = new Label(grp, SWT.NONE);
			lblTableColumns.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblTableColumns.setText(Messages.lblFKColumnInfo);

			txtTableColumns = new Text(grp, SWT.BORDER);
			txtTableColumns.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtTableColumns.setText("");
			//Ref table name
			Label lblRefTableName = new Label(grp, SWT.NONE);
			lblRefTableName.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblRefTableName.setText(Messages.lblFKRefTableName);

			txtRefTableName = new Text(grp, SWT.BORDER);
			txtRefTableName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtRefTableName.setText("");
			//ref columns
			Label lblRefTableColumns = new Label(grp, SWT.NONE);
			lblRefTableColumns.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblRefTableColumns.setText(Messages.lblFKRefColumns);

			txtRefTableColumns = new Text(grp, SWT.BORDER);
			txtRefTableColumns.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtRefTableColumns.setText("");
			//On update
			Label btnOnUpdate = new Label(grp, SWT.CHECK);
			btnOnUpdate.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			btnOnUpdate.setText("On Update: ");

			cboUpdateAction = new Combo(grp, SWT.BORDER | SWT.READ_ONLY);
			cboUpdateAction.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			cboUpdateAction.setItems(FK_ACTIONS);
			cboUpdateAction.select(1);

			//On delete
			Label btnOnDelete = new Label(grp, SWT.CHECK);
			btnOnDelete.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			btnOnDelete.setText("On Delete: ");

			cboDeleteAction = new Combo(grp, SWT.BORDER | SWT.READ_ONLY);
			cboDeleteAction.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			cboDeleteAction.setItems(FK_ACTIONS);
			cboDeleteAction.select(1);

			//On cache object
			//			Label btnOnCacheObj = new Label(grp, SWT.NONE);
			//			btnOnCacheObj.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER,
			//					false, false));
			//			btnOnCacheObj.setText("On Cache Object");
			//
			//			cboCacheObjAction = new Text(grp, SWT.BORDER);
			//			cboCacheObjAction.setLayoutData(new GridData(SWT.FILL, SWT.CENTER,
			//					true, false));
			//			cboCacheObjAction.setText("");
		}

		/**
		 * Validate and save
		 * 
		 * @return VerifyResultMessages
		 */
		VerifyResultMessages save() {
			if (fk == null) {
				return new VerifyResultMessages("Foreign key is not specified.", null, null);
			}
			final String newName = txtFKName.getText().trim().toLowerCase(Locale.US);
			if (!MigrationCfgUtils.verifyTargetDBObjName(newName)) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidFKName,
						sfkc.getParent().getTarget(), newName), null, null);
			}
			if (!newName.equalsIgnoreCase(fk.getName())) {
				if (fk.getTable().getFKByName(newName) != null
						|| fk.getTable().getIndexByName(newName) != null) {
					return new VerifyResultMessages(Messages.bind(Messages.msgErrDupFKName,
							fk.getTable().getName(), newName), null, null);
				}
			}
			if (StringUtils.isBlank(txtRefTableName.getText())) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrEmptyRefTable,
						fk.getTable().getName(), newName), null, null);
			}
			Map<String, String> columns = new TreeMap<String, String>();
			String[] array1 = txtTableColumns.getText().split(",");
			String[] array2 = txtRefTableColumns.getText().split(",");
			if (array1.length != array2.length || array1.length == 0) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidFKColumns,
						fk.getTable().getName(), newName), null, null);
			}
			for (int i = 0; i < array1.length; i++) {
				if (!MigrationCfgUtils.verifyTargetDBObjName(array1[i])
						|| !MigrationCfgUtils.verifyTargetDBObjName(array2[i])) {
					return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidFKColumns,
							fk.getTable().getName(), newName), null, null);
				}
				if (fk.getTable().getColumnByName(array1[i]) == null) {
					return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidFKColumns,
							fk.getTable().getName(), newName), null, null);
				}
				columns.put(array1[i], array2[i]);
			}
			if (cboUpdateAction.getSelectionIndex() == 0) {
				return new VerifyResultMessages(Messages.bind(
						Messages.errCascadeOnUpdateNotSupported, fk.getTable().getName(), newName),
						null, null);
			}
			fk.setName(newName);
			fk.setReferencedTableName(txtRefTableName.getText());
			fk.setColumns(columns);
			fk.setUpdateRule(cboUpdateAction.getSelectionIndex());
			fk.setDeleteRule(cboDeleteAction.getSelectionIndex());
			//fk.setOnCacheObject(cboCacheObjAction.getText());
			return new VerifyResultMessages();
		}

		/**
		 * Set foreign key information
		 * 
		 * @param fk FK
		 */
		void setFK(FK fk) {
			this.fk = fk;
			txtTableName.setText(fk.getTable().getName());
			txtFKName.setText(fk.getName());
			StringBuffer columns = new StringBuffer();
			StringBuffer refcolumns = new StringBuffer();
			for (Entry<String, String> entry : fk.getColumns().entrySet()) {
				if (columns.length() > 0) {
					columns.append(',');
					refcolumns.append(',');
				}
				columns.append(entry.getKey());
				refcolumns.append(entry.getValue());
			}
			txtTableColumns.setText(columns.toString());
			txtRefTableName.setText(fk.getReferencedTableName());
			txtRefTableColumns.setText(refcolumns.toString());
			cboUpdateAction.select(fk.getUpdateRule());
			cboDeleteAction.select(fk.getDeleteRule());
			//			cboCacheObjAction.setText(fk.getOnCacheObject() == null ? ""
			//					: fk.getOnCacheObject());
		}

		/**
		 * Set read only
		 * 
		 * @param editable false if read only
		 */
		void setEditable(boolean editable) {
			txtFKName.setEditable(editable);
			txtTableColumns.setEditable(editable);
			txtRefTableName.setEditable(editable);
			txtRefTableColumns.setEditable(editable);
			cboUpdateAction.setEnabled(editable);
			cboDeleteAction.setEnabled(editable);
			//cboCacheObjAction.setEditable(editable);
		}
	}
}
