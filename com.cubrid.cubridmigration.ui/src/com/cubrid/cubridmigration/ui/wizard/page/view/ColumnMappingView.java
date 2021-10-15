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

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

import com.cubrid.common.ui.listener.IntegerVerifyListener;
import com.cubrid.common.ui.swt.Resources;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.TableOrView;
import com.cubrid.cubridmigration.core.engine.UserDefinedDataHandlerManager;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.navigator.node.ColumnNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SQLTableNode;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * ColumnMappingView Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-26 created by Kevin Cao
 */
public class ColumnMappingView extends
		AbstractMappingView {

	private final CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);

	/**
	 * ColumnInfoComposite responses to display one column's information
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-8-22 created by Kevin Cao
	 */
	private class ColumnInfoComposite {
		private Group grp;
		private Text txtTableName;
		private Text txtColumnName;
		private Text txtDataType;
		private Button btnNullable;
		private Button btnUnique;
		private Label lblDefault;
		private Text txtDefault;
		private Button btnShared;
		private Text txtShared;
		private Button btnAutoIncrement;
		private Text txtSeed;
		private Text txtIncrement;

		private Button btnExpression;

		private Column column;

		private boolean editable = true;

		ColumnInfoComposite(Composite parent, String name) {
			grp = new Group(parent, SWT.NONE);
			grp.setLayout(new GridLayout(2, false));
			GridData gd = new GridData(SWT.LEFT, SWT.FILL, false, true);
			gd.widthHint = PART_WIDTH + 70;
			grp.setLayoutData(gd);
			grp.setText(name);

			Label lblTableName = new Label(grp, SWT.NONE);
			lblTableName.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblTableName.setText(Messages.lblTableName);

			txtTableName = new Text(grp, SWT.BORDER);
			txtTableName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtTableName.setEditable(false);
			txtTableName.setText("");

			Label lblColumnName = new Label(grp, SWT.NONE);
			lblColumnName.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblColumnName.setText(Messages.lblColumnName);

			txtColumnName = new Text(grp, SWT.BORDER);
			txtColumnName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtColumnName.setTextLimit(CUBRIDDataTypeHelper.DB_OBJ_NAME_MAX_LENGTH);
			txtColumnName.setText("");

			Label lblDataType = new Label(grp, SWT.NONE);
			lblDataType.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblDataType.setText(Messages.lblDataType);

			txtDataType = new Text(grp, SWT.BORDER);
			txtDataType.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtDataType.setText("");

			btnNullable = new Button(grp, SWT.CHECK);
			btnNullable.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			btnNullable.setText("Nullable");

			btnUnique = new Button(grp, SWT.CHECK);
			btnUnique.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
			btnUnique.setText("Unique");

			lblDefault = new Label(grp, SWT.NONE);
			lblDefault.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblDefault.setText(Messages.lblDefaultValue);

			Composite comDefault = new Composite(grp, SWT.NONE);
			comDefault.setLayout(new GridLayout(2, false));
			comDefault.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtDefault = new Text(comDefault, SWT.BORDER);
			txtDefault.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtDefault.setText("");

			btnExpression = new Button(comDefault, SWT.CHECK);
			btnExpression.setText(Messages.btnDefaultExpression);

			btnShared = new Button(grp, SWT.CHECK);
			btnShared.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			btnShared.setText(Messages.lblShared);
			btnShared.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent ev) {
					txtShared.setEditable(btnShared.getSelection());
				}
			});

			txtShared = new Text(grp, SWT.BORDER);
			txtShared.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtShared.setText("");

			btnAutoIncrement = new Button(grp, SWT.CHECK);
			btnAutoIncrement.setText(Messages.lblAutoIncrement);
			btnAutoIncrement.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent ev) {
					txtSeed.setEditable(btnAutoIncrement.getSelection());
					txtIncrement.setEditable(btnAutoIncrement.getSelection());
				}
			});

			new Label(grp, SWT.NONE);

			Label lblSeed = new Label(grp, SWT.NONE);
			lblSeed.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblSeed.setText(Messages.lblSeed);

			txtSeed = new Text(grp, SWT.BORDER);
			txtSeed.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtSeed.setTextLimit(39);
			txtSeed.setText("1");
			txtSeed.addVerifyListener(new IntegerVerifyListener());

			Label lblIncrement = new Label(grp, SWT.NONE);
			lblIncrement.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblIncrement.setText(Messages.lblIncrement);

			txtIncrement = new Text(grp, SWT.BORDER);
			txtIncrement.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtIncrement.setTextLimit(39);
			txtIncrement.setText("1");
			txtIncrement.addVerifyListener(new IntegerVerifyListener());
		}

		/**
		 * Save UI to column
		 * 
		 * @return VerifyResultMessages
		 */
		VerifyResultMessages save() {
			if (column == null) {
				return new VerifyResultMessages("Column is not specified.", null, null);
			}
			final String newName = txtColumnName.getText().trim().toLowerCase(Locale.US);
			if (!MigrationCfgUtils.verifyTargetDBObjName(newName)) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidColumnName,
						column.getTableOrView().getName(), newName), null, null);
			}
			if (!newName.equalsIgnoreCase(column.getName())) {
				if (column.getTableOrView().getColumnByName(newName) != null) {
					return new VerifyResultMessages(Messages.bind(Messages.msgErrDupColumnName,
							column.getTableOrView().getName(), column.getName()), null, null);
				}
			}
			if (!dataTypeHelper.isValidDatatype(txtDataType.getText().trim())) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidDataType,
						column.getTableOrView().getName(), column.getName()), null, null);
			}
			column.setName(newName);
			dataTypeHelper.setColumnDataType(txtDataType.getText().trim(), column);
			column.setNullable(btnNullable.getSelection());
			column.setUnique(btnUnique.getSelection());
			String defaultValueText = txtDefault.getText();
			defaultValueText = "NULL".equalsIgnoreCase(defaultValueText) ? "" : defaultValueText;
			if (!btnExpression.getSelection()
					&& !dataTypeHelper.isValidValue(column.getShownDataType(), defaultValueText)) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrDefaultValue,
						column.getTableOrView().getName(), column.getName()), null, null);
			}
			column.setDefaultValue(txtDefault.getText());
			column.setDefaultIsExpression(btnExpression.getSelection());

			column.setShared(btnShared.getSelection());
			if (btnShared.getSelection()) {
				column.setSharedValue(txtShared.getText());
			}
			column.setAutoIncrement(btnAutoIncrement.getSelection());
			if (btnAutoIncrement.getSelection()) {
				column.setAutoIncSeedVal(Long.valueOf(txtSeed.getText()));
				column.setAutoIncIncrVal(Long.valueOf(txtIncrement.getText()));
			}
			return new VerifyResultMessages();
		}

		/**
		 * Show column information
		 * 
		 * @param column to be shown
		 */
		void setColumn(Column column) {
			this.column = column;
			txtTableName.setText(column.getTableOrView().getName());
			txtColumnName.setText(column.getName());
			txtDataType.setText(column.getShownDataType());
			btnNullable.setSelection(column.isNullable());
			btnUnique.setSelection(column.isUnique());
			txtDefault.setText(column.getDefaultValueDisplayString());
			btnExpression.setSelection(column.isDefaultIsExpression());
			btnShared.setSelection(column.isShared());
			txtShared.setEditable(column.isShared() && editable);
			txtShared.setText(column.getSharedValue() == null ? "" : column.getSharedValue());
			btnAutoIncrement.setSelection(column.isAutoIncrement());
			txtSeed.setEditable(column.isAutoIncrement() && editable);
			txtSeed.setText(Long.toString(column.getAutoIncSeedVal()));

			txtIncrement.setEditable(column.isAutoIncrement() && editable);
			txtIncrement.setText(Long.toString(column.getAutoIncIncrVal()));
		}

		/**
		 * Set the read-only status
		 * 
		 * @param editable boolean
		 */
		void setEditable(boolean editable) {
			txtColumnName.setEditable(editable);
			txtDataType.setEditable(editable);
			btnNullable.setEnabled(editable);
			btnUnique.setEnabled(editable);
			txtDefault.setEditable(editable);
			btnExpression.setEnabled(editable);
			btnShared.setEnabled(editable);
			txtShared.setEditable(btnShared.getSelection() && editable);
			btnAutoIncrement.setEnabled(editable);
			txtSeed.setEditable(btnAutoIncrement.getSelection() && editable);
			txtIncrement.setEditable(btnAutoIncrement.getSelection() && editable);
		}
	}

	private Composite container;

	//Source composites
	private ColumnInfoComposite grpSource;

	//Target composites
	private ColumnInfoComposite grpTarget;

	private SourceColumnConfig scc;

	private Button btnCreate;

	private Button btnTrim;

	private Text txtReplace;

	private Text txtHandlerPath;

	public ColumnMappingView(Composite parent) {
		super(parent);
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
		container.setLayout(new GridLayout(3, true));

		btnCreate = new Button(container, SWT.CHECK);
		btnCreate.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false, 2, 1));
		btnCreate.setText(Messages.lblMigrateColumn);
		btnCreate.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent se) {
				grpTarget.setEditable(btnCreate.getEnabled() && btnCreate.getSelection());
			}
		});

		new Label(container, SWT.NONE).setText("");

		createSourcePart(container);
		createTargetPart(container);

		new Label(container, SWT.NONE).setText("");

		Group grpAdvanced = new Group(container, SWT.NONE);
		grpAdvanced.setLayout(new GridLayout());
		grpAdvanced.setLayoutData(new GridData(SWT.FILL, SWT.FILL, false, false, 2, 1));

		btnTrim = new Button(grpAdvanced, SWT.CHECK);
		btnTrim.setText(Messages.lblTrimValue);

		Label lblReplace = new Label(grpAdvanced, SWT.NONE);
		lblReplace.setText(Messages.lblReplacExp);

		txtReplace = new Text(grpAdvanced, SWT.BORDER);
		txtReplace.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtReplace.setText("");

		Group grpDataHandler = new Group(container, SWT.NONE);
		grpDataHandler.setLayout(new GridLayout(2, false));
		grpDataHandler.setLayoutData(new GridData(SWT.FILL, SWT.FILL, false, false, 2, 1));
		grpDataHandler.setText(Messages.lblUserDefinedDataHandler);

		Label lblHandlerFormat = new Label(grpDataHandler, SWT.NONE);
		lblHandlerFormat.setText(Messages.lblUserDefinedDataHandlerFormat);
		lblHandlerFormat.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		Label lblHandlerExample = new Label(grpDataHandler, SWT.NONE);
		lblHandlerExample.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		lblHandlerExample.setText("[ test.jar:com.cubrid.migration.TestConvert ]");
		lblHandlerExample.setCursor(Resources.getInstance().getCursor(SWT.CURSOR_HAND));
		lblHandlerExample.setForeground(Resources.getInstance().getColor(SWT.COLOR_BLUE));
		lblHandlerExample.addMouseListener(new MouseAdapter() {
			public void mouseDown(MouseEvent me) {
				Program.launch("http://www.cubrid.org/wiki_tools/entry/cmt_manual_qanda");
			}
		});

		txtHandlerPath = new Text(grpDataHandler, SWT.BORDER);
		txtHandlerPath.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
		txtHandlerPath.setText("");

	}

	/**
	 * 
	 * Create source part controls
	 * 
	 * @param parent Composite
	 * 
	 */
	protected void createSourcePart(Composite parent) {
		grpSource = new ColumnInfoComposite(container, Messages.lblSource);
		grpSource.setEditable(false);
	}

	/**
	 * 
	 * Create target part controls
	 * 
	 * @param parent Composite
	 * 
	 */
	protected void createTargetPart(Composite parent) {
		grpTarget = new ColumnInfoComposite(container, Messages.lblTarget);
	}

	/**
	 * 
	 * Hide
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(container, true);
	}

	/**
	 * Save UI to configuration
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		scc.setCreate(btnCreate.getSelection());
		if (!scc.isCreate()) {
			return super.save();
		}
		final VerifyResultMessages result = grpTarget.save();
		if (!result.hasError()) {
			scc.setTarget(grpTarget.column.getName());
		}
		scc.setNeedTrim(btnTrim.getSelection());
		scc.setReplaceExpression(txtReplace.getText());
		//User defined handler
		UserDefinedDataHandlerManager udf = UserDefinedDataHandlerManager.getInstance();
		String handler = txtHandlerPath.getText().trim();
		if (StringUtils.isNotBlank(handler)) {
			if (!udf.putColumnDataHandler(handler, true)) {
				return new VerifyResultMessages(Messages.errInvalidHandlerSetting, null, null);
			}
			scc.setUserDataHandler(handler);
		} else {
			scc.setUserDataHandler(null);
		}
		return result;
	}

	/**
	 * 
	 * Show view
	 */
	public void show() {
		CompositeUtils.hideOrShowComposite(container, false);
	}

	/**
	 * Show model
	 * 
	 * @param obj Object to be shown
	 */
	public void showData(Object obj) {
		super.showData(obj);
		if (!(obj instanceof ColumnNode)) {
			return;
		}
		ColumnNode cNode = (ColumnNode) obj;
		btnCreate.setVisible(!(cNode.getParent().getParent() instanceof SQLTableNode));
		Column column = cNode.getColumn();
		TableOrView st = column.getTableOrView();
		scc = config.getExpColumnCfg(st.getOwner(), st.getName(), column.getName());

		grpTarget.setEditable(false);
		if (scc == null) {
			return;
		}
		Column srcCol = config.getSrcColumnSchema(st.getOwner(), st.getName(), column.getName());
		if (srcCol == null) {
			return;
		}
		grpSource.setColumn(srcCol);
		Column tarCol = config.getTargetColumnSchema(scc.getParent().getTarget(), scc.getTarget());
		if (tarCol == null) {
			return;
		}

		btnCreate.setEnabled(scc.getParent().isCreateNewTable() || scc.getParent().isMigrateData());
		btnCreate.setSelection(scc.isCreate());

		grpTarget.setEditable(btnCreate.getEnabled() && btnCreate.getSelection());
		grpTarget.setColumn(tarCol);

		btnTrim.setSelection(scc.isNeedTrim());
		txtReplace.setText(scc.getReplaceExp());
		txtHandlerPath.setText(scc.getUserDataHandler() == null ? "" : scc.getUserDataHandler());
	}
}
