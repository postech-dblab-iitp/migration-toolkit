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
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDSQLHelper;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.navigator.node.ViewNode;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * Edit target view name and SQL statement
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-26 created by Kevin Cao
 */
public class ViewMappingView extends
		AbstractMappingView {

	private Composite container;
	private Text txtTargetName;
	private Text txtTargetSQL;
	private Text txtSourceName;
	private Text txtSourceSQL;
	private Button btnCreate;
	private Button btnReplace;
	private SourceConfig viewConfig;

	public ViewMappingView(Composite parent) {
		super(parent);
	}

	/**
	 * Hide the view.
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(container, true);

	}

	/**
	 * Bring the view onto the top.
	 * 
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
				txtTargetName.setEditable(btnCreate.getSelection());
				txtTargetSQL.setEditable(btnCreate.getSelection());
			}

		});

		btnReplace = new Button(container, SWT.CHECK);
		btnReplace.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnReplace.setText(Messages.lblReplace);

		createSourcePart(container);
		createTargetPart(container);
	}

	/**
	 * Create source part.
	 * 
	 * @param parent of source part
	 */
	private void createSourcePart(Composite parent) {
		Group grpSource = new Group(parent, SWT.NONE);
		grpSource.setLayout(new GridLayout(2, false));
		GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true, 2, 1);
		grpSource.setLayoutData(gd);
		grpSource.setText(Messages.lblSource);

		Label lblSourceName = new Label(grpSource, SWT.NONE);
		lblSourceName.setText(Messages.lblViewName);

		txtSourceName = new Text(grpSource, SWT.BORDER);
		txtSourceName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtSourceName.setEditable(false);
		txtSourceName.setText("");

		Label lblSQL = new Label(grpSource, SWT.NONE);
		lblSQL.setText(Messages.lblViewStatement);

		txtSourceSQL = new Text(grpSource, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.WRAP);
		txtSourceSQL.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		txtSourceSQL.setEditable(false);
		txtSourceSQL.setText("");
	}

	/**
	 * Create target part
	 * 
	 * @param parent of the target part
	 */
	private void createTargetPart(Composite parent) {
		Group grpTarget = new Group(parent, SWT.NONE);
		grpTarget.setLayout(new GridLayout(2, false));
		GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true, 2, 1);
		grpTarget.setLayoutData(gd);
		grpTarget.setText(Messages.lblTarget);

		Label lblTargetName = new Label(grpTarget, SWT.NONE);
		lblTargetName.setText(Messages.lblViewName);

		txtTargetName = new Text(grpTarget, SWT.BORDER);
		txtTargetName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtTargetName.setTextLimit(CUBRIDDataTypeHelper.DB_OBJ_NAME_MAX_LENGTH);
		txtTargetName.setText("");

		Label lblSQL = new Label(grpTarget, SWT.NONE);
		lblSQL.setText(Messages.lblViewStatement);

		txtTargetSQL = new Text(grpTarget, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.WRAP);
		txtTargetSQL.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		txtTargetSQL.setText("");
	}

	/**
	 * Show source view DDL and target view DDL
	 * 
	 * @param obj should be a ViewNode
	 */
	public void showData(Object obj) {
		super.showData(obj);
		if (!(obj instanceof ViewNode)) {
			return;
		}
		btnCreate.setEnabled(false);
		btnReplace.setEnabled(false);
		txtTargetName.setEditable(false);
		txtTargetSQL.setEditable(false);

		ViewNode vNode = (ViewNode) obj;
		View vw = vNode.getView();
		viewConfig = config.getExpViewCfg(vw.getOwner(), vNode.getName());
		vw = config.getSrcViewSchema(vw.getOwner(), vw.getName());
		if (viewConfig == null || vw == null) {
			return;
		}
		txtSourceName.setText(vw.getName());
		txtSourceSQL.setText(vw.getDDL());

		View targetVW = config.getTargetViewSchema(viewConfig.getTarget());
		if (targetVW == null) {
			return;
		}
		txtTargetName.setText(targetVW.getName());
		txtTargetSQL.setText(targetVW.getQuerySpec());
		//Set controls status
		btnCreate.setEnabled(true);
		btnCreate.setSelection(viewConfig.isCreate());
		btnReplace.setEnabled(btnCreate.getSelection());
		btnReplace.setSelection(viewConfig.isReplace() && btnCreate.getSelection());
		txtTargetName.setEditable(btnCreate.getSelection());
		txtTargetSQL.setEditable(btnCreate.getSelection());
	}

	/**
	 * Save UI to view object.
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		if (viewConfig == null) {
			return super.save();
		}
		//Save create option
		viewConfig.setCreate(btnCreate.getSelection());
		viewConfig.setReplace(btnReplace.getSelection());
		if (!btnCreate.getSelection()) {
			return super.save();
		}
		//Validate and Save more information
		View targetVW = config.getTargetViewSchema(viewConfig.getTarget());
		if (targetVW == null) {
			return super.save();
		}
		final String newName = txtTargetName.getText().trim().toLowerCase(Locale.US);
		if (!MigrationCfgUtils.verifyTargetDBObjName(newName)) {
			return new VerifyResultMessages(Messages.bind(Messages.msgErrInvalidViewName, newName),
					null, null);
		}
		if (StringUtils.isBlank(txtTargetSQL.getText())) {
			return new VerifyResultMessages(Messages.msgErrEmptyViewDDL, null, null);
		}
		//Duplicated checking
		if (!newName.equalsIgnoreCase(viewConfig.getTarget())) {
			if (config.isTargetNameInUse(newName)) {
				return new VerifyResultMessages(Messages.bind(Messages.msgErrDupViewName, newName),
						null, null);
			}
		}
		targetVW.setName(newName);
		//targetVW.setDDL(txtTargetSQL.getText().trim());
		targetVW.setQuerySpec(txtTargetSQL.getText().trim());
		//Update DDL sql.
		final CUBRIDSQLHelper ddlUtils = CUBRIDSQLHelper.getInstance(null);
		targetVW.setDDL(ddlUtils.getViewDDL(targetVW));
		viewConfig.setTarget(targetVW.getName());
		return super.save();
	}
}
