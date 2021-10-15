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

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * Edit target view name and SQL statement
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-26 created by Kevin Cao
 */
public class PartitionMappingView extends
		AbstractMappingView {

	private Composite container;
	private Button btnCreate;
	private Group grpSource;
	private Group grpTarget;
	private Text txtSrcSQL;
	private Text txtTargetSQL;

	private SourceEntryTableConfig setc;
	private Table targetTable;

	public PartitionMappingView(Composite parent) {
		super(parent);
	}

	/**
	 * Hide this view
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(container, true);

	}

	/**
	 * Bring this view onto top.
	 */
	public void show() {
		CompositeUtils.hideOrShowComposite(container, false);

	}

	/**
	 * Create controls
	 * 
	 * @param parent of controls to be created
	 */
	protected void createControl(Composite parent) {
		container = new Composite(parent, SWT.NONE);
		GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
		container.setLayoutData(gd);
		container.setLayout(new GridLayout());

		btnCreate = new Button(container, SWT.CHECK);
		btnCreate.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnCreate.setText(Messages.lblCreate);
		btnCreate.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				txtTargetSQL.setEditable(btnCreate.getSelection());
			}
		});

		SashForm form = new SashForm(container, SWT.VERTICAL);
		form.setLayout(new GridLayout());
		form.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		createSourcePart(form);
		createTargetPart(form);
		form.setWeights(new int[] { 50, 50 });
	}

	/**
	 * Create composites that show source information
	 * 
	 * @param parent of the source composites
	 */
	private void createSourcePart(Composite parent) {
		grpSource = new Group(parent, SWT.NONE);
		grpSource.setLayout(new GridLayout());
		GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
		grpSource.setLayoutData(gd);
		grpSource.setText(Messages.lblSourceTablePartition);

		txtSrcSQL = new Text(grpSource, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.WRAP);
		txtSrcSQL.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		txtSrcSQL.setEditable(false);
		txtSrcSQL.setText("");

	}

	/**
	 * Create composites that show target information
	 * 
	 * @param parent of the target composites
	 */
	private void createTargetPart(Composite parent) {
		grpTarget = new Group(parent, SWT.NONE);
		grpTarget.setLayout(new GridLayout());
		GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
		grpTarget.setLayoutData(gd);
		grpTarget.setText(Messages.lblTargetTablePartition);

		txtTargetSQL = new Text(grpTarget, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.WRAP);
		txtTargetSQL.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		txtTargetSQL.setText("");
	}

	/**
	 * Show table partition information
	 * 
	 * @param obj should be TableNode or PartitionsNode or PartitionNode
	 */
	public void showData(Object obj) {
		if (!(obj instanceof SourceEntryTableConfig)) {
			throw new IllegalArgumentException(
					"The data to be shown must be SourceEntryTableConfig.");
		}
		super.showData(obj);
		setc = (SourceEntryTableConfig) obj;

		txtSrcSQL.setText("");
		txtTargetSQL.setText("");
		btnCreate.setEnabled(false);
		txtTargetSQL.setEditable(false);
		targetTable = null;
		Table srcTable = config.getSrcTableSchema(setc.getOwner(), setc.getName());
		if (srcTable == null || srcTable.getPartitionInfo() == null) {
			return;
		}
		final String srcDDL = srcTable.getPartitionInfo().getDDL();
		txtSrcSQL.setText(srcDDL == null ? "" : srcDDL);

		targetTable = config.getTargetTableSchema(setc.getTarget());
		if (targetTable == null) {
			return;
		}
		if (targetTable.getPartitionInfo() == null) {
			PartitionInfo pi = new PartitionInfo();
			pi.setDDL(srcDDL);
			targetTable.setPartitionInfo(pi);
		}
		btnCreate.setEnabled(true);
		btnCreate.setSelection(setc.isCreatePartition());
		txtTargetSQL.setEditable(btnCreate.getEnabled() && btnCreate.getSelection());
		final String tarDDL = targetTable.getPartitionInfo().getDDL();
		txtTargetSQL.setText(tarDDL == null ? "" : tarDDL);
	}

	/**
	 * Save partition. If source table doesn't support table partition, do
	 * nothing here.
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		if (targetTable == null || setc == null || !btnCreate.getEnabled()) {
			return super.save();
		}
		setc.setCreatePartition(btnCreate.getSelection());
		if (!setc.isCreatePartition()) {
			return super.save();
		}
		final String tarDDL = txtTargetSQL.getText().trim();
		if (StringUtils.isEmpty(tarDDL)) {
			return new VerifyResultMessages(Messages.bind(Messages.msgErrEmptyPartition,
					setc.getTarget()), null, null);
		}
		targetTable.getPartitionInfo().setDDL(tarDDL);
		return super.save();
	}

	/**
	 * Set the view's editable status, if the target table doesn't support
	 * partitions, the method will do nothing.
	 * 
	 * @param selection true if read only
	 */
	public void setEditable(boolean selection) {
		if (targetTable == null || targetTable.getPartitionInfo() == null) {
			return;
		}
		btnCreate.setEnabled(selection);
		txtTargetSQL.setEditable(btnCreate.getEnabled() && btnCreate.getSelection() && selection);
	}
}
