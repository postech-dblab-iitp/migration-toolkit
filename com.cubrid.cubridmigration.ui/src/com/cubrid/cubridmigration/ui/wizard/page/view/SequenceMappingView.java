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

import java.math.BigInteger;
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
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;

import com.cubrid.common.ui.listener.IntegerVerifyListener;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.navigator.node.SequenceNode;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.utils.MigrationCfgUtils;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * SequenceMappingView responses to construct sequence mapping UI.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-26 created by Kevin Cao
 */
public class SequenceMappingView extends
		AbstractMappingView {
	private static final BigInteger BIGINTEGER0 = new BigInteger("0");

	private Composite container;
	private SerialInfoComposite grpSource;
	private SerialInfoComposite grpTarget;

	private Button btnCreate;
	private Button btnReplace;

	private SourceSequenceConfig serialConfig;

	public SequenceMappingView(Composite parent) {
		super(parent);
	}

	/**
	 * Hide
	 * 
	 */
	public void hide() {
		CompositeUtils.hideOrShowComposite(container, true);
	}

	/**
	 * Show
	 */
	public void show() {
		CompositeUtils.hideOrShowComposite(container, false);
	}

	/**
	 * @param parent of the composites
	 */
	protected void createControl(Composite parent) {
		container = new Composite(parent, SWT.NONE);
		GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
		gd.exclude = true;
		container.setLayoutData(gd);
		container.setVisible(false);
		container.setLayout(new GridLayout(2, true));

		btnCreate = new Button(container, SWT.CHECK);
		btnCreate.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		btnCreate.setText(Messages.lblCreate);
		btnCreate.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				setButtonsStatus();
			}

		});

		btnReplace = new Button(container, SWT.CHECK);
		btnReplace.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
		btnReplace.setText(Messages.lblReplace);

		createSourcePart(container);
		createTargetPart(container);
	}

	/**
	 * 
	 * @param parent of source object
	 * 
	 */
	protected void createSourcePart(Composite parent) {
		grpSource = new SerialInfoComposite(parent, Messages.lblSource);
		grpSource.setEditable(false);
	}

	/**
	 * @param parent of target object
	 * 
	 */
	protected void createTargetPart(Composite parent) {
		grpTarget = new SerialInfoComposite(parent, Messages.lblTarget);
	}

	/**
	 * @param obj should be a SequenceNode
	 */
	public void showData(Object obj) {
		super.showData(obj);
		if (!(obj instanceof SequenceNode)) {
			return;
		}
		Sequence sequ = ((SequenceNode) obj).getSequence();
		if (sequ == null) {
			grpTarget.setEditable(false);
			return;
		}
		serialConfig = config.getExpSerialCfg(sequ.getOwner(), sequ.getName());
		if (serialConfig == null) {
			grpTarget.setEditable(false);
			return;
		}
		grpSource.setSerial(sequ);
		btnCreate.setSelection(serialConfig.isCreate());

		Sequence tseq = config.getTargetSerialSchema(serialConfig.getTarget());
		if (tseq == null) {
			grpTarget.setEditable(false);
			return;
		}
		grpTarget.setEditable(serialConfig.isCreate());
		grpTarget.setSerial(tseq);

		setButtonsStatus();
	}

	/**
	 * Verify input and save UI to object
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		if (grpSource.seq == null || grpTarget.seq == null) {
			return super.save();
		}
		serialConfig.setCreate(btnCreate.getSelection());
		serialConfig.setReplace(btnReplace.getSelection());
		if (serialConfig.isCreate()) {
			final VerifyResultMessages result = grpTarget.save();
			if (!result.hasError()) {
				serialConfig.setTarget(grpTarget.seq.getName());
			}
			return result;
		}
		return super.save();
	}

	/**
	 * Set the buttons status
	 * 
	 */
	private void setButtonsStatus() {
		btnReplace.setSelection(btnCreate.getSelection());
		btnReplace.setEnabled(btnCreate.getSelection());
		grpTarget.setEditable(btnCreate.getSelection());
	}

	/**
	 * UI Class to display sequence object
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-8-15 created by Kevin Cao
	 */
	private class SerialInfoComposite {

		private Group grp;
		private Text txtName;
		private Text txtStart;
		private Text txtIncrement;
		private Button btnMin;
		private Text txtMin;
		private Button btnMax;
		private Text txtMax;
		private Button btnCache;
		private Spinner txtCache;
		private Button btnCycle;
		private Button specifyStartValue;

		private Sequence seq;
		private boolean editable = true;

		SerialInfoComposite(Composite parent, String name) {
			grp = new Group(parent, SWT.NONE);
			grp.setLayout(new GridLayout(2, false));
			GridData gd = new GridData(SWT.LEFT, SWT.FILL, false, true);
			gd.widthHint = PART_WIDTH;
			grp.setLayoutData(gd);
			grp.setText(name);

			Label lblTableName = new Label(grp, SWT.NONE);
			lblTableName.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblTableName.setText(Messages.lblSerialName);

			txtName = new Text(grp, SWT.BORDER);
			txtName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtName.setTextLimit(CUBRIDDataTypeHelper.DB_OBJ_NAME_MAX_LENGTH);
			txtName.setText("");

			specifyStartValue = new Button(grp, SWT.CHECK);
			specifyStartValue.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			specifyStartValue.setText(Messages.lblStartValue);
			specifyStartValue.addSelectionListener(new SelectionAdapter() {
				public void widgetSelected(SelectionEvent ev) {
					txtStart.setEditable(specifyStartValue.getSelection());
				}
			});

			txtStart = new Text(grp, SWT.BORDER);
			txtStart.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtStart.addVerifyListener(new IntegerVerifyListener());
			txtStart.setTextLimit(39);
			txtStart.setText("1");

			Label lblIncrement = new Label(grp, SWT.NONE);
			lblIncrement.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			lblIncrement.setText(Messages.lblIncrementValue);

			txtIncrement = new Text(grp, SWT.BORDER);
			txtIncrement.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtIncrement.setTextLimit(39);
			txtIncrement.addVerifyListener(new IntegerVerifyListener());
			txtIncrement.setText("1");

			btnMin = new Button(grp, SWT.CHECK);
			btnMin.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			btnMin.setText(Messages.lblMinValue);
			btnMin.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent ev) {
					txtMin.setEditable(btnMin.getSelection());
				}

			});

			txtMin = new Text(grp, SWT.BORDER);
			txtMin.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtMin.setTextLimit(39);
			txtMin.addVerifyListener(new IntegerVerifyListener());
			txtMin.setText("1");

			btnMax = new Button(grp, SWT.CHECK);
			btnMax.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			btnMax.setText(Messages.lblMaxValue);
			btnMax.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent ev) {
					txtMax.setEditable(btnMax.getSelection());
				}

			});

			txtMax = new Text(grp, SWT.BORDER);
			txtMax.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtMax.setTextLimit(39);
			txtMax.addVerifyListener(new IntegerVerifyListener());
			txtMax.setText(IntegerVerifyListener.DEFAULT_MAX_VALUE);

			btnCache = new Button(grp, SWT.CHECK);
			btnCache.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
			btnCache.setText(Messages.lblCacheNum);
			btnCache.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent ev) {
					txtCache.setEnabled(btnCache.getSelection());
				}

			});

			txtCache = new Spinner(grp, SWT.BORDER);
			txtCache.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
			txtCache.setMaximum(Integer.MAX_VALUE);
			txtCache.setMinimum(2);
			txtCache.setSelection(2);

			Label sep = new Label(grp, SWT.NONE);
			sep.setText("");

			btnCycle = new Button(grp, SWT.CHECK);
			btnCycle.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
			btnCycle.setText(Messages.btnCycle);
		}

		/**
		 * Set sequence to UI
		 * 
		 * @param seq to be shown
		 */
		void setSerial(Sequence seq) {
			this.seq = seq;
			txtName.setText(seq.getName());
			specifyStartValue.setSelection(!serialConfig.isAutoSynchronizeStartValue());
			txtStart.setText(seq.getCurrentValue().toString());
			txtIncrement.setText(seq.getIncrementBy().toString());
			btnMin.setSelection(!seq.isNoMinValue());
			txtMin.setEditable(!seq.isNoMinValue() && editable);
			txtMin.setText(seq.getMinValue().toString());
			btnMax.setSelection(!seq.isNoMaxValue());
			txtMax.setEditable(!seq.isNoMaxValue() && editable);
			txtMax.setText(seq.getMaxValue() == null ? "" : seq.getMaxValue().toString());
			btnCache.setSelection(!seq.isNoCache());
			txtCache.setEnabled(!seq.isNoCache() && editable);
			txtCache.setSelection((int) seq.getCacheSize());
			btnCycle.setSelection(seq.isCycleFlag());
		}

		/**
		 * Set the edit-able status of class
		 * 
		 * @param editable true if read-only
		 */
		void setEditable(boolean editable) {
			this.editable = editable;
			txtName.setEditable(editable);
			specifyStartValue.setEnabled(editable);
			txtStart.setEditable(specifyStartValue.getSelection() && editable);
			txtIncrement.setEditable(editable);
			btnMin.setEnabled(editable);
			txtMin.setEditable(btnMin.getSelection() && editable);
			btnMax.setEnabled(editable);
			txtMax.setEditable(btnMax.getSelection() && editable);
			btnCache.setEnabled(editable);
			txtCache.setEnabled(btnCache.getSelection() && editable);
			btnCycle.setEnabled(editable);
		}

		/**
		 * Save UI to sequence including validation
		 * 
		 * @return VerifyResultMessages
		 */
		VerifyResultMessages save() {
			if (seq == null) {
				return new VerifyResultMessages();
			}
			final String newName = txtName.getText().trim().toLowerCase(Locale.US);
			if (!MigrationCfgUtils.verifyTargetDBObjName(newName)) {
				return new VerifyResultMessages(Messages.msgErrInvalidSerialName, null, null);
			}
			if (StringUtils.isBlank(txtStart.getText())) {
				txtStart.setFocus();
				return new VerifyResultMessages(Messages.msgErrEmptyStartValue, null, null);
			}
			if (StringUtils.isBlank(txtIncrement.getText())) {
				txtIncrement.setFocus();
				return new VerifyResultMessages(Messages.msgErrEmptyIncrement, null, null);
			}
			if (btnMin.getSelection() && StringUtils.isBlank(txtMin.getText())) {
				txtMin.setFocus();
				return new VerifyResultMessages(Messages.msgErrEmptyMin, null, null);
			}
			if (btnMax.getSelection() && StringUtils.isBlank(txtMax.getText())) {
				txtMax.setFocus();
				return new VerifyResultMessages(Messages.msgErrEmptyMax, null, null);
			}
			final BigInteger incrementBy = new BigInteger(txtIncrement.getText());
			final boolean asending;
			if (incrementBy.compareTo(BIGINTEGER0) == 0) {
				return new VerifyResultMessages(Messages.msgErr0Increment, null, null);
			} else {
				asending = incrementBy.compareTo(BIGINTEGER0) > 0;
			}
			final BigInteger current = new BigInteger(txtStart.getText());
			final BigInteger min;
			if (btnMin.getSelection()) {
				min = new BigInteger(txtMin.getText());
			} else {
				min = new BigInteger(asending ? txtStart.getText()
						: IntegerVerifyListener.DEFAULT_MIN_VALUE);
			}
			final BigInteger max;
			if (btnMax.getSelection()) {
				max = new BigInteger(txtMax.getText());
			} else {
				max = new BigInteger(IntegerVerifyListener.DEFAULT_MAX_VALUE);
			}

			if (min.compareTo(max) > 0) {
				return new VerifyResultMessages(Messages.msgErrMaxMin, null, null);
			}
			if (current.compareTo(max) > 0) {
				return new VerifyResultMessages(Messages.msgErrSmallMax, null, null);
			}
			if (current.compareTo(min) < 0) {
				return new VerifyResultMessages(Messages.msgErrBigMin, null, null);
			}
			if (incrementBy.abs().compareTo(max.subtract(min)) > 0) {
				return new VerifyResultMessages(Messages.msgErrBigIncrement, null, null);
			}
			if (!newName.equalsIgnoreCase(seq.getName())) {
				if (config.isTargetSerialNameInUse(newName)) {
					return new VerifyResultMessages(Messages.bind(
							Messages.errDuplicateSequenceName, newName), null, null);
				}
			}
			//Save target sequence
			seq.setName(newName);
			serialConfig.setAutoSynchronizeStartValue(!specifyStartValue.getSelection());
			seq.setCurrentValue(current);
			seq.setIncrementBy(incrementBy);
			seq.setNoMinValue(!btnMin.getSelection());
			if (!seq.isNoMinValue()) {
				seq.setMinValue(min);
			}
			seq.setNoMaxValue(!btnMax.getSelection());
			if (!seq.isNoMaxValue()) {
				seq.setMaxValue(max);
			}
			seq.setNoCache(!btnCache.getSelection());
			if (!seq.isNoCache()) {
				seq.setCacheSize(txtCache.getSelection());
			}
			seq.setCycleFlag(btnCycle.getSelection());
			return new VerifyResultMessages();
		}
	}
}
