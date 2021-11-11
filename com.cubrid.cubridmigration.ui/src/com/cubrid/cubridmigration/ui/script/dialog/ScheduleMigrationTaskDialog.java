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
package com.cubrid.cubridmigration.ui.script.dialog;

import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Scheduler;

import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.MigrationScript;
import com.cubrid.cubridmigration.ui.script.MigrationScriptManager;
import com.cubrid.cubridmigration.ui.script.MigrationScriptSchedulerManager;

/**
 * 
 * TaskReservationDialog
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-12-7
 */
public class ScheduleMigrationTaskDialog extends
		Dialog {

	private Composite grpOnce;
	private Composite grpRepeat;
	private Composite grpAdvance;
	private Button btnOnce;
	private Button btnRepeat;
	private Button btnAdvance;
	private Text txtOnce;
	private Text txtRepeat;
	private Text txtAdvance;

	private final MigrationScript script;

	/**
	 * Create the dialog
	 * 
	 * @param parentShell
	 * @param script
	 */
	public ScheduleMigrationTaskDialog(Shell parentShell, MigrationScript script) {
		super(parentShell);
		this.script = script;
	}

	/**
	 * Create contents of the dialog
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createDialogArea(Composite parent) {
		Composite area = (Composite) super.createDialogArea(parent);
		area.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		area.setLayout(new GridLayout());

		area = new Composite(area, SWT.BORDER);
		area.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		area.setLayout(new GridLayout());

		btnOnce = new Button(area, SWT.RADIO);
		btnOnce.setText(Messages.btnOneTimeOnly);
		btnOnce.setSelection(true);
		btnOnce.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent se) {
				updateControlsStatus();
			}

		});

		grpOnce = new Composite(area, SWT.BORDER);
		grpOnce.setLayout(new GridLayout(3, false));
		grpOnce.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

		Label lblTime = new Label(grpOnce, SWT.NONE);
		lblTime.setText(Messages.lblStartTime);
		txtOnce = new Text(grpOnce, SWT.BORDER);
		txtOnce.setTextLimit(11);
		txtOnce.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		Label lblEG = new Label(grpOnce, SWT.NONE);
		lblEG.setText(Messages.tipOneTime);

		btnRepeat = new Button(area, SWT.RADIO);
		btnRepeat.setText(Messages.btnRepeat);
		btnRepeat.setSelection(script.getCronMode() == 1);
		btnRepeat.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent se) {
				updateControlsStatus();
			}

		});
		grpRepeat = new Composite(area, SWT.BORDER);
		grpRepeat.setLayout(new GridLayout(3, false));
		grpRepeat.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

		Label lblTime2 = new Label(grpRepeat, SWT.NONE);
		lblTime2.setText(Messages.lblStartTime);
		txtRepeat = new Text(grpRepeat, SWT.BORDER);
		txtRepeat.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		txtRepeat.setTextLimit(5);

		lblEG = new Label(grpRepeat, SWT.NONE);
		lblEG.setText(Messages.tipRepeat);

		btnAdvance = new Button(area, SWT.RADIO);
		btnAdvance.setText(Messages.btnCronPatten);
		btnAdvance.setSelection(script.getCronMode() == 2);
		btnAdvance.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent se) {
				updateControlsStatus();
			}

		});

		grpAdvance = new Composite(area, SWT.BORDER);
		grpAdvance.setLayout(new GridLayout(2, false));
		grpAdvance.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

		Label lblTime3 = new Label(grpAdvance, SWT.NONE);
		lblTime3.setText(Messages.lblCronPatten);
		txtAdvance = new Text(grpAdvance, SWT.BORDER);
		txtAdvance.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		initInputs();
		updateControlsStatus();
		return area;
	}

	/**
	 * Initialize composites with input script.
	 * 
	 */
	private void initInputs() {
		txtOnce.setText(CUBRIDTimeUtil.formatDateTime(
				System.currentTimeMillis() + 3600000, "MM-dd HH:mm",
				TimeZone.getDefault()));
		txtRepeat.setText(CUBRIDTimeUtil.formatDateTime(
				System.currentTimeMillis() + 3600000, "HH:mm", TimeZone.getDefault()));
		txtAdvance.setText("10 6 * * *");
		btnOnce.setSelection(script.getCronMode() == 0);
		btnRepeat.setSelection(script.getCronMode() == 1);
		btnAdvance.setSelection(script.getCronMode() == 2);
		if (StringUtils.isBlank(script.getCronPatten())) {
			return;
		}
		if (btnOnce.getSelection()) {
			String[] split = script.getCronPatten().split(" ");
			txtOnce.setText(split[3] + "-" + split[2] + " " + split[1] + ":"
					+ split[0]);
		} else if (btnRepeat.getSelection()) {
			String[] split = script.getCronPatten().split(" ");
			txtRepeat.setText(split[1] + ":" + split[0]);
		} else if (btnAdvance.getSelection()) {
			txtAdvance.setText(script.getCronPatten());
		}

	}

	/**
	 * Update texts read-only status with button clicked.
	 * 
	 */
	private void updateControlsStatus() {
		txtOnce.setEnabled(btnOnce.getSelection());
		txtRepeat.setEnabled(btnRepeat.getSelection());
		txtAdvance.setEnabled(btnAdvance.getSelection());
	}

	/**
	 * Create contents of the button bar
	 * 
	 * @param parent Composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		createButton(parent, IDialogConstants.OK_ID, IDialogConstants.OK_LABEL,
				true);
		createButton(parent, IDialogConstants.CANCEL_ID,
				IDialogConstants.CANCEL_LABEL, false);

	}

	/**
	 * configureShell
	 * 
	 * @param newShell Shell
	 */
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setText(Messages.titleReservationSettings);
	}

	private final static int[] DAY_OF_MONTH = new int[]{31, 29, 31, 30, 31, 30,
			31, 31, 30, 31, 30, 31 };

	/**
	 * click OK button
	 * 
	 * @param buttonId int
	 */
	protected void buttonPressed(int buttonId) {

		if (buttonId == IDialogConstants.OK_ID) {
			if (btnOnce.getSelection()) {
				try {
					if (!txtOnce.getText().matches("\\d+-\\d+ \\d+:\\d+")) {
						throw new RuntimeException();
					}
					String[] strs = txtOnce.getText().trim().split(" ");
					String[] dateStr = strs[0].split("-");
					String[] timeStr = strs[1].split(":");
					int month = Integer.parseInt(dateStr[0]);
					int day = Integer.parseInt(dateStr[1]);
					int hour = Integer.parseInt(timeStr[0]);
					int min = Integer.parseInt(timeStr[1]);
					if (month < 1 || month > 12 || day < 1
							|| day > DAY_OF_MONTH[month - 1] || hour < 0
							|| hour > 23 || min < 0 || min > 59) {
						throw new RuntimeException();
					}
					script.setCronMode(0);
					script.setCronPatten(min + " " + hour + " " + day + " "
							+ month + " " + " *");
					MigrationScriptSchedulerManager.addReservation(script);
				} catch (Exception e) {
					MessageDialog.openError(getShell(), Messages.msgError,
							Messages.errMsgInvalidStartTime);
					txtOnce.setFocus();
					txtOnce.selectAll();
					return;
				}
			} else if (btnRepeat.getSelection()) {
				try {

					String dd = txtRepeat.getText().trim();
					if (!dd.matches("\\d+:\\d+")) {
						throw new RuntimeException();
					}
					String[] splid = dd.split(":");
					int hour = Integer.parseInt(splid[0]);
					int min = Integer.parseInt(splid[1]);
					if (hour < 0 || hour > 23 || min < 0 || min > 59) {
						throw new RuntimeException();
					}
					script.setCronMode(1);
					script.setCronPatten(splid[1] + " " + splid[0] + " * * *");
					MigrationScriptSchedulerManager.addReservation(script);
				} catch (Exception e) {
					MessageDialog.openError(getShell(), Messages.msgError,
							Messages.errMsgInvalidStartTime);
					txtRepeat.setFocus();
					txtRepeat.selectAll();
					return;
				}
			} else if (btnAdvance.getSelection()) {
				try {
					Scheduler scheduler = new Scheduler();
					String text = txtAdvance.getText().trim();
					scheduler.schedule(text, new Runnable() {

						public void run() {

						}
					});
					script.setCronMode(2);
					script.setCronPatten(text);
					MigrationScriptSchedulerManager.addReservation(script);
				} catch (InvalidPatternException ex) {
					MessageDialog.openError(getShell(), Messages.msgError,
							Messages.errMsgInvalidCronPatten);
					txtAdvance.setFocus();
					txtAdvance.selectAll();
					return;
				}
			}
			//MigrationScriptManager.getInstance().updateScriptCron(script);
			MigrationScriptManager.getInstance().save();
		}
		super.buttonPressed(buttonId);
	}

	/**
	 * The initial size of dialog
	 * 
	 * @return Point
	 */
	protected Point getInitialSize() {
		return new Point(400, 300);
	}

	/**
	 * Remove help button
	 * 
	 * @return false
	 */
	public boolean isHelpAvailable() {
		return false;
	}

}
