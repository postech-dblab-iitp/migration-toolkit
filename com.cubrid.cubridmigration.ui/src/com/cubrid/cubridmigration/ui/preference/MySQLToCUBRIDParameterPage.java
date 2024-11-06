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
package com.cubrid.cubridmigration.ui.preference;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.tibero.trans.MigrationParameters;
import com.cubrid.cubridmigration.ui.common.Status;
import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * MySQLToCUBRID parameter preference page
 * 
 * @author moulinwang
 * @version 1.0 - 2010-12-14 created by moulinwang
 */
public class MySQLToCUBRIDParameterPage extends
		PreferencePage implements
		IWorkbenchPreferencePage {

	private static final Logger LOG = LogUtil.getLogger(MySQLToCUBRIDParameterPage.class);

	private Text txtOtherTime;
	private Text txtOtherTimestamp;
	private Text txtOtherChar;
	private Button btnTimeNull;
	private Button btnTime0;
	private Button btnOtherTime;
	private Button btnTimeStampNull;
	private Button btnTimeStampOther;
	private Button btnTimeStamp0;
	private Button btnChar0Space;
	private Button btnChar0Unchanged;
	private Button btnChar0Other;
	private Button btnDate0Null;
	private Button btnDate0First;
	private Button btnOtherDate;
	private Text txtOtherDate;

	/**
	 * get the replaced time
	 * 
	 * @return String
	 * @throws ParseException e
	 */
	private String getReplacedTime() throws ParseException {
		if (btnTimeNull.getSelection()) {
			return null;
		} else if (btnTime0.getSelection()) {
			return MigrationParameters.DEFAULT_UNPARSED_TIME_VALUE;
		} else {
			long timestamp = CUBRIDTimeUtil.parseTime2Long(txtOtherTime.getText(),
					TimeZone.getDefault());
			return CUBRIDTimeUtil.defaultFormatTime(new Time(timestamp));
		}
	}

	/**
	 * get the replaced date
	 * 
	 * @return String
	 * @throws ParseException e
	 */
	private String getReplacedDate() throws ParseException {
		if (btnDate0Null.getSelection()) {
			return null;
		} else if (btnDate0First.getSelection()) {
			return MigrationParameters.DEFAULT_UNPARSED_DATE_VALUE;
		} else {
			long time = CUBRIDTimeUtil.parseDate2Long(txtOtherDate.getText(), TimeZone.getDefault());
			return CUBRIDTimeUtil.defaultFormatDate(new Date(time));
		}
	}

	/**
	 * get the replaced timestamp
	 * 
	 * @return String
	 * @throws ParseException e
	 */
	private String getReplacedTimestamp() throws ParseException {
		if (btnTimeStampNull.getSelection()) {
			return null;
		} else if (btnTimeStamp0.getSelection()) {
			return MigrationParameters.DEFAULT_UNPARSED_TIMESTAMP_VALUE;
		} else {
			long timestamp = CUBRIDTimeUtil.parseDatetime2Long(txtOtherTimestamp.getText(),
					TimeZone.getDefault());
			return CUBRIDTimeUtil.defaultFormatMilin(new Timestamp(timestamp));
		}
	}

	/**
	 * get the replaced character
	 * 
	 * @return String
	 * @throws ParseException e
	 */
	private String getReplacedChar() throws ParseException {
		if (btnChar0Space.getSelection()) {
			return MigrationParameters.DEFAULT_REPLAXE_CHAR0_VALUE;
		} else if (btnChar0Unchanged.getSelection()) {
			return "'\u0000'";
		} else {
			String replacedChar = txtOtherChar.getText();
			if (replacedChar.startsWith("'") && replacedChar.endsWith("'")
					&& replacedChar.length() >= 2) {
				replacedChar = replacedChar.substring(1, replacedChar.length() - 1);
			}

			if (replacedChar.length() == 0) {
				throw new ParseException("Replaced character is not set", -1);
			} else {
				if (replacedChar.startsWith("0x") || replacedChar.startsWith("\\u")) {
					return "'" + (char) Integer.parseInt(replacedChar.substring(2), 16) + "'";
				} else {
					return "'" + replacedChar.charAt(0) + "'";
				}
			}
		}
	}

	public MySQLToCUBRIDParameterPage() {
		super("Migration Parameters", null);
	}

	/**
	 * init
	 * 
	 * @param workbench IWorkbench
	 */
	public void init(IWorkbench workbench) {
		//empty
	}

	/**
	 * performOk
	 * 
	 * @return boolean
	 */
	public boolean performOk() {

		updateDialogStatus(null);

		if (getErrorMessage() != null) {
			return false;
		}

		try {
			MigrationParameters.putMigrationParamter(MigrationParameters.UNPARSED_TIME,
					getReplacedTime());
			MigrationParameters.putMigrationParamter(MigrationParameters.UNPARSED_DATE,
					getReplacedDate());
			MigrationParameters.putMigrationParamter(MigrationParameters.UNPARSED_TIMESTAMP,
					getReplacedTimestamp());
			MigrationParameters.putMigrationParamter(MigrationParameters.REPLAXE_CHAR0,
					getReplacedChar());
			MigrationPreferenceUtils.save();
		} catch (ParseException e) {
			return false;
		}

		return true;
	}

	/**
	 * update dialog status
	 * 
	 * @param errorStatus IStatus
	 */
	protected void updateDialogStatus(IStatus errorStatus) {

		if (errorStatus != null) {
			firePageStatusChanged(errorStatus);
			return;
		}

		java.util.List<IStatus> statusList = new ArrayList<IStatus>();
		IStatus dialogStatus = new Status(IStatus.INFO, null);
		statusList.add(dialogStatus);

		statusList.add(checkTimeStatus());
		statusList.add(checkDateStatus());
		statusList.add(checkTimestampStatus());
		statusList.add(checkCharStatus());

		IStatus status = UICommonTool.getMostSevere(statusList);

		firePageStatusChanged(status);
	}

	/**
	 * fire page changed
	 * 
	 * @param status IStatus
	 */
	private void firePageStatusChanged(IStatus status) {
		if (status == null) {
			return;
		}
		if (status.getSeverity() == IStatus.INFO) {
			setErrorMessage(null);
			setMessage(status.getMessage());
		} else {
			setErrorMessage(status.getMessage());
		}
	}

	/**
	 * performDefaults
	 */
	protected void performDefaults() {
		MigrationParameters.restoreDefault();
		MigrationPreferenceUtils.save();
		refreshGUI();
		updateDialogStatus(null);
	}

	/**
	 * checkCharStatus
	 * 
	 * @return IStatus
	 */
	private IStatus checkCharStatus() {
		try {
			if (btnChar0Other.getSelection()) {
				getReplacedChar();
			}

			return null;
		} catch (Exception e) {
			return new Status(IStatus.ERROR, Messages.bind(Messages.errInvalidChar,
					txtOtherChar.getText()));
		}
	}

	/**
	 * checkTimestampStatus
	 * 
	 * @return IStatus
	 */
	private IStatus checkTimestampStatus() {
		try {
			if (btnTimeStampOther.getSelection()) {
				getReplacedTimestamp();
			}

			return null;
		} catch (Exception e) {
			return new Status(IStatus.ERROR, Messages.bind(Messages.errInvalidTimestamp,
					txtOtherTimestamp.getText()));
		}
	}

	/**
	 * checkTimeStatus
	 * 
	 * @return IStatus
	 */
	private IStatus checkTimeStatus() {
		try {
			if (btnOtherTime.getSelection()) {
				getReplacedTime();
			}

			return null;
		} catch (Exception e) {
			return new Status(IStatus.ERROR, Messages.bind(Messages.errInvalidTime,
					txtOtherTime.getText()));

		}
	}

	/**
	 * checkDateStatus
	 * 
	 * @return IStatus
	 */
	private IStatus checkDateStatus() {
		try {
			if (btnOtherDate.getSelection()) {
				getReplacedDate();
			}

			return null;
		} catch (Exception e) {
			return new Status(IStatus.ERROR, Messages.bind(Messages.errInvalidDate,
					txtOtherDate.getText()));
		}
	}

	/**
	 * createContents
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createContents(Composite parent) {
		Composite compsite = new Composite(parent, SWT.None);
		compsite.setLayout(new GridLayout(1, false));
		compsite.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true, 1, 1));

		int firstColumnWidth = 135;
		int secondColumnWidth = 173;

		Group grpUnparseTime = new Group(compsite, SWT.NONE);
		grpUnparseTime.setText(Messages.infoGrpUnparsedTime);
		grpUnparseTime.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
		grpUnparseTime.setLayout(new GridLayout(4, false));

		btnTimeNull = new Button(grpUnparseTime, SWT.RADIO);
		GridData gdBtnTimeNull = new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 1);

		gdBtnTimeNull.widthHint = firstColumnWidth;
		btnTimeNull.setLayoutData(gdBtnTimeNull);
		btnTimeNull.setText(Messages.infoGrpUnparsedTimeNull);

		btnTime0 = new Button(grpUnparseTime, SWT.RADIO);
		btnTime0.setSelection(true);
		GridData gdBtnTime0 = new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 1);

		gdBtnTime0.widthHint = secondColumnWidth;
		btnTime0.setLayoutData(gdBtnTime0);
		btnTime0.setText(Messages.infoGrpUnparsedTime0);

		btnOtherTime = new Button(grpUnparseTime, SWT.RADIO);
		SelectionAdapter timeChangedListener = new SelectionAdapter() {

			public void widgetDefaultSelected(SelectionEvent event) {
				selectionChanged();
			}

			public void widgetSelected(SelectionEvent event) {
				selectionChanged();
			}

			private void selectionChanged() {
				txtOtherTime.setEnabled(btnOtherTime.getSelection());
				updateDialogStatus(null);
			}
		};
		btnTimeNull.addSelectionListener(timeChangedListener);
		btnTime0.addSelectionListener(timeChangedListener);
		btnOtherTime.addSelectionListener(timeChangedListener);
		btnOtherTime.setText(Messages.infoGrpUnparsedTimeOther);

		txtOtherTime = new Text(grpUnparseTime, SWT.BORDER);
		txtOtherTime.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent event) {
				updateDialogStatus(checkTimeStatus());
			}
		});
		txtOtherTime.addFocusListener(new FocusAdapter() {
			public void focusLost(FocusEvent event) {
				if (checkTimeStatus() == null) {
					try {
						String timeValue = getReplacedTime();
						txtOtherTime.setText(timeValue);
					} catch (ParseException ignored) { // NOPMD - this surely will never happen
						LOG.error("", ignored);
					}
				}
			}
		});
		txtOtherTime.setEnabled(false);
		GridData gdTxtOtherTime = new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1);
		gdTxtOtherTime.widthHint = secondColumnWidth;
		txtOtherTime.setLayoutData(gdTxtOtherTime);

		//

		new Label(compsite, SWT.NONE);

		Group grpDate0Convert = new Group(compsite, SWT.NONE);
		grpDate0Convert.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));

		grpDate0Convert.setText(Messages.infoGrpUnparsedDate);
		grpDate0Convert.setLayout(new GridLayout(4, false));

		btnDate0Null = new Button(grpDate0Convert, SWT.RADIO);
		btnDate0Null.setSelection(true);
		GridData gdBtnDate0Space = new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 1);
		gdBtnDate0Space.widthHint = firstColumnWidth;
		btnDate0Null.setLayoutData(gdBtnDate0Space);
		btnDate0Null.setText(Messages.infoGrpUnparsedDateNull);

		btnDate0First = new Button(grpDate0Convert, SWT.RADIO);
		GridData gdBtnDate0Unchanged = new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 1);
		gdBtnDate0Unchanged.widthHint = secondColumnWidth;
		btnDate0First.setLayoutData(gdBtnDate0Unchanged);
		btnDate0First.setText(Messages.infoGrpUnparsedDate0);

		btnOtherDate = new Button(grpDate0Convert, SWT.RADIO);
		btnOtherDate.setText(Messages.infoGrpUnparsedDateOther);

		SelectionAdapter dateChangedListener = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent event) {
				selectionChanged();
			}

			public void widgetSelected(SelectionEvent event) {
				selectionChanged();
			}

			private void selectionChanged() {
				txtOtherDate.setEnabled(btnOtherDate.getSelection());
				updateDialogStatus(null);
			}
		};
		btnDate0First.addSelectionListener(dateChangedListener);
		btnDate0Null.addSelectionListener(dateChangedListener);
		btnOtherDate.addSelectionListener(dateChangedListener);

		txtOtherDate = new Text(grpDate0Convert, SWT.BORDER);
		txtOtherDate.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent event) {
				updateDialogStatus(checkDateStatus());
			}
		});
		txtOtherDate.addFocusListener(new FocusAdapter() {
			public void focusLost(FocusEvent event) {
				if (checkCharStatus() == null) {
					try {
						String dateValue = getReplacedDate();
						txtOtherDate.setText(dateValue);
					} catch (ParseException ignored) { // NOPMD - this surely will never happen
						LOG.error("", ignored);
					}
				}
			}
		});
		txtOtherDate.setEnabled(false);
		txtOtherDate.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
		//

		new Label(compsite, SWT.NONE);

		Group grpUnparseTimeStamp = new Group(compsite, SWT.NONE);
		grpUnparseTimeStamp.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
		grpUnparseTimeStamp.setText(Messages.infoGrpUnparsedTimestamp);
		grpUnparseTimeStamp.setLayout(new GridLayout(4, false));

		btnTimeStampNull = new Button(grpUnparseTimeStamp, SWT.RADIO);
		GridData gdBtnTimeStampNull = new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 1);
		gdBtnTimeStampNull.widthHint = firstColumnWidth;
		btnTimeStampNull.setLayoutData(gdBtnTimeStampNull);
		btnTimeStampNull.setText(Messages.infoGrpUnparsedTimestampNull);
		btnTimeStamp0 = new Button(grpUnparseTimeStamp, SWT.RADIO);
		btnTimeStamp0.setSelection(true);
		GridData gdBtnTimeStamp0 = new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 1);
		gdBtnTimeStamp0.widthHint = secondColumnWidth;
		btnTimeStamp0.setLayoutData(gdBtnTimeStamp0);
		btnTimeStamp0.setText(Messages.infoGrpUnparsedTimestamp0);

		btnTimeStampOther = new Button(grpUnparseTimeStamp, SWT.RADIO);
		btnTimeStampOther.setText(Messages.infoGrpUnparsedTimestampOther);

		SelectionAdapter timestampChangedListener = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent event) {
				selectionChanged();
			}

			public void widgetSelected(SelectionEvent event) {
				selectionChanged();
			}

			private void selectionChanged() {
				txtOtherTimestamp.setEnabled(btnTimeStampOther.getSelection());
				updateDialogStatus(null);
			}
		};
		btnTimeStampNull.addSelectionListener(timestampChangedListener);
		btnTimeStamp0.addSelectionListener(timestampChangedListener);
		btnTimeStampOther.addSelectionListener(timestampChangedListener);

		txtOtherTimestamp = new Text(grpUnparseTimeStamp, SWT.BORDER);
		txtOtherTimestamp.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent event) {
				updateDialogStatus(checkTimestampStatus());
			}
		});
		txtOtherTimestamp.addFocusListener(new FocusAdapter() {
			public void focusLost(FocusEvent event) {
				if (checkTimestampStatus() == null) {
					try {
						String timestampValue = getReplacedTimestamp();
						txtOtherTimestamp.setText(timestampValue);
					} catch (ParseException ignored) { // NOPMD - this surely will never happen
						LOG.error("", ignored);
					}
				}
			}
		});
		txtOtherTimestamp.setEnabled(false);
		txtOtherTimestamp.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));

		new Label(compsite, SWT.NONE);

		Group grpChar0Convert = new Group(compsite, SWT.NONE);
		grpChar0Convert.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));

		grpChar0Convert.setText(Messages.infoGrpUnparsedChar);
		grpChar0Convert.setLayout(new GridLayout(4, false));

		btnChar0Space = new Button(grpChar0Convert, SWT.RADIO);
		btnChar0Space.setSelection(true);
		GridData gdBtnChar0Space = new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 1);
		gdBtnChar0Space.widthHint = firstColumnWidth;
		btnChar0Space.setLayoutData(gdBtnChar0Space);
		btnChar0Space.setText(Messages.infoGrpUnparsedCharSpace);

		btnChar0Unchanged = new Button(grpChar0Convert, SWT.RADIO);
		GridData gdBtnChar0Unchanged = new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 1);
		gdBtnChar0Unchanged.widthHint = secondColumnWidth;
		btnChar0Unchanged.setLayoutData(gdBtnChar0Unchanged);
		btnChar0Unchanged.setText(Messages.infoGrpUnparsedChar0);

		btnChar0Other = new Button(grpChar0Convert, SWT.RADIO);
		btnChar0Other.setText(Messages.infoGrpUnparsedCharOther);

		SelectionAdapter charChangedListener = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent event) {
				selectionChanged();
			}

			public void widgetSelected(SelectionEvent event) {
				selectionChanged();
			}

			private void selectionChanged() {
				txtOtherChar.setEnabled(btnChar0Other.getSelection());
				updateDialogStatus(null);
			}
		};
		btnChar0Space.addSelectionListener(charChangedListener);
		btnChar0Unchanged.addSelectionListener(charChangedListener);
		btnChar0Other.addSelectionListener(charChangedListener);

		txtOtherChar = new Text(grpChar0Convert, SWT.BORDER);
		txtOtherChar.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent event) {
				updateDialogStatus(checkCharStatus());
			}
		});
		txtOtherChar.addFocusListener(new FocusAdapter() {
			public void focusLost(FocusEvent event) {
				if (checkCharStatus() == null) {
					try {
						String charValue = getReplacedChar();
						txtOtherChar.setText(charValue);
					} catch (ParseException ignored) { // NOPMD - this surely will never happen
						LOG.error("", ignored);
					}
				}
			}
		});
		txtOtherChar.setEnabled(false);
		txtOtherChar.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));

		refreshGUI();

		return compsite;
	}

	/**
	 * refreshGUI
	 * 
	 */
	private void refreshGUI() {
		String timeValue = MigrationParameters.getMigrationParamter(MigrationParameters.UNPARSED_TIME);
		btnTimeNull.setSelection(false);
		btnTime0.setSelection(false);
		btnOtherTime.setSelection(false);

		if (timeValue == null) {
			btnTimeNull.setSelection(true);
		} else if (timeValue.equals(MigrationParameters.DEFAULT_UNPARSED_TIME_VALUE)) {
			btnTime0.setSelection(true);
		} else {
			btnOtherTime.setSelection(true);
			txtOtherTime.setText(timeValue);
		}

		String dateValue = MigrationParameters.getMigrationParamter(MigrationParameters.UNPARSED_DATE);
		btnDate0Null.setSelection(false);
		btnDate0First.setSelection(false);
		btnOtherDate.setSelection(false);

		if (dateValue == null) {
			btnDate0Null.setSelection(true);
		} else if (dateValue.equals(MigrationParameters.DEFAULT_UNPARSED_DATE_VALUE)) {
			btnDate0First.setSelection(true);
		} else {
			btnOtherDate.setSelection(true);
			txtOtherDate.setText(dateValue);
		}

		String timestampValue = MigrationParameters.getMigrationParamter(MigrationParameters.UNPARSED_TIMESTAMP);
		btnTimeStampNull.setSelection(false);
		btnTimeStamp0.setSelection(false);
		btnTimeStampOther.setSelection(false);

		if (timestampValue == null) {
			btnTimeStampNull.setSelection(true);
		} else if (timestampValue.equals(MigrationParameters.DEFAULT_UNPARSED_TIMESTAMP_VALUE)) {
			btnTimeStamp0.setSelection(true);
		} else {
			btnTimeStampOther.setSelection(true);
			txtOtherTimestamp.setText(timestampValue);
		}

		String charValue = MigrationParameters.getMigrationParamter(MigrationParameters.REPLAXE_CHAR0);
		btnChar0Space.setSelection(false);
		btnChar0Unchanged.setSelection(false);
		btnChar0Other.setSelection(false);

		if (charValue.equals(MigrationParameters.DEFAULT_REPLAXE_CHAR0_VALUE)) {
			btnChar0Space.setSelection(true);
		} else if ("'\u0000'".equals(charValue)) {
			btnChar0Unchanged.setSelection(true);
		} else {
			btnChar0Other.setSelection(true);
			txtOtherChar.setText(charValue);
		}

		txtOtherTime.setEnabled(btnOtherTime.getSelection());
		txtOtherDate.setEnabled(btnOtherDate.getSelection());
		txtOtherTimestamp.setEnabled(btnTimeStampOther.getSelection());
		txtOtherChar.setEnabled(btnChar0Other.getSelection());
	}

	/**
	 * Create controls
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		super.createControl(parent);
		Button btnDB = this.getDefaultsButton();
		btnDB.setText(Messages.btnDefault);
		Button btnAB = this.getApplyButton();
		btnAB.setText(Messages.btnApply);
	}
}
