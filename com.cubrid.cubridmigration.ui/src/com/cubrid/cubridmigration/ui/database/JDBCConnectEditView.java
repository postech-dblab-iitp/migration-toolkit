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
package com.cubrid.cubridmigration.ui.database;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.common.CharsetUtils;
import com.cubrid.cubridmigration.core.common.ValidationUtils;
import com.cubrid.cubridmigration.core.connection.CMTConParamManager;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.connection.JDBCData;
import com.cubrid.cubridmigration.core.connection.JDBCDriverManager;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.ui.common.Status;
import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * JDBCConnectEditView
 * 
 * @author JessieHuang,Kevin Cao
 */
public class JDBCConnectEditView {

	private Combo cboCharset;
	private Combo cboDBTypes;
	private Combo cboDrivers;

	private DatabaseType[] databaseTypes;
	private String oldName;

	private Text txtConName;
	private Text txtDBName;
	private Text txtHostIp;
	private Spinner txtHostPort;
	private Text txtPassword;
	private Text txtUserName;

	private String userJDBCURL;

	/**
	 * Create the composite
	 * 
	 * @param dbTypes DatabaseType[]
	 */
	public JDBCConnectEditView(final DatabaseType[] dbTypes) {
		databaseTypes = Arrays.copyOf(dbTypes, dbTypes.length);
	}

	/**
	 * addDriver
	 */
	private void addDriver() {
		final FileDialog fileDialog = new FileDialog(Display.getDefault().getActiveShell(),
				SWT.SINGLE);
		fileDialog.setFilterPath(".");
		fileDialog.setFilterExtensions(new String[] {"*.jar", "*.zip", "*.*"});
		fileDialog.setFilterNames(new String[] {"*.jar", "*.zip", "*.*"});

		DatabaseType dt = getDBType();
		JDBCData jd = (JDBCData) cboDrivers.getData(cboDrivers.getText());
		if (jd != null) {
			fileDialog.setFilterPath(jd.getJdbcDriverPath());
		}
		final String firstFile = fileDialog.open();
		if (StringUtils.isBlank(firstFile)) {
			return;
		}
		if (JDBCDriverManager.getInstance().isDriverDuplicated(firstFile)) {
			UICommonTool.openErrorBox(Display.getDefault().getActiveShell(),
					Messages.msgDuplicatedJdbcDriverFile);
			return;
		}
		if (!JDBCDriverManager.getInstance().addDriver(firstFile, false)) {
			UICommonTool.openErrorBox(Display.getDefault().getActiveShell(),
					Messages.errInvalidJdbcJar);
			return;
		}
		jd = dt.getJDBCData(firstFile);
		//If not added into the combo
		final String desc = jd.getDesc();
		if (cboDrivers.getData(desc) == null) {
			addDriver2CBO(jd);
			JDBCConfigDataManager.saveJdbcData();
		}
		setDriverName(firstFile);
	}

	/**
	 * Add driver to CBO.
	 * 
	 * @param jd JDBCData
	 */
	private void addDriver2CBO(JDBCData jd) {
		final String desc = jd.getDesc();
		cboDrivers.add(desc);
		cboDrivers.setData(desc, jd);
	}

	/**
	 * Retrieves the status
	 * 
	 * @return IStatus
	 */
	public IStatus checkStatus() {
		boolean needBaseInfo = StringUtils.isBlank(userJDBCURL);
		if (cboDBTypes.getSelectionIndex() < 0 || StringUtils.isBlank(cboDBTypes.getText())) {
			return new Status(IStatus.ERROR, Messages.dBConnectCompositeErrDbSys);
		}
		if (StringUtils.isBlank(cboDrivers.getText())) {
			return new Status(IStatus.ERROR, Messages.dBConnectCompositeErrDriver);
		}
		if (StringUtils.isBlank(getConName())) {
			return new Status(IStatus.ERROR, Messages.dBConnectCompositeErrEmptyConnNm);
		}

		final CMTConParamManager cpm = CMTConParamManager.getInstance();
		if (StringUtils.isBlank(oldName)) {
			if (cpm.isNameUsed(getConName())) {
				return new Status(IStatus.ERROR, Messages.dBConnectCompositeErrDupConnNm);
			}
		} else {
			if (!oldName.equals(getConName()) && cpm.isNameUsed(getConName())) {
				return new Status(IStatus.ERROR, Messages.dBConnectCompositeErrDupConnNm);
			}
		}

		if (needBaseInfo) {
			String hostIp = getHostIp();
			if (StringUtils.isBlank(hostIp)) {
				return new Status(IStatus.ERROR, Messages.bind(Messages.dBConnectCompositeStatusIP,
						hostIp));
			}
			String port = getHostPort();
			if (StringUtils.isBlank(port)) {
				String errMess = Messages.bind(Messages.dBConnectCompositeStatusPort, port);
				return new Status(IStatus.ERROR, errMess);
			} else if (!ValidationUtils.isValidPort(port)) {
				String errMess = Messages.bind(Messages.dBConnectCompositeErrPort, port);
				return new Status(IStatus.ERROR, errMess);
			}
			String str = getDbName();
			if (StringUtils.isBlank(str)) {
				final String errMess = Messages.bind(Messages.dBConnectCompositeErrName,
						Messages.dBConnectCompositeLblDbName, str);
				return (new Status(IStatus.ERROR, errMess));
			}
		}

		String userName = getUserName();
		if (StringUtils.isBlank(userName)) {
			final String errMess = Messages.bind(Messages.dBConnectCompositeErrName,
					Messages.dBConnectCompositeLblUsername, userName);
			return new Status(IStatus.ERROR, errMess);
		}
		//Check if the connection is in the list.
		final ConnParameters newcp = getConnParameters();
		if (StringUtils.isBlank(oldName)) {
			if (cpm.isNameUsed(getConName())) {
				return new Status(IStatus.ERROR, Messages.dBConnectCompositeErrDupConnNm);
			}
			if (cpm.isConnectionExists(newcp)) {
				return new Status(IStatus.ERROR, Messages.dBConnectCompositeErrDupConnParam);
			}
		} else {
			if (!oldName.equals(getConName()) && cpm.isNameUsed(getConName())) {
				return new Status(IStatus.ERROR, Messages.dBConnectCompositeErrDupConnNm);
			}
			ConnParameters oldcp = cpm.getConnection(oldName);
			if (oldcp != null && !oldcp.isSameDB(newcp) && cpm.isConnectionExists(newcp)) {
				return new Status(IStatus.ERROR, Messages.dBConnectCompositeErrDupConnParam);
			}
		}

		if (isCUBRID() && (StringUtils.isBlank(cboCharset.getText()))) {
			return new Status(IStatus.ERROR, Messages.dBConnectCompositeStatusCharset);
		}
		return null;
	}

	/**
	 * Create children components and initialize them.
	 * 
	 * @param parent Composite
	 */
	public void createConstrols(final Composite parent) {
		final Composite container = new Composite(parent, SWT.BORDER);
		container.setLayout(new GridLayout());
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		final Composite composite = new Composite(container, SWT.NONE);
		composite.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false));
		composite.setLayout(new GridLayout(3, false));

		final Label databaseSystemLbl = new Label(composite, SWT.NONE);
		databaseSystemLbl.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		databaseSystemLbl.setText(Messages.dBConnectCompositeLblDbSys);

		cboDBTypes = new Combo(composite, SWT.READ_ONLY);
		final GridData gdDbSystemName = new GridData(SWT.FILL, SWT.CENTER, true, false);
		gdDbSystemName.widthHint = 222;
		cboDBTypes.setLayoutData(gdDbSystemName);

		new Label(composite, SWT.NONE);

		final Label jdbcDriverLabel = new Label(composite, SWT.NONE);
		jdbcDriverLabel.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		jdbcDriverLabel.setText(Messages.dBConnectCompositeLblDriver);

		cboDrivers = new Combo(composite, SWT.READ_ONLY);
		final GridData gdDriverName = new GridData(SWT.FILL, SWT.CENTER, true, false);
		gdDriverName.widthHint = 222;
		cboDrivers.setLayoutData(gdDriverName);

		Button btnBrowse = new Button(composite, SWT.NONE);
		btnBrowse.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				addDriver();
			}

		});
		btnBrowse.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		btnBrowse.setText(Messages.dBConnectCompositeBtnBrowse);
		btnBrowse.setEnabled(true);

		final Group group = new Group(container, SWT.NONE);
		group.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		final GridLayout gridLayout1 = new GridLayout();
		gridLayout1.marginLeft = 5;
		gridLayout1.numColumns = 4;
		group.setLayout(gridLayout1);

		final Label conNameLabel = new Label(group, SWT.NONE);
		conNameLabel.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		conNameLabel.setText(Messages.dBConnectCompositeLblConnNm);

		txtConName = new Text(group, SWT.BORDER);
		final GridData gd = new GridData(SWT.FILL, SWT.CENTER, true, false);
		gd.widthHint = 174;
		gd.horizontalSpan = 3;
		txtConName.setLayoutData(gd);
		txtConName.setTextLimit(256);

		final Label hostNameLabel = new Label(group, SWT.NONE);
		hostNameLabel.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		hostNameLabel.setText(Messages.dBConnectCompositeLblIP);

		txtHostIp = new Text(group, SWT.BORDER);
		final GridData gdHostIp = new GridData(SWT.FILL, SWT.CENTER, true, false);
		gdHostIp.widthHint = 174;
		txtHostIp.setLayoutData(gdHostIp);

		final Label portLabel = new Label(group, SWT.NONE);
		portLabel.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		portLabel.setText(Messages.dBConnectCompositeLblPort);

		txtHostPort = new Spinner(group, SWT.BORDER);
		txtHostPort.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtHostPort.setMinimum(0);
		txtHostPort.setMaximum(65535);

		final Label databaseNameLabel = new Label(group, SWT.NONE);
		databaseNameLabel.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		databaseNameLabel.setText(Messages.dBConnectCompositeLblDbName);

		txtDBName = new Text(group, SWT.BORDER);
		final GridData gdDbName = new GridData(SWT.FILL, SWT.CENTER, true, false);
		gdDbName.widthHint = 154;
		txtDBName.setLayoutData(gdDbName);

		final Label charsetLabel = new Label(group, SWT.NONE);
		charsetLabel.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		charsetLabel.setText(Messages.dBConnectCompositeLblCharset);

		cboCharset = new Combo(group, SWT.READ_ONLY);
		cboCharset.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));

		final Label usernameLabel = new Label(group, SWT.NONE);
		usernameLabel.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		usernameLabel.setText(Messages.dBConnectCompositeLblUsername);

		txtUserName = new Text(group, SWT.BORDER);
		final GridData gdUsername = new GridData(SWT.FILL, SWT.CENTER, true, false, 3, 1);
		gdUsername.widthHint = 283;
		txtUserName.setLayoutData(gdUsername);

		final Label passwordLabel = new Label(group, SWT.NONE);
		passwordLabel.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		passwordLabel.setText(Messages.dBConnectCompositeLblPassword);

		txtPassword = new Text(group, SWT.PASSWORD | SWT.BORDER);
		txtPassword.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 3, 1));

		Composite btnCom = new Composite(group, SWT.NONE);
		btnCom.setLayout(new GridLayout(2, false));
		btnCom.setLayoutData(new GridData(SWT.RIGHT, SWT.BOTTOM, false, false, 4, 1));

		Button btnAdvance = new Button(btnCom, SWT.NONE);
		btnAdvance.setText(Messages.btnAdvanced);
		btnAdvance.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		btnAdvance.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				ConnParameters cp = getConnParameters();
				JDBCPatternDialog dialog = new JDBCPatternDialog(
						Display.getDefault().getActiveShell(), cp);
				if (dialog.open() != IDialogConstants.OK_ID) {
					return;
				}
				userJDBCURL = cp.getUserJDBCURL();
			}
		});

		cboDBTypes.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(final SelectionEvent event) {
				fireDBSystemChanged();
			}

		});

		cboDrivers.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(final SelectionEvent event) {
				cboDrivers.setToolTipText(cboDrivers.getText());
			}
		});

		init();
	}

	/**
	 * Initialize the composites.
	 * 
	 */
	private void init() {
		for (DatabaseType dt : databaseTypes) {
			cboDBTypes.add(dt.getName());
		}
		cboDBTypes.select(0);

		String[] charsets = CharsetUtils.getCharsets();
		cboCharset.setItems(charsets);
		cboCharset.select(0);
	}

	/**
	 * fire DBSystem Changed, update driver combo and default port and encoding
	 */
	public void fireDBSystemChanged() {
		final DatabaseType dt = getDBType();
		List<JDBCData> driverList = dt.getJDBCDatas();
		txtHostPort.setSelection(Integer.parseInt(dt.getDefaultJDBCPort()));
		cboCharset.setEnabled(dt.isSupportJDBCEncoding());
		if (dt.isSupportJDBCEncoding()) {
			cboCharset.select(1);
		} else {
			cboCharset.select(0);
		}
		//If there is no driver.
		if (CollectionUtils.isEmpty(driverList)) {
			cboDrivers.setItems(new String[0]);
			return;
		}
		//Update driver names combo-box
		cboDrivers.setItems(new String[0]);
		for (JDBCData jd : driverList) {
			addDriver2CBO(jd);
		}
		cboDrivers.select(cboDrivers.getItemCount() - 1);
	}

	/**
	 * return Catalog object
	 * 
	 * @return Catalog
	 */
	public Catalog getCatalogWithProgress() {
		ConnParameters cp = getConnParameters();
		Catalog catalog = SchemaFetcherWithProgress.fetch(cp);
		return catalog;
	}

	/**
	 * 
	 * getCharsetText
	 * 
	 * @return charset
	 */
	private String getCharsetText() {
		return cboCharset.getText();
	}

	/**
	 * Retrieves the connection name
	 * 
	 * @return String
	 */
	private String getConName() {
		return txtConName.getText().trim();
	}

	/**
	 * return a new ConnParameters instance
	 * 
	 * @return ConnParameters
	 */
	public ConnParameters getConnParameters() {
		String name = getConName();
		String hostIp = getHostIp();
		String hostPortString = getHostPort();
		int port = Integer.parseInt(hostPortString);
		String dbName = getDbName();
		String charSet = getCharsetText();
		String user = getUserName();
		String pass = getPassword();
		String driverPath = getDriverName();
		//String schema = getSchemaName(); //Empty means all schemas.
		DatabaseType dt = getDBType();
		ConnParameters cp = ConnParameters.getConParam(name, hostIp, port, dbName, dt, charSet,
				user, pass, driverPath, "");
		cp.setUserJDBCURL(userJDBCURL);
		return cp;
	}

	private String getDbName() {
		return txtDBName.getText().trim();
	}

	/**
	 * Get current selected database type.
	 * 
	 * @return DatabaseType
	 */
	private DatabaseType getDBType() {
		if (cboDBTypes.getSelectionIndex() < 0) {
			throw new IllegalArgumentException("No database system selected.");
		}
		return databaseTypes[cboDBTypes.getSelectionIndex()];
	}

	/**
	 * get driver name
	 * 
	 * @return driver name
	 */
	private String getDriverName() {
		Object obj = cboDrivers.getData(cboDrivers.getText());
		if (obj == null) {
			return null;
		}
		JDBCData jd = (JDBCData) obj;
		return jd.getJdbcDriverPath();

	}

	private String getHostIp() {
		return txtHostIp.getText().trim();
	}

	private String getHostPort() {
		return txtHostPort.getText().trim();
	}

	private String getPassword() {
		return txtPassword.getText();
	}

	/**
	 * @return String
	 */
	private String getUserName() {
		return txtUserName.getText().trim();
	}

	/**
	 * setCharsetText
	 * 
	 * @param charset String
	 */
	private void setCharsetText(String charset) {
		cboCharset.setText(charset);
	}

	/**
	 * Is current database is CUBRID DB.
	 * 
	 * @return true if CUBRID
	 */
	private boolean isCUBRID() {
		return DatabaseType.CUBRID.equals(getDBType());
	}

	/**
	 * Set Connection Name
	 * 
	 * @param conName connection name
	 */
	private void setConName(String conName) {
		txtConName.setText(conName == null ? "" : conName);
	}

	/**
	 * set connection parameters to initial GUI
	 * 
	 * @param cp ConnParameters
	 */
	public void setConParameters(ConnParameters cp) {
		if (cp == null) {
			fireDBSystemChanged();
		} else {
			cboDBTypes.setText(cp.getDatabaseType().getName());
			fireDBSystemChanged();

			setDriverName(cp.getDriverFileName());
			oldName = cp.getConName(); //save old name for validation
			setConName(cp.getConName());

			setHostIp(cp.getHost());
			setHostPort(cp.getPort() + "");
			setDbName(cp.getDbName());
			setUsername(cp.getConUser());
			setPassword(cp.getConPassword());
			setCharsetText(cp.getCharset());

			userJDBCURL = cp.getUserJDBCURL();
		}
	}

	/**
	 * setDbName
	 * 
	 * @param dbName String
	 */
	private void setDbName(String dbName) {
		this.txtDBName.setText(dbName == null ? "" : dbName);
	}

	/**
	 * setDriverName
	 * 
	 * @param driverName String
	 */
	private void setDriverName(final String driverName) {
		final DatabaseType dt = getDBType();
		JDBCData jd = dt.getJDBCData(driverName);
		if (jd == null) {
			return;
		}
		String desc = jd.getDesc();
		cboDrivers.setText(desc);
	}

	/**
	 * setHostIp
	 * 
	 * @param hostIp String
	 */
	private void setHostIp(String hostIp) {
		this.txtHostIp.setText(hostIp == null ? "" : hostIp);
	}

	/**
	 * setHostPort
	 * 
	 * @param hostPort String
	 */
	private void setHostPort(String hostPort) {
		this.txtHostPort.setSelection(hostPort == null ? 0 : Integer.parseInt(hostPort));
	}

	/**
	 * setPassword
	 * 
	 * @param password String
	 */
	private void setPassword(String password) {
		this.txtPassword.setText(password == null ? "" : password);
	}

	/**
	 * setUsername
	 * 
	 * @param username String
	 */
	private void setUsername(String username) {
		this.txtUserName.setText(username == null ? "" : username);
	}

}
