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

import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;

import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.trans.MigrationTransFactory;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * page class of Oracle to CUBRID data type mapping
 * 
 * @author moulinwang
 * 
 */
public class SQLServer2CUBRIDTypeMapPage extends
		PreferencePage implements
		IWorkbenchPreferencePage {
	private DataTypeMappingComposite container;

	public SQLServer2CUBRIDTypeMapPage() {
		super(Messages.sqlServer2CUBRID, null);
	}

	/**
	 * createContents
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createContents(Composite parent) {
		Composite com = new Composite(parent, SWT.NONE);
		com.setLayout(new GridLayout());
		com.setLayoutData(new GridData(SWT.FILL));
		container = new DataTypeMappingComposite(com,
				MigrationTransFactory.getTransformHelper(DatabaseType.MSSQL,
						DatabaseType.CUBRID).getDataTypeMappingHelper());
		return com;
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
		if (container == null) {
			return true;
		}
		if (!checkValues()) {
			return false;
		}

		container.save();
		return true;
	}

	/**
	 * performDefaults
	 */
	protected void performDefaults() {
		if (container == null) {
			return;
		}
		if (!checkValues()) {
			return;
		}

		container.perfromDefaults();
	}

	/**
	 * check input value
	 * 
	 * @return boolean
	 */
	private boolean checkValues() {
		return true;
	}

}
