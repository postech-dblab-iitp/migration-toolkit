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
package com.cubrid.cubridmigration.ui.wizard.dialog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.hadoop.HadoopStreamFactory;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * CUBRIDHadoopFileDialog Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-8-30 created by Kevin Cao
 */
public class CUBRIDHadoopFileDialog extends
		Dialog {

	private Text txtPath;
	private List<String> hdfsPath = new ArrayList<String>();

	public CUBRIDHadoopFileDialog(Shell parentShell) {
		super(parentShell);
	}

	/**
	 * @param parent of the composites
	 * @return dialog area.
	 */
	protected Control createDialogArea(Composite parent) {
		this.getShell().setText(Messages.titleAddCSVFromHDFS);

		Composite rootCom = new Composite(parent, SWT.NONE);
		rootCom.setLayout(new GridLayout());
		rootCom.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		Composite com = new Composite(rootCom, SWT.NONE);
		com.setLayout(new GridLayout(2, false));
		com.setLayoutData(new GridData(SWT.FILL, SWT.BOTTOM, true, false));
		Label lblPath = new Label(com, SWT.NONE);
		lblPath.setText(Messages.lblHDFSPath);

		txtPath = new Text(com, SWT.BORDER);
		txtPath.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
		txtPath.setText("hdfs://");
		txtPath.selectAll();
		return com;
	}

	/**
	 * OK button pressed
	 */
	protected void okPressed() {
		try {
			String hdfs = txtPath.getText().trim();
			if (StringUtils.isBlank(hdfs)) {
				return;
			}
			if (!hdfs.toLowerCase(Locale.ENGLISH).startsWith("hdfs://")) {
				hdfs = "hdfs://" + hdfs;
			}
			FileStatus fs = HadoopStreamFactory.getFileStatus(hdfs);
			if (fs == null || fs.isDir()) {
				return;
			}
			hdfsPath.add(fs.getPath().toString());
			super.okPressed();
		} catch (IOException e) {
			MessageDialog.openError(getShell(), Messages.msgError,
					e.getMessage());
		}
	}

	/**
	 * Hdfs Path
	 * 
	 * @return HDFS path
	 */
	public List<String> getHdfsPath() {
		return new ArrayList<String>(hdfsPath);
	}

	/**
	 * @return dialog size
	 */
	protected Point getInitialSize() {
		return new Point(480, 160);
	}

}
