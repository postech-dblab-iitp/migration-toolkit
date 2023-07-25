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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.preferences.ConfigurationScope;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.osgi.service.prefs.BackingStoreException;

import com.cubrid.common.ui.swt.Resources;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * SelectSrcTarTypesCom is view of select source type and destination type
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-4-15 created by Kevin Cao
 */
public class GraphSelectSrcTarTypesView {
	private static final Logger LOG = LogUtil.getLogger(GraphSelectSrcTarTypesView.class);

	private static final String TARGET_TYPE_KEY = "target_type";
	private static final String SOURCE_TYPE_KEY = "source_type";

	private Button btnOnlineTar;
	private Button btnOnlineGraph;
	//private Button btnOfflineTar;
	private Button btnDumpTar;
	//private Button btnSQLTar;

	private Button btnCSVTar;
	//private Button btnXLSTar;

	private Button btnOnlineCUBRIDSrc;
	private Button btnOnlineGraphSrc;
	//private Button btnOnlineOracleSrc;
	//private Button btnOnlineMYSQLSrc;
	//private Button btnOnlineMSSQLSrc;

	//private Button btnDumpSrc;
	//private Button btnSQLSrc;
	//private Button btnCSVSrc;

	private final List<Button> srcButtons = new ArrayList<Button>(4);

	private final List<Button> tarButtons = new ArrayList<Button>(6);

	public GraphSelectSrcTarTypesView(Composite parent) {
		Composite sectionClient = new Composite(parent, SWT.NONE);
		sectionClient.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		sectionClient.setLayout(new GridLayout(2, true));

		Group grpSrc = new Group(sectionClient, SWT.NONE);
		grpSrc.setLayout(new GridLayout());
		grpSrc.setLayoutData(new GridData(SWT.LEFT, SWT.FILL, false, true));
		grpSrc.setText(Messages.msgSrcType);

		btnOnlineCUBRIDSrc = createSrcTarTypeBtn(grpSrc, Messages.btnSrcOnlineCUBRIDDB,
				Messages.btnSrcOnlineCUBRIDDBDes);
		btnOnlineCUBRIDSrc.setData(MigrationConfiguration.SOURCE_TYPE_CUBRID);
		srcButtons.add(btnOnlineCUBRIDSrc);
		
		btnOnlineCUBRIDSrc.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent e){
				Button sourceBtn = (Button) e.getSource();
				if (sourceBtn.getSelection()) {
					selectCUBRIDSrc();
				}
			}
		});
		
		btnOnlineGraphSrc = createSrcTarTypeBtn(grpSrc, Messages.btnSrcOnlineGraphDB,
				Messages.btnSrcOnlineGraphDBDes);
		btnOnlineGraphSrc.setData(MigrationConfiguration.SOURCE_TYPE_GRAPH);
		
		btnOnlineGraphSrc.addSelectionListener(new SelectionListener() {
			@Override
			public void widgetSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				Button sourceBtn = (Button) e.getSource();
				if (sourceBtn.getSelection()) {
					selectGraphSrc();
				}
			}
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		srcButtons.add(btnOnlineGraphSrc);

//		btnOnlineMYSQLSrc = createSrcTarTypeBtn(grpSrc, Messages.btnSrcOnlineMySQLDB,
//				Messages.btnSrcOnlineMySQLDBDes);
//		btnOnlineMYSQLSrc.setData(MigrationConfiguration.SOURCE_TYPE_MYSQL);
//		srcButtons.add(btnOnlineMYSQLSrc);
//
//		btnOnlineOracleSrc = createSrcTarTypeBtn(grpSrc, Messages.btnSrcOnlineOracleDB,
//				Messages.btnSrcOnlineOracleDBDes);
//		btnOnlineOracleSrc.setData(MigrationConfiguration.SOURCE_TYPE_ORACLE);
//		srcButtons.add(btnOnlineOracleSrc);
//
//		btnOnlineMSSQLSrc = createSrcTarTypeBtn(grpSrc, Messages.btnSrcOnlineMSSQLDB,
//				Messages.btnSrcOnlineMSSQLDBDes);
//		btnOnlineMSSQLSrc.setData(MigrationConfiguration.SOURCE_TYPE_MSSQL);
//		srcButtons.add(btnOnlineMSSQLSrc);
		Label comSep = new Label(grpSrc, SWT.SEPARATOR | SWT.HORIZONTAL);
		{
			GridData gd = new GridData(SWT.FILL, SWT.NONE, true, false);
			gd.verticalIndent = 8;
			comSep.setLayoutData(gd);
		}

//		btnDumpSrc = createSrcTarTypeBtn(grpSrc, Messages.btnSrcMySQLDumpDB,
//				Messages.btnSrcMySQLDumpDBDes);
//		btnDumpSrc.setData(MigrationConfiguration.SOURCE_TYPE_XML_1);
//		srcButtons.add(btnDumpSrc);
//
//		btnSQLSrc = createSrcTarTypeBtn(grpSrc, Messages.btnSrcSQLFiles, Messages.btnSrcSQLFilesDes);
//		btnSQLSrc.setData(MigrationConfiguration.SOURCE_TYPE_SQL);
//		srcButtons.add(btnSQLSrc);
//
//		btnCSVSrc = createSrcTarTypeBtn(grpSrc, Messages.btnSrcCSVFiles, Messages.btnSrcCSVFilesDes);
//		btnCSVSrc.setData(MigrationConfiguration.SOURCE_TYPE_CSV);
//		srcButtons.add(btnCSVSrc);

		Group grpTar = new Group(sectionClient, SWT.NONE);
		grpTar.setLayout(new GridLayout());
		grpTar.setLayoutData(new GridData(SWT.LEFT, SWT.FILL, false, true));
		grpTar.setText(Messages.msgDestType);

		btnOnlineTar = createSrcTarTypeBtn(grpTar, Messages.btnDestOnlineCUBRIDDB,
				Messages.btnDestOnlineCUBRIDDBDes);
		btnOnlineTar.setData(MigrationConfiguration.DEST_ONLINE);
		tarButtons.add(btnOnlineTar);
		
		//GDB online neo4j target connect button
		btnOnlineGraph = createSrcTarTypeBtn(grpTar, Messages.btnDestOnlineGraph, Messages.btnDestOnlineGraphes);
		btnOnlineGraph.setData(MigrationConfiguration.DEST_GRAPH);
		tarButtons.add(btnOnlineGraph);
		
		

		//		btnOfflineTar = createSrcTarTypeBtn(grpTar,
		//				Messages.btnDestOfflineCUBRIDDB,
		//				Messages.btnDestOfflineCUBRIDDBDes);
		//		btnOfflineTar.setData(MigrationConfiguration.DEST_OFFLINE);
		//		tarButtons.add(btnOfflineTar);
		comSep = new Label(grpTar, SWT.SEPARATOR | SWT.HORIZONTAL);
		comSep.setLayoutData(new GridData(SWT.FILL, SWT.NONE, true, false));
		{
			GridData gd = new GridData(SWT.FILL, SWT.NONE, true, false);
			gd.verticalIndent = 8;
			comSep.setLayoutData(gd);
		}

		btnDumpTar = createSrcTarTypeBtn(grpTar, Messages.btnDestGraphDump,
				Messages.btnDestGraphDumpDes);
		btnDumpTar.setData(MigrationConfiguration.DEST_DB_UNLOAD);
		tarButtons.add(btnDumpTar);
		
//		btnSQLTar = createSrcTarTypeBtn(grpTar, Messages.btnDestSQLFiles,
//				Messages.btnDestSQLFilesDes);
//		btnSQLTar.setData(MigrationConfiguration.DEST_SQL);
//		tarButtons.add(btnSQLTar);
//
		btnCSVTar = createSrcTarTypeBtn(grpTar, Messages.btnDestCSVFiles,
				Messages.btnDestCSVFilesDes);
		btnCSVTar.setData(MigrationConfiguration.DEST_CSV);
		tarButtons.add(btnCSVTar);
//
//		btnXLSTar = createSrcTarTypeBtn(grpTar, Messages.btnDestXLSFiles,
//				Messages.btnDestXLSFilesDes);
//		btnXLSTar.setData(MigrationConfiguration.DEST_XLS);
//		tarButtons.add(btnXLSTar);

		readDefaultTypes();
	}

	/**
	 * Create button and description.
	 * 
	 * @param parent Composite
	 * @param name String
	 * @param des String
	 * @return button
	 */
	private Button createSrcTarTypeBtn(Composite parent, String name, String des) {
		Button result = new Button(parent, SWT.RADIO);
		result.setText(name);
		{
			GridData gd = new GridData(SWT.LEFT, SWT.TOP, false, false);
			gd.verticalIndent = 8;
			result.setLayoutData(gd);
		}
		Text txt = new Text(parent, SWT.MULTI | SWT.WRAP | SWT.READ_ONLY);
		{
			GridData gd = new GridData(SWT.LEFT, SWT.TOP, false, false);
			gd.horizontalIndent = 15;
			gd.widthHint = 370;
			txt.setLayoutData(gd);
		}
		txt.setBackground(parent.getBackground());
		txt.setForeground(Resources.getInstance().getColor(SWT.COLOR_DARK_GRAY));
		txt.setText(des);
		return result;
	}

	/**
	 * Retrieves the source type
	 * 
	 * @return integer
	 */
	public int getSourceType() {
		for (Button btn : srcButtons) {
			if (btn.getSelection()) {
				return (Integer) btn.getData();
			}
		}
		return MigrationConfiguration.SOURCE_TYPE_CUBRID;
	}

	/**
	 * Retrieves the target type
	 * 
	 * @return integer
	 */
	public int getTargetType() {
		for (Button btn : tarButtons) {
			if (btn.getSelection()) {
				return (Integer) btn.getData();
			}
		}
		return MigrationConfiguration.DEST_GRAPH;
	}

	/**
	 * Set default types selection
	 * 
	 */
	private void readDefaultTypes() {
		IEclipsePreferences preferences = new ConfigurationScope().getNode(MigrationUIPlugin.PLUGIN_ID);
		String srcType = preferences.get(SOURCE_TYPE_KEY,
				String.valueOf(MigrationConfiguration.SOURCE_TYPE_CUBRID));
		String tarType = preferences.get(TARGET_TYPE_KEY,
				String.valueOf(MigrationConfiguration.DEST_GRAPH));
		try {
			showCfg(Integer.valueOf(srcType), Integer.valueOf(tarType));
		} catch (Exception ex) {
			showCfg(MigrationConfiguration.SOURCE_TYPE_CUBRID, MigrationConfiguration.DEST_GRAPH);
		}
	}

	/**
	 * Save UI to configuration
	 * 
	 * @return error message
	 */
	public String save() {
//		if (btnSQLSrc.getSelection() && !btnOnlineTar.getSelection()) {
//			return Messages.errMsgErrorDestinationType1;
//		}
//		if (btnCSVSrc.getSelection() && !btnOnlineTar.getSelection()) {
//			return Messages.errMsgErrorDestinationType2;
//		}
		save2Default();
		return "";
	}

	/**
	 * Save configuration to default.
	 * 
	 */
	private void save2Default() {
		try {
			IEclipsePreferences preferences = new ConfigurationScope().getNode(MigrationUIPlugin.PLUGIN_ID);
			preferences.put(SOURCE_TYPE_KEY, String.valueOf(getSourceType()));
			
			preferences.put(TARGET_TYPE_KEY, String.valueOf(getTargetType()));
			
			preferences.flush();
		} catch (BackingStoreException e) {
			LOG.error("", e);
		}
	}
	
	private void selectCUBRIDSrc() {
		btnOnlineGraph.setEnabled(true);
		btnDumpTar.setEnabled(true);
	}
	
	private void selectGraphSrc() {
		btnOnlineGraph.setEnabled(false);
		btnOnlineGraph.setSelection(false);
		
		btnOnlineTar.setEnabled(true);
		btnOnlineTar.setSelection(true);
		
		btnDumpTar.setEnabled(false);
		btnDumpTar.setSelection(false);
		
	}

	/**
	 * Show the configuration's source type and target type
	 * 
	 * @param srcType type of source
	 * @param tarType type of target
	 */
	public void showCfg(int srcType, int tarType) {
		boolean flag = false;
		for (Button btn : srcButtons) {
			btn.setSelection(false);
			if (((Integer) btn.getData()).intValue() == srcType) {
				btn.setSelection(true);
				flag = true;
				
				if (srcType == MigrationConfiguration.SOURCE_TYPE_GRAPH) {
					selectGraphSrc();
				}
			}
		}
		if (!flag) {
			btnOnlineCUBRIDSrc.setSelection(true);
			selectGraphSrc();
		}
		flag = false;
		for (Button btn : tarButtons) {
			btn.setSelection(false);
			if (((Integer) btn.getData()).intValue() == tarType) {
				btn.setSelection(true);
				flag = true;
			}
		}

		if (!flag) {
			//btnOnlineTar.setSelection(true);
			btnOnlineGraph.setSelection(true);
		}
	}
}
