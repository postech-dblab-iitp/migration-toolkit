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

import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swt.widgets.Widget;

import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.listener.ISQLTableChangedListener;
import com.cubrid.cubridmigration.ui.common.navigator.node.ColumnNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ColumnsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.DatabaseNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SQLTableNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SQLTablesNode;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * Show source DB objects including DB objects and user defined SQLs.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-23 created by Kevin Cao
 */
public class SourceDBExploreView implements
		ISQLTableChangedListener {

	private final SQLTablesNode sqlFolder = new SQLTablesNode("sql_folder",
			Messages.labelTreeObjSql);
	private final TreeViewer treeView;

	private MigrationConfiguration config;

	private final List<ISelectionChangedListener> listeners = new ArrayList<ISelectionChangedListener>();

	public SourceDBExploreView(Composite parent, int style) {
		treeView = new TreeViewer(parent, style);
		treeView.setLabelProvider(new DBTreeNodeLabelProvider());
		treeView.setContentProvider(new DBTreeContentProvider());
		treeView.getTree().setLayout(new GridLayout());
		treeView.getTree().setLayoutData(
				new GridData(SWT.FILL, SWT.FILL, true, true));

	}

	/**
	 * Add the tree node or table node selection listener
	 * 
	 * @param listener ISelectionChangedListener
	 */
	public void addSelectionChangedListener(ISelectionChangedListener listener) {
		listeners.add(listener);
		treeView.addSelectionChangedListener(listener);
	}

	/**
	 * Build SQL table node in the tree viewer
	 * 
	 * @param sstc SourceSQLTableConfig
	 * @param stn SQLTableNode
	 */
	private void buildSQLTableNode(SourceSQLTableConfig sstc, SQLTableNode stn) {
		final Table srcSQLSchema = this.config.getSrcSQLSchema(sstc.getName());
		stn.setId(sqlFolder.getId() + "/" + sstc.getName());
		stn.setLabel(sstc.getName());
		stn.setTable(srcSQLSchema);
		stn.setSstc(sstc);
		stn.removeAllChild();

		final List<SourceColumnConfig> cols = sstc.getColumnConfigList();
		final ColumnsNode columnsNode = new ColumnsNode(stn.getId()
				+ "/columns", "Columns(" + cols.size() + ")");
		stn.addChild(columnsNode);
		if (srcSQLSchema == null) {
			return;
		}
		for (SourceColumnConfig scc : cols) {
			ColumnNode cn = new ColumnNode(columnsNode.getId() + "/"
					+ scc.getName(), scc.getName());
			cn.setColumn(srcSQLSchema.getColumnByName(scc.getName()));
			columnsNode.addChild(cn);
		}
	}

	/**
	 * Create tree viewer's input
	 * 
	 * @param input DatabaseNode root node
	 * @param config MigrationConfiguration
	 * @return List<ICubridNode>
	 */
	private List<ICUBRIDNode> createTreeViewInput(DatabaseNode input,
			MigrationConfiguration config) {
		this.config = config;
		List<ICUBRIDNode> tvContent = new ArrayList<ICUBRIDNode>();
		//Database/schema/tables|views|sequences/.....
		//If one one schema in the source database, don't show schema node 
		if (input.getChildren().size() == 1) {
			tvContent.addAll(input.getChildren().get(0).getChildren());
		} else {
			tvContent.addAll(input.getChildren());
		}

		//Build SQL table nodes
		sqlFolder.removeAllChild();
		for (SourceSQLTableConfig sstc : config.getExpSQLCfg()) {
			SQLTableNode stn = new SQLTableNode("sql_folder/" + sstc.getName(),
					sstc.getName());
			sqlFolder.addChild(stn);
			buildSQLTableNode(sstc, stn);
		}
		tvContent.add(sqlFolder);
		return tvContent;
	}

	/**
	 * 
	 * After add a SourceSQLTableConfig
	 * 
	 * @param sstc SourceSQLTableConfig added
	 */
	public void onAddSQL(SourceSQLTableConfig sstc) {
		SQLTableNode stn = new SQLTableNode(sqlFolder.getId() + "/"
				+ sstc.getName(), sstc.getName());
		sqlFolder.addChild(stn);
		buildSQLTableNode(sstc, stn);
		treeView.refresh(sqlFolder);
	}

	/**
	 * onEditSQL
	 * 
	 * @param sstc SourceSQLTableConfig
	 */
	public void onEditSQL(SourceSQLTableConfig sstc) {
		for (ICUBRIDNode cn : sqlFolder.getChildren()) {
			SQLTableNode stn = (SQLTableNode) cn;
			if (stn.getSstc() == sstc) {
				buildSQLTableNode(sstc, stn);
				treeView.collapseToLevel(stn, 2);
				treeView.refresh(stn);
				break;
			}
		}
	}

	/**
	 * On SQL removed
	 * 
	 * @param sstc SourceSQLTableConfig
	 */
	public void onRemoveSQL(SourceSQLTableConfig sstc) {
		List<ICUBRIDNode> nodes = new ArrayList<ICUBRIDNode>(
				sqlFolder.getChildren());
		for (ICUBRIDNode cn : nodes) {
			SQLTableNode stn = (SQLTableNode) cn;
			if (stn.getSstc() == sstc) {
				treeView.remove(stn);
				sqlFolder.removeChild(stn);
				break;
			}
		}
	}

	/**
	 * Refresh
	 * 
	 */
	public void refresh() {
		treeView.refresh();
	}

	/**
	 * Set focus
	 * 
	 */
	public void setFocus() {
		treeView.getTree().setFocus();
	}

	/**
	 * Set the source database information to be display.
	 * 
	 * @param input DatabaseNode
	 * @param config MigrationConfiguration
	 */
	public void setInput(DatabaseNode input, MigrationConfiguration config) {
		//this.config = config;
		if (input == null) {
			return;
		}
		treeView.setInput(createTreeViewInput(input, config));
		final Tree tree = treeView.getTree();
		tree.setSelection(tree.getItem(0));
		treeView.expandToLevel(1);
	}

	/**
	 * Set selection of the tree view or SQL table view.
	 * 
	 * @param model ICubridNode or SourceSQLConfig
	 */
	public void setSelection(Object model) {
		if (model instanceof ICUBRIDNode) {
			ICUBRIDNode nd = ((ICUBRIDNode) model).getParent();
			if (treeView.isExpandable(nd)) {
				treeView.setExpandedState(nd, true);
			}
			final Widget it = treeView.testFindItem(model);
			if (it == null) {
				return;
			}
			treeView.getTree().setSelection((TreeItem) it);
		}
	}
}
