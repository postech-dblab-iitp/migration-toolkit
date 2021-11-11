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
package com.cubrid.cubridmigration.ui.script;

import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.action.IMenuListener;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.ui.part.ViewPart;

import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.controller.MigrationScriptExplorerController;

/**
 * 
 * MigrationScriptExplorerView is a explorer to manage migration scripts.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-6-26
 */
public class MigrationScriptExplorerView extends
		ViewPart implements
		MigrationScriptManagerListener {

	private static final String MENU_ADDITIONS = "additions";

	public static final String ID = MigrationScriptExplorerView.class.getName(); //$NON-NLS-1$

	private TreeViewer treeViewer = null;

	private final MigrationScriptExplorerController controller = new MigrationScriptExplorerController();

	private MigrationScriptManager scriptManager = MigrationScriptManager.getInstance();

	/**
	 * Add drop target
	 * 
	 * @param tv TreeViewer
	 */
	protected void addNavTreeDnDSupport(final TreeViewer tv) {
		// DropTarget for tree
		Transfer[] types = new Transfer[] {TextTransfer.getInstance()};
		int operations = DND.DROP_MOVE; //| DND.DROP_COPY;
		final Tree tree = tv.getTree();

		// DragSource
		final DragSource source = new DragSource(tree, operations);
		source.setTransfer(types);
		source.addDragListener(new CubridNodeDragSourceListener(tree));

		DropTarget target = new DropTarget(tree, operations);
		target.setTransfer(types);
		target.addDropListener(new CubridNodeDropTargetListener(tree) {
			/**
			 * Move nodes
			 * 
			 * @param targetNode target dropped node
			 * @param isBefore isBefore
			 */
			protected void moveNodes(ICUBRIDNode targetNode, boolean isBefore) {
				controller.moveNodes((IStructuredSelection) treeViewer.getSelection(), targetNode,
						isBefore);
				refreshTreeInput();
			}
		});
	}

	/**
	 * Create part controls.
	 * 
	 * @param parent composite
	 */
	public void createPartControl(Composite parent) {
		treeViewer = new TreeViewer(parent, SWT.VIRTUAL | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
		treeViewer.getTree().setDragDetect(true);
		treeViewer.setContentProvider(new MigrationScriptContentProvider());
		treeViewer.setLabelProvider(new MigrationScriptTreeLabelProvider());
		treeViewer.getTree().addMouseListener(new ScriptNodeDoubleClickListener());
		treeViewer.getTree().addKeyListener(new ScriptNodeDeleteKeyListener());

		addNavTreeDnDSupport(treeViewer);
		scriptManager.addListener(this);
		createPopupMenus();
		createViewMenus();
		//For RCP platform controls the tool buttons' enable/disable status
		getSite().setSelectionProvider(treeViewer);
		refreshTreeInput();
	}

	/**
	 * Create the tree view's pop menus.
	 * 
	 */
	private void createPopupMenus() {
		final MenuManager menuManager = new MenuManager();
		IMenuListener listener = new ForceUpdateMenuListener();
		menuManager.addMenuListener(listener);
		Separator item = new Separator(MENU_ADDITIONS);
		item.setId(MENU_ADDITIONS);
		menuManager.add(item);
		Menu contextMenu = menuManager.createContextMenu(treeViewer.getControl());
		treeViewer.getControl().setMenu(contextMenu);
		getSite().registerContextMenu(menuManager, treeViewer);
	}

	/**
	 * Create view part menus.
	 * 
	 */
	private void createViewMenus() {
		final IMenuManager menuManager = getViewSite().getActionBars().getMenuManager();
		addGroupManageMenusToViewMenu(menuManager);
	}

	/**
	 * @param menuManager IMenuManager
	 */
	protected void addGroupManageMenusToViewMenu(final IMenuManager menuManager) {
		final IAction act9 = new Action(Messages.menuItem, Action.AS_RADIO_BUTTON) {

			public void run() {
				controller.setShowGroupPreference(false);
				refreshTreeInput();
			}

		};

		final IAction act10 = new Action(Messages.menuGroup, Action.AS_RADIO_BUTTON) {

			public void run() {
				controller.setShowGroupPreference(true);
				refreshTreeInput();
			}

		};
		final IAction act11 = new Action(Messages.menuGroupSetting, null) {

			public void run() {
				controller.openGroupSettingDialog();
				refreshTreeInput();
			}

		};
		boolean showGroup = controller.getShowGroupPreference();
		act9.setChecked(!showGroup);
		act10.setChecked(showGroup);

		final IMenuManager mmGrp = new MenuManager(Messages.menuTopElment);
		mmGrp.add(act9);
		mmGrp.add(act10);
		menuManager.add(mmGrp);
		menuManager.add(new Separator());
		menuManager.add(act11);
	}

	/**
	 * Remove listener before disposed.
	 */
	public void dispose() {
		scriptManager.removeListener(this);
		super.dispose();
	}

	/**
	 * Get adapter
	 * 
	 * @param adapter Class
	 * @return TreeViewer
	 */
	@SuppressWarnings("rawtypes")
	public Object getAdapter(Class adapter) {
		if (adapter.equals(TreeViewer.class)) {
			return treeViewer;
		}
		return super.getAdapter(adapter);
	}

	/**
	 * Refresh the input of tree viewer.
	 * 
	 */
	private void refreshTreeInput() {
		treeViewer.setInput(controller.getTreeInput());
		treeViewer.refresh();
	}

	/**
	 * The scriptChanged will be called after the migration script manager saved
	 * scripts.
	 */
	public void scriptChanged() {
		Display.getDefault().asyncExec(new Runnable() {

			public void run() {
				if (isTreeViewAvailable()) {
					controller.reloadGroups();
					refreshTreeInput();
				}
			}
		});
	}

	/**
	 * Set focus on tree view.
	 */
	public void setFocus() {
		if (isTreeViewAvailable()) {
			treeViewer.getControl().setFocus();
		}
	}

	/**
	 * If the tree viewer available
	 * 
	 * @return true if available
	 */
	protected boolean isTreeViewAvailable() {
		return CompositeUtils.isViewerAvailable(treeViewer);
	}
}
