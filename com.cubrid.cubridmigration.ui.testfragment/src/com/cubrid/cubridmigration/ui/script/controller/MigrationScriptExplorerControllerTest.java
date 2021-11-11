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
package com.cubrid.cubridmigration.ui.script.controller;

import java.io.File;
import java.util.List;

import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.StructuredSelection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.common.ui.navigator.node.AbstractGroupNode;
import com.cubrid.cubridmigration.ui.BaseUITestCase;
import com.cubrid.cubridmigration.ui.script.MigrationScript;
import com.cubrid.cubridmigration.ui.script.MigrationScriptManager;

/**
 * @author Kevin Cao
 * 
 */
public class MigrationScriptExplorerControllerTest extends
		BaseUITestCase {

	@Before
	public void start() {
		MigrationScriptManager.initialize();
		try {
			File file = new File(testScriptFilesDir);
			String[] scriptFileNames = file.list();
			for (String scriptFileName : scriptFileNames) {
				MigrationScriptManager.getInstance().importScript(
						testScriptFilesDir + scriptFileName);
			}
		} catch (Exception ex) {

		}

	}

	@After
	public void end() {
		List<MigrationScript> scripts = MigrationScriptManager.getInstance().getScripts();
		for (MigrationScript script : scripts) {
			MigrationScriptManager.getInstance().remove(script);
		}
		MigrationScriptManager.getInstance().save();
	}

	@Test
	public void testSaveAndShowPreference() {

		MigrationScriptExplorerController controller = new MigrationScriptExplorerController();

		controller.setShowGroupPreference(true);
		Assert.assertTrue(controller.getShowGroupPreference());
		controller.setShowGroupPreference(false);
		Assert.assertFalse(controller.getShowGroupPreference());

	}

	@Test
	public void test_getTreeInput() {
		MigrationScriptExplorerController controller = new MigrationScriptExplorerController();

		controller.setShowGroupPreference(true);
		List<? extends ICUBRIDNode> treeInput = controller.getTreeInput();
		Assert.assertEquals(1, treeInput.size());

		controller.setShowGroupPreference(false);
		treeInput = controller.getTreeInput();
		Assert.assertEquals(3, treeInput.size());

	}

	@Test
	public void test_moveNodes_item_1() {
		//Move item without group node
		MigrationScriptExplorerController controller = new MigrationScriptExplorerController();
		controller.setShowGroupPreference(false);

		List<? extends ICUBRIDNode> treeInput = controller.getTreeInput();
		ICUBRIDNode selectedNode = treeInput.get(2);
		ICUBRIDNode targetNode = treeInput.get(1);
		IStructuredSelection selection = new StructuredSelection(selectedNode);

		controller.moveNodes(selection, targetNode, true);
		//Verify results
		controller.reloadGroups();
		treeInput = controller.getTreeInput();
		Assert.assertEquals(selectedNode.getLabel(), treeInput.get(1).getLabel());
	}

	@Test
	public void test_moveNodes_item_2() {
		//Move item without group node
		MigrationScriptExplorerController controller = new MigrationScriptExplorerController();
		controller.setShowGroupPreference(false);

		List<? extends ICUBRIDNode> treeInput = controller.getTreeInput();
		ICUBRIDNode selectedNode = treeInput.get(0);
		ICUBRIDNode targetNode = treeInput.get(1);
		IStructuredSelection selection = new StructuredSelection(selectedNode);

		controller.moveNodes(selection, targetNode, false);

		//Verify results
		controller.reloadGroups();
		treeInput = controller.getTreeInput();
		Assert.assertEquals(selectedNode.getId(), treeInput.get(1).getId());

	}

	@Test
	public void test_moveNodes_group_1() {
		//Move item under one group node
		MigrationScriptExplorerController controller = new MigrationScriptExplorerController();
		controller.setShowGroupPreference(true);

		List<? extends ICUBRIDNode> treeInput = controller.getTreeInput();
		ICUBRIDNode groupNode = treeInput.get(0);
		List<ICUBRIDNode> children = groupNode.getChildren();
		ICUBRIDNode targetNode = children.get(1);
		ICUBRIDNode selectedNode = children.get(2);
		IStructuredSelection selection = new StructuredSelection(selectedNode);

		controller.moveNodes(selection, targetNode, true);
		//Verify results
		treeInput = controller.getTreeInput();
		groupNode = treeInput.get(0);
		children = groupNode.getChildren();
		Assert.assertEquals(selectedNode.getId(), children.get(1).getId());
	}

	@Test
	public void test_moveNodes_group_2() {
		//Move item under one group node
		MigrationScriptExplorerController controller = new MigrationScriptExplorerController();
		controller.setShowGroupPreference(true);

		List<? extends ICUBRIDNode> treeInput = controller.getTreeInput();
		ICUBRIDNode groupNode = treeInput.get(0);
		List<ICUBRIDNode> children = groupNode.getChildren();
		ICUBRIDNode targetNode = children.get(1);
		ICUBRIDNode selectedNode = children.get(0);
		IStructuredSelection selection = new StructuredSelection(selectedNode);

		controller.moveNodes(selection, targetNode, false);

		//Verify results
		treeInput = controller.getTreeInput();
		groupNode = treeInput.get(0);
		children = groupNode.getChildren();
		Assert.assertEquals(selectedNode.getId(), children.get(1).getId());

	}

	@Test
	public void test_moveNodes_group_3() {
		//Move a node from one group to another group
		MigrationScriptExplorerController controller = new MigrationScriptExplorerController();
		controller.setShowGroupPreference(true);

		AbstractGroupNode group = new AbstractGroupNode("test2", "testGroup", null);
		controller.getGgroupNodeManager().addGroupNode(group);
		try {
			List<? extends ICUBRIDNode> treeInput = controller.getTreeInput();
			ICUBRIDNode groupNode = treeInput.get(0);
			List<ICUBRIDNode> children = groupNode.getChildren();
			ICUBRIDNode targetNode = group;
			ICUBRIDNode selectedNode = children.get(0);
			IStructuredSelection selection = new StructuredSelection(selectedNode);

			controller.moveNodes(selection, targetNode, false);

			//Verify results
			treeInput = controller.getTreeInput();
			Assert.assertEquals(selectedNode.getId(), group.getChildren().get(0).getId());
		} finally {
			controller.getGgroupNodeManager().removeGroup(group.getId());
		}

	}

	@Test
	public void test_reloadGroups() {
		MigrationScriptExplorerController controller = new MigrationScriptExplorerController();

		controller.reloadGroups();
	}
}
