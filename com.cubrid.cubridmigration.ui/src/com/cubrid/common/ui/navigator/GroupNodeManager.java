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
package com.cubrid.common.ui.navigator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import com.cubrid.common.ui.navigator.node.AbstractGroupNode;
import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.common.xml.IXMLMemento;
import com.cubrid.cubridmigration.core.common.xml.XMLMemento;

/**
 * 
 * Group node persist manager
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-4-9 created by Kevin Cao
 */
public final class GroupNodeManager implements
		ICUBRIDGroupNodeManager {

	private static final Logger LOGGER = LogUtil.getLogger(GroupNodeManager.class);

	private static final String DEFAULT_GROUP_ID = "Default Group";
	private static final String DEFAULT_GROUP_IMAGE = "icon/group.png";

	private final String configFile;

	private List<AbstractGroupNode> groupNodeList = new ArrayList<AbstractGroupNode>();
	private final IItemModelOfGroupProvider itemModelProvider;
	private final IGroupAndItemNodeFactory nodeFactory;
	private List<ICUBRIDNode> itemNodes = new ArrayList<ICUBRIDNode>();

	public GroupNodeManager(String configFile,
			IGroupAndItemNodeFactory groupNodeFactory,
			IItemModelOfGroupProvider itemProvider) {
		this.configFile = configFile;
		this.itemModelProvider = itemProvider;
		this.nodeFactory = groupNodeFactory;
		refreshItemNodes();
		loadGroupNode();
	}

	/**
	 * Add a new group node to list. The default group node only contain the
	 * items which has no parent group.
	 * 
	 * @param group new group node.
	 */
	public void addGroupNode(AbstractGroupNode group) {
		if (!groupNodeList.contains(group)) {
			groupNodeList.add(group);
		}
		//refresh the default node.
		List<ICUBRIDNode> nodesHasParent = new ArrayList<ICUBRIDNode>();
		AbstractGroupNode dftNode = getDefaultGroup();
		for (AbstractGroupNode grp : groupNodeList) {
			//if is default node, don't add children here.
			if (!isDefaultGroup(grp)) {
				nodesHasParent.addAll(grp.getChildren());
			}
		}
		//Refresh default node
		dftNode.removeAllChild();
		for (ICUBRIDNode db : itemNodes) {
			if (!nodesHasParent.contains(db)) {
				dftNode.addChild(db);
			}
		}
	}

	/**
	 * Group node factory method.
	 * 
	 * @param id node id
	 * @param label node label
	 * @return AbstractGroupNode
	 */
	public AbstractGroupNode createGroupNode(String id, String label) {
		return nodeFactory.createNewGroupNode(id, label, DEFAULT_GROUP_IMAGE);
	}

	/**
	 * Get all group items just like hosts or connections.
	 * 
	 * @return the group items of all.
	 */
	public List<ICUBRIDNode> getAllGroupItems() {
		return new ArrayList<ICUBRIDNode>(itemNodes);
	}

	/**
	 * Get all group nodes save at local.
	 * 
	 * @return all group nodes.
	 */
	public List<AbstractGroupNode> getAllGroupNodes() {
		return new ArrayList<AbstractGroupNode>(groupNodeList);
	}

	/**
	 * Retrieve the default group, woun't be null.
	 * 
	 * @return the default group.
	 */
	public AbstractGroupNode getDefaultGroup() {
		AbstractGroupNode groupById = getGroupById(DEFAULT_GROUP_ID);
		if (groupById == null) {
			groupById = nodeFactory.createNewGroupNode(DEFAULT_GROUP_ID,
					DEFAULT_GROUP_ID, DEFAULT_GROUP_IMAGE);
			groupNodeList.add(groupById);
		}
		return groupById;
	}

	/**
	 * get the group object by group id or group name
	 * 
	 * @param id group id or group name
	 * @return Group node.
	 */
	public AbstractGroupNode getGroupById(String id) {
		List<AbstractGroupNode> groups = getAllGroupNodes();
		for (AbstractGroupNode group : groups) {
			if (group.getId().equals(id)) {
				return group;
			}
		}
		return null;
	}

	/**
	 * get the group object by group name
	 * 
	 * @param nodeList group list
	 * @param name group name
	 * @return Group node.
	 */
	public AbstractGroupNode getGroupByName(List<AbstractGroupNode> nodeList,
			String name) {
		for (AbstractGroupNode group : nodeList) {
			if (group.getName().equals(name)) {
				return group;
			}
		}
		return null;
	}

	/**
	 * get the group object by group name
	 * 
	 * @param name group name
	 * @return Group node.
	 */
	public AbstractGroupNode getGroupByName(String name) {
		return getGroupByName(getAllGroupNodes(), name);
	}

	/**
	 * Get the group's item by item's name
	 * 
	 * @param name item's name
	 * @return Group item
	 */
	public ICUBRIDNode getGroupItemByItemName(String name) {
		List<ICUBRIDNode> result = getAllGroupItems();
		for (ICUBRIDNode node : result) {
			if (node.getName().equals(name)) {
				return node;
			}
		}
		return null;
	}

	/**
	 * Find group's item by item id.
	 * 
	 * @param itemId node id
	 * @return ICUBRIDNode
	 */
	private ICUBRIDNode getItemByID(String itemId) {
		for (ICUBRIDNode node : itemNodes) {
			if (node.getId().equals(itemId)) {
				return node;
			}
		}
		return null;
	}

	/**
	 * Retrieves whether the parameter is default group.
	 * 
	 * @param group that need to be compare.
	 * @return true:is default;
	 */
	public boolean isDefaultGroup(AbstractGroupNode group) {
		return group.getId().equals(DEFAULT_GROUP_ID);
	}

	/**
	 * Load group nodes from local preference.
	 * 
	 */
	private void loadGroupNode() {
		IXMLMemento memento = null;
		try {
			memento = XMLMemento.loadMemento(configFile);
		} catch (IOException e) {
			LOGGER.error("Load group error", e);
		}
		loadGroupNode(memento);
	}

	/**
	 * Load group nodes from xml memento.
	 * 
	 * @param memento IXMLMemento
	 */
	private void loadGroupNode(IXMLMemento memento) {
		List<AbstractGroupNode> tempList = new ArrayList<AbstractGroupNode>();
		AbstractGroupNode defaultGroup = getDefaultGroup();
		//Reset parents
		for (ICUBRIDNode it : itemNodes) {
			it.setParent(null);
		}
		if (memento != null) {
			IXMLMemento[] children = memento.getChildren("group");
			List<String> idList = new ArrayList<String>();
			for (int i = 0; i < children.length; i++) {
				String id = children[i].getString("id");
				//Duplicated ID will be ignored.
				if (idList.contains(id)) {
					continue;
				}
				idList.add(id);
				AbstractGroupNode cgn = getGroupById(id);
				//Reuse the node instances.
				if (null == cgn) {
					String name = children[i].getString("name");
					cgn = nodeFactory.createNewGroupNode(id, name,
							DEFAULT_GROUP_IMAGE);
				}
				tempList.add(cgn);
				cgn.removeAllChild();

				IXMLMemento[] items = children[i].getChildren("item");
				for (IXMLMemento item : items) {
					String itemId = item.getString("id");
					ICUBRIDNode cs = getItemByID(itemId);
					//NULL or already has parent. 
					if (cs == null) {
						continue;
					}
					cgn.addChild(cs);
				}
			}
		}
		//If there is no group node found.
		if (tempList.isEmpty()) {
			tempList.add(defaultGroup);
		}
		//All no parent node will be the children of the default node.
		for (ICUBRIDNode it : itemNodes) {
			if (it.getParent() == null) {
				defaultGroup.addChild(it);
			}
		}
		groupNodeList.clear();
		groupNodeList.addAll(tempList);
		saveAllGroupNode();
	}

	/**
	 * Refresh the item nodes with input models.
	 * 
	 */
	private void refreshItemNodes() {
		List<Object> list = itemModelProvider.getItems();
		List<ICUBRIDNode> tempList = new ArrayList<ICUBRIDNode>();
		for (Object obj : list) {
			ICUBRIDNode node = nodeFactory.createItem(obj);
			ICUBRIDNode oldItem = getItemByID(node.getId());
			//Reuse the old node instance.
			if (null != oldItem) {
				node = oldItem;
			}
			tempList.add(node);
		}
		itemNodes.clear();
		itemNodes.addAll(tempList);
	}

	/**
	 * Reload all groups
	 * 
	 * @return CubridGroupNode List
	 */
	public void reloadGroups() {
		refreshItemNodes();
		loadGroupNode();
	}

	/**
	 * Remove group by id
	 * 
	 * @param groupId group id or group name
	 */
	public void removeGroup(String groupId) {
		AbstractGroupNode tobeRemoved = null;
		for (AbstractGroupNode group : groupNodeList) {
			if (group.getId().equals(groupId)) {
				tobeRemoved = group;
				break;
			}
		}
		if (tobeRemoved == null) {
			return;
		}
		this.groupNodeList.remove(tobeRemoved);
		AbstractGroupNode defaultGroup = getDefaultGroup();
		List<ICUBRIDNode> children = tobeRemoved.getChildren();
		for (ICUBRIDNode chi : children) {
			defaultGroup.addChild(chi);
		}
		saveAllGroupNode();
	}

	/**
	 * Reorder the groups by input string array.
	 * 
	 * @param orderedName the ordered group names.
	 */
	public void reorderGroup(String[] orderedName) {
		List<AbstractGroupNode> tempNode = new ArrayList<AbstractGroupNode>();
		for (String name : orderedName) {
			AbstractGroupNode cgn = getGroupByName(name);
			if (cgn == null) {
				continue;
			}
			tempNode.add(cgn);
		}
		groupNodeList.clear();
		groupNodeList.addAll(tempNode);
		saveAllGroupNode();
	}

	/**
	 * Save all group node.
	 * 
	 */
	public void saveAllGroupNode() {
		try {
			XMLMemento memento = XMLMemento.createWriteRoot("groups");
			for (AbstractGroupNode group : groupNodeList) {
				IXMLMemento child = memento.createChild("group");
				child.putString("id", group.getId());
				child.putString("name", group.getName());
				for (ICUBRIDNode cn : group.getChildren()) {
					IXMLMemento childHost = child.createChild("item");
					childHost.putString("id", cn.getId());
				}
			}
			saveMemento(memento);
		} catch (Exception e) {
			LOGGER.error("Save group error.", e);
		}
	}

	/**
	 * Save the XMLMemento to file.
	 * 
	 * @param memento XMLMemento
	 * @throws FileNotFoundException when file is not found.
	 * @throws IOException when IO errors.
	 */
	private void saveMemento(XMLMemento memento) throws FileNotFoundException,
			IOException {
		File file = new File(configFile);
		if (!file.exists()) {
			CUBRIDIOUtils.clearFileOrDir(file);
		}
		FileOutputStream writer = new FileOutputStream(configFile);
		try {
			memento.save(writer);
		} finally {
			writer.close();
		}
	}

	/**
	 * changeItemPosition
	 * 
	 * @param targetNode ICUBRIDNode
	 * @param selected List<ICUBRIDNode> same type nodes
	 * @param isBefore true if insert before
	 */
	public void changeItemPosition(ICUBRIDNode targetNode,
			List<ICUBRIDNode> selected, boolean isBefore) {
		if (CollectionUtils.isEmpty(selected)) {
			return;
		}
		ICUBRIDNode realTargetGrp;
		ICUBRIDNode realTarget = null;
		if (targetNode == null) {
			realTargetGrp = groupNodeList.get(groupNodeList.size() - 1);
		} else if (targetNode instanceof AbstractGroupNode) {
			realTargetGrp = (AbstractGroupNode) targetNode;
		} else {
			realTargetGrp = (AbstractGroupNode) targetNode.getParent();
			realTarget = targetNode;
		}
		//Remove to be re-ordered object from old list.
		for (ICUBRIDNode tobe : selected) {
			//If target can't be added into group node. 
			if (!realTargetGrp.canAddToChildren(tobe)) {
				return;
			}
			if (tobe.getParent() != null) {
				tobe.getParent().removeChild(tobe);
				tobe.setParent(null);
			}
		}
		List<ICUBRIDNode> children = new ArrayList<ICUBRIDNode>(
				realTargetGrp.getChildren());
		int oldIdx = realTarget == null ? children.size()
				: children.indexOf(realTarget);
		oldIdx = isBefore ? oldIdx : (oldIdx + 1);
		int idx = Math.max(0, oldIdx);
		idx = Math.min(idx, children.size());
		children.addAll(idx, selected);
		realTargetGrp.removeAllChild();
		realTargetGrp.addChild(children);
		saveAllGroupNode();
	}
}
