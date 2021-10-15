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

import java.util.List;

import com.cubrid.common.ui.navigator.node.AbstractGroupNode;

/**
 * CUBRID GroupNode Manager
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-3-24 created by Kevin Cao
 */
public interface ICUBRIDGroupNodeManager {

	//	/**
	//	 * A prototype of default node,don't change any attribute of it,please use
	//	 * Object.clone to get a new default group node.
	//	 */
	//	public static final AbstractGroupNode DEFAULT_GROUP_NODE = new AbstractGroupNode(
	//			"Default Group", "Default Group", "icons/navigator/group.png");

	/**
	 * Get all group nodes save at local.
	 * 
	 * @return all group nodes.
	 */
	public List<AbstractGroupNode> getAllGroupNodes();

	/**
	 * Add a new group node to list. The default group node only contain the
	 * items which has no parent group.
	 * 
	 * @param group new group node.
	 */
	public void addGroupNode(AbstractGroupNode group);

	/**
	 * Save all group node.
	 * 
	 */
	public void saveAllGroupNode();

	/**
	 * Get all group items just like hosts or connections.
	 * 
	 * @return the group items of all.
	 */
	public List<ICUBRIDNode> getAllGroupItems();

	/**
	 * Get the group's item by item's name
	 * 
	 * @param name item's name
	 * @return Group item
	 */
	public ICUBRIDNode getGroupItemByItemName(String name);

	/**
	 * get the group object by group id
	 * 
	 * @param id group id
	 * @return Group node.
	 */
	public AbstractGroupNode getGroupById(String id);

	/**
	 * get the group object by group name
	 * 
	 * @param name group name
	 * @return Group node.
	 */
	public AbstractGroupNode getGroupByName(String name);

	/**
	 * get the group object by group name
	 * 
	 * @param nodeList the group node list
	 * @param name group name
	 * @return Group node.
	 */
	public AbstractGroupNode getGroupByName(List<AbstractGroupNode> nodeList,
			String name);

	/**
	 * Remove group by id
	 * 
	 * @param groupId group id or group name
	 */
	public void removeGroup(String groupId);

	/**
	 * Reorder the groups by input string array.
	 * 
	 * @param orderedName the ordered group names.
	 */
	public void reorderGroup(String[] orderedName);

	/**
	 * Get the default group of the group list.
	 * 
	 * @return default group.
	 */
	public AbstractGroupNode getDefaultGroup();

	/**
	 * Retrieves whether the parameter is default group.
	 * 
	 * @param group that need to be compare.
	 * @return true:is default;
	 */
	public boolean isDefaultGroup(AbstractGroupNode group);

	/**
	 * createGroupNode
	 * 
	 * @param id String
	 * @param label String
	 * @return AbstractGroupNode
	 */
	public AbstractGroupNode createGroupNode(String id, String label);

	/**
	 * reloadGroups
	 * 
	 */
	public void reloadGroups();

	/**
	 * changeItemPosition
	 * 
	 * @param targetNode ICUBRIDNode
	 * @param selected List<ICUBRIDNode> same type nodes
	 * @param isBefore true if insert before
	 */
	public void changeItemPosition(ICUBRIDNode targetNode,
			List<ICUBRIDNode> selected, boolean isBefore);
}
