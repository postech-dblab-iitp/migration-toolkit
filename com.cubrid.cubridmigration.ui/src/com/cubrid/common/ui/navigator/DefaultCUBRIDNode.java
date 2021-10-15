/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
 * Solution.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 */
package com.cubrid.common.ui.navigator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.Platform;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IPersistableElement;

import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.common.navigator.node.CubridNodeType;

/**
 * 
 * DefaultCubridNode
 * 
 * @author moulinwang
 * @version 1.0 - 2009-10-13
 */
public class DefaultCUBRIDNode implements
		ICUBRIDNode {
	protected List<ICUBRIDNode> childList = null;
	private String id = "";
	private String editorId = null;
	private String viewId = null;
	private String label = "";
	private ICUBRIDNode parent = null;
	private boolean isRoot = false;
	private String iconPath = "";
	private CubridNodeType type = null;

	private boolean isContainer = false;
	private Object modelObj = null;

	private final Map<String, Object> data = new HashMap<String, Object>();

	private boolean collapseStatus; //mark CubridNode collapse status

	/**
	 * clear
	 */
	public void clear() {
		data.clear();

		if (childList != null) {
			for (ICUBRIDNode childNode : childList) {
				childNode.clear();
			}
		}
	}

	public boolean isCollapseStatus() {
		return collapseStatus;
	}

	public void setCollapseStatus(boolean collapseStatus) {
		this.collapseStatus = collapseStatus;
	}

	/**
	 * get Data
	 * 
	 * @param key String
	 * @return Object
	 */
	public Object get(String key) {
		return data.get(key);
	}

	/**
	 * put
	 * 
	 * @param key String
	 * @param value Object
	 * @return Object
	 */
	public Object put(String key, Object value) {
		return data.put(key, value);
	}

	/**
	 * putAll
	 * 
	 * @param key String
	 * @param value Object
	 * @return Object
	 */
	public Object putAll(String key, Object value) {
		if (childList != null) {
			for (ICUBRIDNode childNode : childList) {
				((DefaultCUBRIDNode) childNode).putAll(key, value);
			}
		}

		return data.put(key, value);
	}

	/**
	 * The constructor
	 * 
	 * @param id
	 * @param label
	 * @param iconPath
	 */
	public DefaultCUBRIDNode(String id, String label, String iconPath) {
		this.id = id;
		this.label = label;
		this.iconPath = iconPath;
		isRoot = false;
		childList = new ArrayList<ICUBRIDNode>();
	}

	/**
	 * @see ICUBRIDNode#isContainer()
	 * @return boolean
	 */
	public boolean isContainer() {
		return isContainer;
	}

	/**
	 * @see ICUBRIDNode#setContainer(boolean)
	 * @param isContainer boolean
	 */
	public void setContainer(boolean isContainer) {
		this.isContainer = isContainer;
	}

	/**
	 * @see ICUBRIDNode#getChild(String)
	 * @param id String
	 * @return ICubridNode
	 */
	public ICUBRIDNode getChild(String id) {
		if (childList != null) {
			for (ICUBRIDNode node : childList) {
				if (node != null && node.getId().equals(id)) {
					return node;
				}
			}
		}

		return null;
	}

	/**
	 * @see ICUBRIDNode#getChildInAll(String)
	 * @param id String
	 * @return ICubridNode
	 */
	public ICUBRIDNode getChildInAll(String id) {
		ICUBRIDNode childNode = getChild(id);

		if (childNode == null) {
			for (ICUBRIDNode node : childList) {
				childNode = node.getChild(id);

				if (childNode != null) {
					return childNode;
				}
			}
		} else {

			return childNode;
		}

		return null;
	}

	/**
	 * @see ICUBRIDNode#getChildren()
	 * @return List<ICubridNode>
	 */
	public List<ICUBRIDNode> getChildren() {
		return childList;
	}

	/**
	 * @see ICUBRIDNode#getChildren(IProgressMonitor)
	 * @param monitor IProgressMonitor
	 * @return ICubridNode[]
	 */
	public ICUBRIDNode[] getChildren(IProgressMonitor monitor) {
		if (!childList.isEmpty()) {
			ICUBRIDNode[] nodeArr = new ICUBRIDNode[childList.size()];
			return childList.toArray(nodeArr);
		}

		return new ICUBRIDNode[]{};
	}

	/**
	 * @see ICUBRIDNode#addChild(ICUBRIDNode)
	 * @param obj ICubridNode
	 */
	public void addChild(ICUBRIDNode obj) {
		if (obj != null && !isContained(obj)) {
			obj.setParent(this);
			childList.add(obj);
		}
	}

	/**
	 * @see ICUBRIDNode#removeChild(ICUBRIDNode)
	 * @param obj ICubridNode
	 */
	public void removeChild(ICUBRIDNode obj) {
		if (obj != null) {
			childList.remove(obj);
		}
	}

	/**
	 * @see ICUBRIDNode#removeAllChild()
	 */
	public void removeAllChild() {
		childList.clear();
	}

	/**
	 * @see ICUBRIDNode#getParent()
	 * @return ICubridNode
	 */
	public ICUBRIDNode getParent() {
		return parent;
	}

	/**
	 * @see ICUBRIDNode#setParent(ICUBRIDNode)
	 * @param obj ICubridNode
	 */
	public void setParent(ICUBRIDNode obj) {
		parent = obj;
	}

	/**
	 * @see ICUBRIDNode#isContained(ICUBRIDNode)
	 * @param obj ICubridNode
	 * @return boolean
	 */
	public boolean isContained(ICUBRIDNode obj) {
		if (obj == null) {
			return false;
		}
		for (ICUBRIDNode node : childList) {
			if (node != null && node.getId().equals(obj.getId())) {
				return true;
			}
		}

		return false;
	}

	/**
	 * @see ICUBRIDNode#isContainedInAll(ICUBRIDNode)
	 * @param obj ICubridNode
	 * @return boolean
	 */
	public boolean isContainedInAll(ICUBRIDNode obj) {
		if (childList.contains(obj)) {
			return true;
		} else {
			for (ICUBRIDNode node : childList) {
				if (node.isContainedInAll(obj)) {
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * @see ICUBRIDNode#isRoot()
	 * @return boolean
	 */
	public boolean isRoot() {
		return isRoot;
	}

	/**
	 * @see ICUBRIDNode#setRoot(boolean)
	 * @param isRoot boolean
	 */
	public void setRoot(boolean isRoot) {
		this.isRoot = isRoot;
	}

	/**
	 * @see ICUBRIDNode#getIconPath()
	 * @return String
	 */
	public String getIconPath() {
		return iconPath;
	}

	/**
	 * @see ICUBRIDNode#setIconPath(String)
	 * @param iconPath String
	 */
	public void setIconPath(String iconPath) {
		this.iconPath = iconPath;
	}

	/**
	 * @see ICUBRIDNode#getLabel()
	 * @return String
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * @see ICUBRIDNode#setLabel(String)
	 * @param label String
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	/**
	 * @see ICUBRIDNode#getId()
	 * @return String
	 */
	public String getId() {
		return id;
	}

	/**
	 * @see ICUBRIDNode#setId(String)
	 * @param id String
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @see ICUBRIDNode#getEditorId()
	 * @return String
	 */
	public String getEditorId() {
		return editorId;
	}

	/**
	 * @see ICUBRIDNode#setEditorId(String)
	 * @param editorId String
	 */
	public void setEditorId(String editorId) {
		this.editorId = editorId;

	}

	/**
	 * @see ICUBRIDNode#getViewId()
	 * @param String
	 * @return String
	 */
	public String getViewId() {
		return viewId;
	}

	/**
	 * @see ICUBRIDNode#setViewId(String)
	 * @param viewId String
	 */
	public void setViewId(String viewId) {
		this.viewId = viewId;

	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.core.runtime.IAdaptable#getAdapter(java.lang.Class)
	 * @param adapter Class
	 * @return Object
	 */
	@SuppressWarnings({"rawtypes" })
	public Object getAdapter(Class adapter) {
		if (modelObj != null && modelObj.getClass() == adapter) {
			return modelObj;
		}

		return Platform.getAdapterManager().getAdapter(this, adapter);
	}

	/**
	 * @see ICUBRIDNode#setType(CubridNodeType)
	 * @param type CubridNodeType
	 */
	public void setType(CubridNodeType type) {
		this.type = type;
	}

	/**
	 * @see ICUBRIDNode#getType()
	 * @return CubridNodeType
	 */
	public CubridNodeType getType() {
		return this.type;
	}

	/**
	 * @see IEditorInput#exists()
	 * @return boolean
	 */
	public boolean exists() {
		return false;
	}

	/**
	 * @see IEditorInput#getPersistable()
	 * @return IPersistableElement
	 */
	public IPersistableElement getPersistable() {
		return null;
	}

	/**
	 * @see IEditorInput#getName()
	 * @return String
	 */
	public String getName() {
		return getLabel();
	}

	/**
	 * @see IEditorInput#getToolTipText()
	 * @return String
	 */
	public String getToolTipText() {
		String tipText = getLabel();
		ICUBRIDNode parent = getParent();

		while (parent != null) {
			tipText = parent.getLabel() + "/" + tipText;
			parent = parent.getParent();
		}

		return tipText;
	}

	/**
	 * @see IEditorInput#getImageDescriptor()
	 * @return ImageDescriptor
	 */
	public ImageDescriptor getImageDescriptor() {
		if (getIconPath() != null && getIconPath().trim().length() > 0) {
			return MigrationUIPlugin.getImageDescriptor(getIconPath());
		}

		return null;
	}

	/**
	 * @see ICUBRIDNode#setModelObj(Object)
	 * @param obj Object
	 */
	public void setModelObj(Object obj) {
		modelObj = obj;
	}

	/**
	 * Retrieves if the input can be added into this node's children list.
	 * 
	 * @param child node
	 * @return true if can be this node's child.
	 */
	public boolean canAddToChildren(ICUBRIDNode child) {
		return true;
	}

	/**
	 * If The node can be DnD.
	 * 
	 * @return true if it can.
	 */
	public boolean dndable() {
		return false;
	}

	/**
	 * Add child object to this node
	 * 
	 * @param objs ICubridNode
	 */
	public void addChild(List<ICUBRIDNode> objs) {
		List<ICUBRIDNode> tmp = new ArrayList<ICUBRIDNode>(objs);
		for (ICUBRIDNode nd : tmp) {
			addChild(nd);
		}
	}
}
