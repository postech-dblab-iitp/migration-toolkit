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

import java.util.List;

import org.eclipse.core.runtime.IAdaptable;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.ui.IEditorInput;

import com.cubrid.cubridmigration.ui.common.navigator.node.CubridNodeType;

/**
 * 
 * ICubridNode
 * 
 * @author moulinwang
 * @version 1.0 - 2009-10-13
 */
public interface ICUBRIDNode extends
		IAdaptable,
		IEditorInput {

	/**
	 * clear
	 */
	public void clear();

	/**
	 * Get whether it is container node
	 * 
	 * @return boolean
	 */
	public boolean isContainer();

	/**
	 * 
	 * Set this node for container node
	 * 
	 * @param isContainer boolean
	 */
	public void setContainer(boolean isContainer);

	/**
	 * 
	 * Get child CUBRID Node by id
	 * 
	 * @param id String
	 * @return ICubridNode
	 */
	public ICUBRIDNode getChild(String id);

	/**
	 * Search all children nodes and Get the child node by id
	 * 
	 * @param id String
	 * @return ICubridNode
	 */
	public ICUBRIDNode getChildInAll(String id);

	/**
	 * 
	 * Get all children of this node
	 * 
	 * @return List<ICubridNode>
	 */
	public List<ICUBRIDNode> getChildren();

	/**
	 * 
	 * Get all children of this node
	 * 
	 * @param monitor IProgressMonitor
	 * @return Cubrid Node Array
	 */
	public ICUBRIDNode[] getChildren(IProgressMonitor monitor);

	/**
	 * Add child object to this node
	 * 
	 * @param obj ICubridNode
	 */
	public void addChild(ICUBRIDNode obj);

	/**
	 * Add child object to this node
	 * 
	 * @param objs ICubridNode
	 */
	public void addChild(List<ICUBRIDNode> objs);

	/**
	 * Remove child object from this node
	 * 
	 * @param obj ICubridNode
	 */
	public void removeChild(ICUBRIDNode obj);

	/**
	 * Remove all child objects from this node
	 */
	public void removeAllChild();

	/**
	 * Get parent object of this node
	 * 
	 * @return ITree
	 */
	public ICUBRIDNode getParent();

	/**
	 * Set this node's parent node object
	 * 
	 * @param obj ICubridNode
	 */
	public void setParent(ICUBRIDNode obj);

	/**
	 * Get whether contain this child node in this node,only traverse the first
	 * level
	 * 
	 * @param obj ICubridNode
	 * @return boolean
	 */
	public boolean isContained(ICUBRIDNode obj);

	/**
	 * Get whether contain this child node in this node,traverse all children
	 * 
	 * @param obj ICubridNode
	 * @return boolean
	 */
	public boolean isContainedInAll(ICUBRIDNode obj);

	/**
	 * Retrun whether it is the top level node
	 * 
	 * @return boolean
	 */
	public boolean isRoot();

	/**
	 * Set this node for root node
	 * 
	 * @param isRoot boolean
	 */
	public void setRoot(boolean isRoot);

	/**
	 * Get the path of this node object's icon path
	 * 
	 * @return String
	 */
	public String getIconPath();

	/**
	 * Set the path of this node object's icon path
	 * 
	 * @param iconPath String
	 */
	public void setIconPath(String iconPath);

	/**
	 * Get displayed label of this node
	 * 
	 * @return String
	 */
	public String getLabel();

	/**
	 * Set displayed label of this node
	 * 
	 * @param label String
	 */
	public void setLabel(String label);

	/**
	 * Get the UUID of this node
	 * 
	 * @return String
	 */
	public String getId();

	/**
	 * Set the UUID of this node
	 * 
	 * @param id String
	 */
	public void setId(String id);

	/**
	 * Get editor id of this node
	 * 
	 * @return String
	 */
	public String getEditorId();

	/**
	 * 
	 * Set editor id of this node
	 * 
	 * @param editorId String
	 */
	public void setEditorId(String editorId);

	/**
	 * 
	 * Get view id of this node
	 * 
	 * @return String
	 */
	public String getViewId();

	/**
	 * 
	 * Set view id of this node
	 * 
	 * @param viewId String
	 */
	public void setViewId(String viewId);

	/**
	 * Set this node object's type
	 * 
	 * @param type CubridNodeType
	 */
	public void setType(CubridNodeType type);

	/**
	 * Get this node object's type
	 * 
	 * @return CubridNodeType
	 */
	public CubridNodeType getType();

	/**
	 * Set the corresponding CUBRID model object of this node
	 * 
	 * @param obj Object
	 */
	public void setModelObj(Object obj);

	/**
	 * Set the collapse status
	 * 
	 * @param collapseStatus is collapsed or not
	 */
	public void setCollapseStatus(boolean collapseStatus);

	/**
	 * get the collapse status
	 * 
	 * @return is collapsed or not
	 */
	public boolean isCollapseStatus();

	/**
	 * Retrieves if the input can be added into this node's children list.
	 * 
	 * @param child node
	 * @return true if can be this node's child.
	 */
	public boolean canAddToChildren(ICUBRIDNode child);

	/**
	 * If The node can be DnD.
	 * 
	 * @return true if it can.
	 */
	public boolean dndable();
}
