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

import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DropTargetAdapter;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.common.ui.navigator.node.AbstractGroupNode;

/**
 * 
 * CUBRID Node DnD support: DropTargetListener
 * 
 * @author Kevin Cao
 * 
 */
public abstract class CubridNodeDropTargetListener extends
		DropTargetAdapter {
	private final Tree tree;

	CubridNodeDropTargetListener(Tree tree) {
		this.tree = tree;
	}

	/**
	 * @param pt point
	 * @param bounds Rectangle
	 * @return true if the y is above the rectangle's middle line
	 */
	protected boolean aboveTheRactangleMiddle(Point pt, Rectangle bounds) {
		return pt.y < (bounds.y + bounds.height / 2);
	}

	/**
	 * Drag item enter the tree items
	 * 
	 * @param event DropTargetEvent
	 */
	public void dragEnter(DropTargetEvent event) {
		TreeItem[] selectedItems = getTreeSelection();
		//do not support multi DROP_COPY
		int selectionCount = selectedItems.length;
		if (!isSupportedBehavior(event, selectionCount)) {
			setDetailToNone(event);
			setBeedbackToNone(event);
		}
	}

	/**
	 * When drag operation change, check whether to support this operation
	 * 
	 * @param event DropTargetEvent
	 */
	public void dragOperationChanged(DropTargetEvent event) {
		dragEnter(event);
	}

	/**
	 * Drag item over the tree items.
	 * 
	 * @param event DropTargetEvent
	 */
	public void dragOver(DropTargetEvent event) {
		event.feedback = DND.FEEDBACK_EXPAND | DND.FEEDBACK_SCROLL;
		if (event.item == null) {
			setBeedbackToNone(event);
			return;
		}
		//do not support multi DROP_COPY
		TreeItem[] selectedItems = getTreeSelection();
		int selectionCount = selectedItems.length;
		if (!isSupportedBehavior(event, selectionCount)) {
			setBeedbackToNone(event);
			return;
		}
		for (TreeItem ti : selectedItems) {
			if (ti.equals(event.item)) {
				setBeedbackToNone(event);
				return;
			}
		}
		//Target TreeItem
		TreeItem targetTreeItem = (TreeItem) event.item;
		ICUBRIDNode data = (ICUBRIDNode) targetTreeItem.getData();
		if (!data.dndable()) {
			setBeedbackToNone(event);
			return;
		}
		if (data instanceof AbstractGroupNode) {
			event.feedback |= DND.FEEDBACK_SELECT;
			return;
		}
		//Convert drop coordinate from Display to Tree
		Point pt = getMousePointInTreeViewer(event);
		Rectangle bounds = targetTreeItem.getBounds();
		if (aboveTheRactangleMiddle(pt, bounds)) {
			event.feedback |= DND.FEEDBACK_INSERT_BEFORE;
		} else {
			//if (pt.y > bounds.y + 2 * bounds.height / 3)
			event.feedback |= DND.FEEDBACK_INSERT_AFTER;
		}
	}

	/**
	 * @param event DropTargetEvent
	 */
	public void drop(DropTargetEvent event) {
		if (event.data == null) {
			setDetailToNone(event);
			return;
		}
		TreeItem[] selectedItems = getTreeSelection();
		int selectionCount = selectedItems.length;
		if (!isSupportedBehavior(event, selectionCount)) {
			setDetailToNone(event);
		}
		//If drop on a selected node, do nothing.
		for (TreeItem ti : selectedItems) {
			if (ti.equals(event.item)) {
				setBeedbackToNone(event);
				return;
			}
		}

		//final int dropOperation = event.detail;
		ICUBRIDNode dropNode = null;
		boolean insertBefore = includInsertBeforeTag(event);
		if (event.item != null) {
			//Move under a TreeItem node
			TreeItem dropItem = (TreeItem) event.item;
			dropNode = (ICUBRIDNode) dropItem.getData();
			Point pt = getMousePointInTreeViewer(event);
			Rectangle bounds = dropItem.getBounds();
			if (aboveTheRactangleMiddle(pt, bounds)) {
				insertBefore = true;
			}
		}
		moveNodes(dropNode, insertBefore);
	}

	/**
	 * @param event DnD event
	 * @return point in the tree viewer
	 */
	protected Point getMousePointInTreeViewer(DropTargetEvent event) {
		return Display.getCurrent().map(null, tree, event.x, event.y);
	}

	/**
	 * @return selected tree items
	 */
	protected TreeItem[] getTreeSelection() {
		return tree.getSelection();
	}

	/**
	 * @param event DropTargetEvent
	 * @return true if event.feedback contains DND.FEEDBACK_INSERT_BEFORE
	 */
	protected boolean includInsertBeforeTag(DropTargetEvent event) {
		return (event.feedback & DND.FEEDBACK_INSERT_BEFORE) == DND.FEEDBACK_INSERT_BEFORE;
	}

	/**
	 * @param event DropTargetEvent
	 * @param selectionCount selected items count
	 * 
	 * @return true if the Dnd behavior is supported.
	 */
	protected boolean isSupportedBehavior(DropTargetEvent event, int selectionCount) {
		if (event.detail == DND.DROP_COPY && selectionCount > 1) {
			return false;
		}
		return true;
	}

	/**
	 * Move nodes
	 * 
	 * @param targetNode target dropped node
	 * @param isBefore isBefore
	 */
	protected abstract void moveNodes(ICUBRIDNode targetNode, boolean isBefore);

	/**
	 * @param event DropTargetEvent
	 */
	protected void setBeedbackToNone(DropTargetEvent event) {
		event.feedback = DND.FEEDBACK_NONE;
	}

	/**
	 * @param event DropTargetEvent
	 */
	protected void setDetailToNone(DropTargetEvent event) {
		event.detail = DND.DROP_NONE;
	}
}