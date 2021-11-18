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

import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DragSourceListener;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import com.cubrid.common.ui.navigator.ICUBRIDNode;

/**
 * 
 * CUBRID Nodes DnD support: DragSourceListener.
 * 
 * @author Kevin Cao
 * 
 */
public final class CubridNodeDragSourceListener implements
		DragSourceListener {
	private final Tree tree;

	public CubridNodeDragSourceListener(Tree tree) {
		this.tree = tree;
	}

	/**
	 * item drag finished
	 * 
	 * @param event drag event
	 */
	public void dragFinished(DragSourceEvent event) {
		//System.out.println("dragFinished.");
	}

	/**
	 * item drag data
	 * 
	 * @param event drag event
	 */
	public void dragSetData(DragSourceEvent event) {
		event.data = ((ICUBRIDNode) tree.getSelection()[0].getData()).getLabel();
	}

	/**
	 * item drag start
	 * 
	 * @param event drag event
	 */
	public void dragStart(DragSourceEvent event) {
		event.doit = false;
		TreeItem[] selection = tree.getSelection();
		if (selection.length == 0) {
			return;
		}

		Class<?> clazz = null;
		for (TreeItem ti : selection) {
			ICUBRIDNode data = (ICUBRIDNode) ti.getData();
			//Node must support DND.
			if (!data.dndable()) {
				return;
			}
			//Selection must be same nodes
			Class<? extends Object> class1 = data.getClass();
			if (clazz == null) {
				clazz = class1;
			} else if (!clazz.equals(class1)) {
				return;
			}
		}
		event.doit = true;
	}
}