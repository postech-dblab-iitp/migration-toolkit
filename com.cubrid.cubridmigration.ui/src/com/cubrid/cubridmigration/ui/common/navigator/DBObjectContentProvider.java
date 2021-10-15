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
package com.cubrid.cubridmigration.ui.common.navigator;

import java.util.List;

import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.Viewer;

import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TableNode;

/**
 * 
 * DBObjectContentProvider
 * 
 * @author moulinwang
 * @version 1.0 - 2009-10-13
 */
public class DBObjectContentProvider implements
		ITreeContentProvider {

	/**
	 * getElements
	 * 
	 * @param inputElement Object
	 * @return Object[]
	 */
	@SuppressWarnings("unchecked")
	public Object[] getElements(Object inputElement) {
		if (inputElement instanceof List<?>) {
			List<ICUBRIDNode> list = (List<ICUBRIDNode>) inputElement;
			ICUBRIDNode[] nodeArr = new ICUBRIDNode[list.size()];
			return list.toArray(nodeArr);
		} else if (inputElement instanceof TableNode) {
			return new Object[]{};
		} else if (inputElement instanceof ICUBRIDNode) {
			List<ICUBRIDNode> list = ((ICUBRIDNode) inputElement).getChildren();
			ICUBRIDNode[] nodeArr = new ICUBRIDNode[list.size()];
			return list.toArray(nodeArr);
		}

		return new Object[]{};
	}

	/**
	 * dispose
	 */
	public void dispose() {
		//do nothing
	}

	/**
	 * inputChanged
	 * 
	 * @param viewer Viewer
	 * @param oldInput Object
	 * @param newInput Object
	 */
	public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
		//do nothing
	}

	/**
	 * getChildren
	 * 
	 * @param parentElement Object
	 * @return Object[]
	 */
	public Object[] getChildren(Object parentElement) {
		return getElements(parentElement);
	}

	/**
	 * getParent
	 * 
	 * @param element Object
	 * @return Object
	 */
	public Object getParent(Object element) {
		if (element instanceof ICUBRIDNode) {
			ICUBRIDNode node = (ICUBRIDNode) element;
			return node.getParent();
		}

		return null;
	}

	/**
	 * hasChildren
	 * 
	 * @param element Object
	 * @return boolean
	 */
	public boolean hasChildren(Object element) {
		if (element instanceof TableNode) {
			return false;
		} else if (element instanceof ICUBRIDNode) {
			ICUBRIDNode node = (ICUBRIDNode) element;
			return node.isContainer();
		}

		return false;
	}

}