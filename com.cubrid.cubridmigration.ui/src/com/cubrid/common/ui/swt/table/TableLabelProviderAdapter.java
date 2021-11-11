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
package com.cubrid.common.ui.swt.table;

import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.swt.graphics.Image;

/**
 * TableLabelProviderAdaptor Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-8-2 created by Kevin Cao
 */
public abstract class TableLabelProviderAdapter implements
		ITableLabelProvider {

	/**
	 * Adds a listener to this label provider. Has no effect if an identical
	 * listener is already registered.
	 * <p>
	 * Label provider listeners are informed about state changes that affect the
	 * rendering of the viewer that uses this label provider.
	 * </p>
	 * 
	 * @param listener a label provider listener
	 */
	public void addListener(ILabelProviderListener listener) {
		//Do nothing

	}

	/**
	 * Disposes of this label provider. When a label provider is attached to a
	 * viewer, the viewer will automatically call this method when the viewer is
	 * being closed. When label providers are used outside of the context of a
	 * viewer, it is the client's responsibility to ensure that this method is
	 * called when the provider is no longer needed.
	 */
	public void dispose() {
		//Do nothing

	}

	/**
	 * Returns whether the label would be affected by a change to the given
	 * property of the given element. This can be used to optimize a
	 * non-structural viewer update. If the property mentioned in the update
	 * does not affect the label, then the viewer need not update the label.
	 * 
	 * @param element the element
	 * @param property the property
	 * @return <code>true</code> if the label would be affected, and
	 *         <code>false</code> if it would be unaffected
	 */
	public boolean isLabelProperty(Object element, String property) {
		return false;
	}

	/**
	 * Removes a listener to this label provider. Has no affect if an identical
	 * listener is not registered.
	 * 
	 * @param listener a label provider listener
	 */
	public void removeListener(ILabelProviderListener listener) {
		//Do nothing

	}

	/**
	 * Returns the label image for the given column of the given element.
	 * 
	 * @param element the object representing the entire row, or
	 *        <code>null</code> indicating that no input object is set in the
	 *        viewer
	 * @param columnIndex the zero-based index of the column in which the label
	 *        appears
	 * @return Image or <code>null</code> if there is no image for the given
	 *         object at columnIndex
	 */
	public Image getColumnImage(Object element, int columnIndex) {
		return null;
	}

	/**
	 * Returns the label text for the given column of the given element.
	 * 
	 * @param element the object representing the entire row, or
	 *        <code>null</code> indicating that no input object is set in the
	 *        viewer
	 * @param columnIndex the zero-based index of the column in which the label
	 *        appears
	 * @return String or or <code>null</code> if there is no text for the given
	 *         object at columnIndex
	 */
	public abstract String getColumnText(Object element, int columnIndex);

}
