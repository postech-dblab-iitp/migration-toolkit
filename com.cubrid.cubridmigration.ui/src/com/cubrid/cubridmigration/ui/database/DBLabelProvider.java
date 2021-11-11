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
package com.cubrid.cubridmigration.ui.database;

import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.swt.graphics.Image;

import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * 
 * DBLabelProvider
 * 
 * @author JessieHuang
 * @version 1.0 - 2009-12-24 created by JessieHuang
 */
public class DBLabelProvider implements
		ITableLabelProvider {
	public static final Image CHECK_IMAGE = MigrationUIPlugin.getImage("icon/checked.gif");
	public static final Image UNCHECK_IMAGE = MigrationUIPlugin.getImage("icon/unchecked.gif");

	/**
	 * getColumnImage
	 * 
	 * @param element Object
	 * @param columnIndex int
	 * @return Image
	 */
	public Image getColumnImage(Object element, int columnIndex) {
		DatabaseConnectionInfo info = (DatabaseConnectionInfo) element;

		if (columnIndex == 0) {
			if (info.isSelected()) {
				return CHECK_IMAGE;
			} else {
				return UNCHECK_IMAGE;
			}
		} else {
			return null;
		}
	}

	/**
	 * getColumnText
	 * 
	 * @param element Object
	 * @param columnIndex int
	 * @return Column Text
	 */
	public String getColumnText(Object element, int columnIndex) {
		DatabaseConnectionInfo info = (DatabaseConnectionInfo) element;

		switch (columnIndex) {
		case 0:
			return null;
		case 1:
			return info.getConnParameters().getConName();
		case 2:
			return info.getDbName();
		case 3:
			return info.getHostIp();
		case 4:
			return info.getPort() + "";
		case 5:
			return DatabaseType.getDatabaseTypeByID(info.getDatabaseTypeID()).getName();
		case 6:
			return info.getCharacterEncoding();
		default:
			return null;
		}
	}

	/**
	 * addListener
	 * 
	 * @param listener ILabelProviderListener
	 */
	public void addListener(ILabelProviderListener listener) {
		//do nothing
	}

	/**
	 * dispose
	 */
	public void dispose() {
		//do nothing
	}

	/**
	 * isLabelProperty
	 * 
	 * @param element Object
	 * @param property String
	 * @return boolean
	 */
	public boolean isLabelProperty(Object element, String property) {
		return false;
	}

	/**
	 * removeListener
	 * 
	 * @param listener ILabelProviderListener
	 */
	public void removeListener(ILabelProviderListener listener) {
		//do nothing
	}
}
