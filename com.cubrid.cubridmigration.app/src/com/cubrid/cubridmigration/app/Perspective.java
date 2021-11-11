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
package com.cubrid.cubridmigration.app;

import org.eclipse.ui.IFolderLayout;
import org.eclipse.ui.IPageLayout;
import org.eclipse.ui.IPerspectiveFactory;
import org.eclipse.ui.IViewLayout;

import com.cubrid.cubridmigration.ui.script.MigrationScriptExplorerView;

/**
 * 
 * Perspective
 * 
 * @author moulinwang
 * @version 1.0 - 2009-10-13
 */
public class Perspective implements
		IPerspectiveFactory {
	// pespective ID
	public static final String ID = "com.cubrid.cubridmigration.app.perspective";

	/**
	 * createInitialLayout
	 * 
	 * @param layout IPageLayout
	 */
	public void createInitialLayout(IPageLayout layout) {
		layout.setEditorAreaVisible(true);

		IFolderLayout navigatorFolder = layout.createFolder("NavigatorFolder",
				IPageLayout.LEFT, 0.35f, IPageLayout.ID_EDITOR_AREA);
		navigatorFolder.addView(MigrationScriptExplorerView.ID);
		//navigatorFolder.addView(DatabaseExplorerView.ID);

		IViewLayout viewLayout = layout.getViewLayout(MigrationScriptExplorerView.ID);
		viewLayout.setCloseable(false);
		viewLayout.setMoveable(false);

		//		viewLayout = layout.getViewLayout(DatabaseExplorerView.ID);
		//		viewLayout.setCloseable(true);
		//		viewLayout.setMoveable(false);

	}
}
