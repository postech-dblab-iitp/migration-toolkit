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
package com.cubrid.cubridmigration.ui.script.handler;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.TreeViewer;

import com.cubrid.cubridmigration.ui.common.navigator.node.MigrationScriptNode;
import com.cubrid.cubridmigration.ui.script.MigrationScript;
import com.cubrid.cubridmigration.ui.script.MigrationScriptManager;
import com.cubrid.cubridmigration.ui.script.MigrationScriptSchedulerManager;

/**
 * Action to cancel migration reservation.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-6-29 created by Kevin Cao
 */
public class CancelReservationHandler extends
		BaseMigrationScriptHandler {

	/**
	 * Cancel schedule
	 * 
	 * @param scripts List<MigrationScript>
	 */
	protected void handlerScripts(List<MigrationScript> scripts) {
		for (MigrationScript obj : scripts) {
			obj.setCronPatten(null);
			MigrationScriptSchedulerManager.cancel(obj);
		}
		MigrationScriptManager.getInstance().save();
		TreeViewer tv = (TreeViewer) getSelectionProvider();
		tv.refresh();
	}

	/**
	 * If the selected object is null or not a migration script object, the
	 * action should be disabled.
	 * 
	 * @return true if it supports the selected object.
	 */
	public boolean isEnabled() {
		IStructuredSelection selection = getSelection();
		if (selection.isEmpty()) {
			return false;
		}
		Iterator<?> iterator = selection.iterator();
		while (iterator.hasNext()) {
			Object sel = iterator.next();
			if (!(sel instanceof MigrationScriptNode)) {
				return false;
			}
			final MigrationScript script = ((MigrationScriptNode) sel).getScript();
			if (StringUtils.isBlank(script.getCronPatten())) {
				return false;
			}
		}
		return true;
	}
}
