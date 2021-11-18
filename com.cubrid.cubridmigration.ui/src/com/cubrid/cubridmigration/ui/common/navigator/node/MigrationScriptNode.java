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
package com.cubrid.cubridmigration.ui.common.navigator.node;

import com.cubrid.common.ui.navigator.DefaultCUBRIDNode;
import com.cubrid.cubridmigration.ui.script.MigrationScript;

/**
 * MigrationScriptNode Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-4-10 created by Kevin Cao
 */
public class MigrationScriptNode extends
		DefaultCUBRIDNode {

	public MigrationScriptNode(MigrationScript script) {
		super(script.getConfigFileName(), script.getName(), "");
		setModelObj(script);
	}

	/**
	 * Migration script can be input only.
	 * 
	 * @param obj MigrationScript
	 */
	public void setModelObj(Object obj) {
		if (!(obj instanceof MigrationScript)) {
			throw new IllegalArgumentException(
					"Model object should be a migration script object.");
		}
		super.setModelObj(obj);
	}

	/**
	 * Get migration script object
	 * 
	 * @return MigrationScript
	 */
	public MigrationScript getScript() {
		return (MigrationScript) getAdapter(MigrationScript.class);
	}

	/**
	 * @return Script's name
	 */
	public String getLabel() {
		return getScript() == null ? super.getLabel() : getScript().getName();
	}

	/**
	 * @return script's file name
	 */
	public String getId() {
		return getScript() == null ? super.getId()
				: getScript().getConfigFileName();
	}

	/**
	 * If The node can be DnD.
	 * 
	 * @return true if it can.
	 */
	public boolean dndable() {
		return true;
	}
}
