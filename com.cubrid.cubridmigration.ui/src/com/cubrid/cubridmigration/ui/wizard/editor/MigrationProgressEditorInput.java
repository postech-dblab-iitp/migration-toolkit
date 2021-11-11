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
package com.cubrid.cubridmigration.ui.wizard.editor;

import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IPersistableElement;

import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;
import com.cubrid.cubridmigration.ui.script.MigrationScript;

/**
 * MigrationCfgEditorInput Description
 * 
 * @author Kevin Cao
 */
public class MigrationProgressEditorInput implements
		IEditorInput {

	private final MigrationConfiguration config;
	private final MigrationScript migrationScript;
	private final int startMode;

	public MigrationProgressEditorInput(MigrationConfiguration config, MigrationScript migrationScript) {
		this(config, migrationScript, MigrationBriefReport.SM_USER);
	}

	public MigrationProgressEditorInput(MigrationConfiguration config, MigrationScript migrationScript,
			int startMode) {
		this.config = config;
		this.migrationScript = migrationScript;
		this.startMode = startMode;
	}

	/**
	 * 
	 * @param adapter support MigrationConfiguration and MigrationCfgEditorInput
	 * @return MigrationConfiguration and MigrationCfgEditorInput
	 */
	@SuppressWarnings("rawtypes")
	public Object getAdapter(Class adapter) {
		if (adapter.equals(MigrationConfiguration.class)) {
			return config;
		} else if (adapter.equals(MigrationProgressEditorInput.class)) {
			return this;
		} else if (adapter.equals(MigrationScript.class)) {
			return migrationScript;
		}
		return null;
	}

	/**
	 * @return false
	 */
	public boolean exists() {
		return false;
	}

	/**
	 * no image
	 * 
	 * @return null
	 */
	public ImageDescriptor getImageDescriptor() {
		return null;
	}

	/**
	 * name
	 * 
	 * @return string
	 */
	public String getName() {
		return "Migration from " + config.getSourceTypeName();
	}

	/**
	 * @return null
	 */
	public IPersistableElement getPersistable() {
		return null;
	}

	/**
	 * @return tool tip text
	 */
	public String getToolTipText() {
		return this.getName();
	}

	public int getStartMode() {
		return startMode;
	}

	/**
	 * Retrieves if the migration was started by user
	 * 
	 * @return true if by user, false if by scheduler.
	 */
	public boolean isStartedByUser() {
		return startMode == MigrationBriefReport.SM_USER;
	}

}
