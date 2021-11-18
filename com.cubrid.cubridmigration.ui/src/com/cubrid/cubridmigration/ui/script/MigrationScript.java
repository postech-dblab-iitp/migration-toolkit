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

import java.io.File;
import java.io.Serializable;

import com.cubrid.cubridmigration.core.common.PathUtils;

/**
 * Migration Script including migration task reservation information.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-6-19 created by Kevin Cao
 */
public class MigrationScript implements
		Serializable,
		Cloneable {

	private static final long serialVersionUID = -795068213100644440L;
	private String name;
	private String parent;
	private String configFileName;
	private String cronPatten;
	//0:running only once mode; 1:repeating mode; 2:advanced mode.
	private int cronMode = 0;

	private String reservationID;

	/**
	 * @return clone object
	 * @throws CloneNotSupportedException if errors
	 */
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public String getAbstractConfigFileName() {
		return PathUtils.getScriptDir() + File.separatorChar + configFileName;
	}

	public String getConfigFileName() {
		return configFileName;
	}

	public int getCronMode() {
		return cronMode;
	}

	public String getCronPatten() {
		return cronPatten;
	}

	public String getName() {
		return name;
	}

	public String getParent() {
		return parent;
	}

	public void setConfigFileName(String configFileName) {
		this.configFileName = configFileName;
	}

	public void setCronMode(int cronMode) {
		this.cronMode = cronMode;
	}

	public void setCronPatten(String cronPatten) {
		this.cronPatten = cronPatten;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

	public String getReservationID() {
		return reservationID;
	}

	public void setReservationID(String reservationID) {
		this.reservationID = reservationID;
	}
}
