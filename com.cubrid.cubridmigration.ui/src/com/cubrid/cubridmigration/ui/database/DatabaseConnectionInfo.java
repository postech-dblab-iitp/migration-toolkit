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

import com.cubrid.cubridmigration.core.connection.ConnParameters;

/**
 * 
 * DatabaseConnectionInfo
 * 
 * @author JessieHuang
 * @version 1.0 - 2009-12-24 created by JessieHuang
 */
public class DatabaseConnectionInfo {
	private boolean isSelected;

	private ConnParameters connParameters;

	public String getCharacterEncoding() {
		return connParameters.getCharset();
	}

	public ConnParameters getConnParameters() {
		return connParameters;
	}

	public int getDatabaseTypeID() {
		return connParameters.getDatabaseType().getID();
	}

	public String getDbName() {
		return connParameters.getDbName();
	}

	public String getDriverClass() {
		return connParameters.getDriverClass();
	}

	public String getDriverPath() {
		return connParameters.getDriverFileName();
	}

	public String getHostIp() {
		return connParameters.getHost();
	}

	public String getPassword() {
		return connParameters.getConPassword();
	}

	public int getPort() {
		return connParameters.getPort();
	}

	//	/**
	//	 * setDriverVersion
	//	 * 
	//	 * @param driverVersion String
	//	 */
	//	public void setDriverVersion(String driverVersion) {
	//		connectionParameters.setDriverVersion(driverVersion);
	//	}

	public String getUrl() {
		return connParameters.getUrl();
	}

	public String getUser() {
		return connParameters.getConUser();
	}

	public boolean isSelected() {
		return isSelected;
	}

	/**
	 * setCharacterEncoding
	 * 
	 * @param charSet String
	 */
	public void setCharacterEncoding(String charSet) {
		connParameters.setCharset(charSet);
	}

	public void setConnParameters(ConnParameters connectionParameters) {
		this.connParameters = connectionParameters;
	}

	/**
	 * setDriverPath
	 * 
	 * @param driverPath String
	 */
	public void setDriverPath(String driverPath) {
		connParameters.setDriverFileName(driverPath);
	}

	/**
	 * setPassword
	 * 
	 * @param password String
	 */
	public void setPassword(String password) {
		connParameters.setConPassword(password);
	}

	/**
	 * setPort
	 * 
	 * @param port int
	 */
	public void setPort(int port) {
		connParameters.setPort(port);
	}

	public void setSelected(boolean isSelected) {
		this.isSelected = isSelected;
	}

	/**
	 * setUser
	 * 
	 * @param user String
	 */
	public void setUser(String user) {
		connParameters.setConUser(user);
	}

}