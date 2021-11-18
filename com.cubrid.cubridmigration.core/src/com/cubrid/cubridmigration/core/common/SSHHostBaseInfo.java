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
package com.cubrid.cubridmigration.core.common;

/**
 * @author Kevin Cao
 * 
 */
public class SSHHostBaseInfo {

	private String host;
	private int port;
	private String user;
	private int authType;
	private String privateKeyAbsoluteFile;
	private String password;
	private String krbConfig;
	private String krbTicket;

	public int getAuthType() {
		return authType;
	}

	public String getHost() {
		return host;
	}

	public String getKrbConfig() {
		return krbConfig;
	}

	public String getKrbTicket() {
		return krbTicket;
	}

	public String getPassword() {
		return password;
	}

	public int getPort() {
		return port;
	}

	public String getPrivateKeyAbsoluteFile() {
		return privateKeyAbsoluteFile;
	}

	public String getUser() {
		return user;
	}

	public void setAuthType(int authType) {
		this.authType = authType;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setKrbConfig(String krbConfig) {
		this.krbConfig = krbConfig;
	}

	public void setKrbTicket(String krbTicket) {
		this.krbTicket = krbTicket;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setPrivateKeyAbsoluteFile(String publicKey) {
		this.privateKeyAbsoluteFile = publicKey;
	}

	public void setUser(String user) {
		this.user = user;
	}
}
