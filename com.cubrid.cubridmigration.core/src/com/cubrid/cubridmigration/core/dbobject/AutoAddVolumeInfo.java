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
package com.cubrid.cubridmigration.core.dbobject;

/**
 * A java bean model that implements IModel
 * 
 * @author lizhiqiang 2009-4-14
 */
public class AutoAddVolumeInfo {
	private String dbname;

	private boolean autoAddDataVolume;
	private String data_warn_outofspace;
	private String data_ext_page;

	private boolean autoAddIndexVolume;
	private String index_warn_outofspace;
	private String index_ext_page;

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public boolean isAutoAddDataVolume() {
		return autoAddDataVolume;
	}

	public void setAutoAddDataVolume(boolean autoAddDataVolume) {
		this.autoAddDataVolume = autoAddDataVolume;
	}

	public String getData_warn_outofspace() {
		return data_warn_outofspace;
	}

	public void setData_warn_outofspace(String dataWarnOutofspace) {
		this.data_warn_outofspace = dataWarnOutofspace;
	}

	public String getData_ext_page() {
		return data_ext_page;
	}

	public void setData_ext_page(String dataExtPage) {
		this.data_ext_page = dataExtPage;
	}

	public boolean isAutoAddIndexVolume() {
		return autoAddIndexVolume;
	}

	public void setAutoAddIndexVolume(boolean autoAddIndexVolume) {
		this.autoAddIndexVolume = autoAddIndexVolume;
	}

	public String getIndex_warn_outofspace() {
		return index_warn_outofspace;
	}

	public void setIndex_warn_outofspace(String indexWarnOutofspace) {
		this.index_warn_outofspace = indexWarnOutofspace;
	}

	public String getIndex_ext_page() {
		return index_ext_page;
	}

	public void setIndex_ext_page(String indexExtPage) {
		this.index_ext_page = indexExtPage;
	}

}
