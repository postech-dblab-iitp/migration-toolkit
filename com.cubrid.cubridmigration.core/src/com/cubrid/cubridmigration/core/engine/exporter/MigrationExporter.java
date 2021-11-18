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
package com.cubrid.cubridmigration.core.engine.exporter;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.engine.ICanInterrupt;
import com.cubrid.cubridmigration.core.engine.IMigrationEventHandler;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;

/**
 * RecordExporter Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-8 created by Kevin Cao
 */
public abstract class MigrationExporter implements
		IMigrationExporter,
		ICanInterrupt {

	protected MigrationConfiguration config;
	protected IMigrationEventHandler eventHandler;
	protected boolean interrupted = false;

	public MigrationExporter() {
	}

	/**
	 * Default return schema's DDL
	 * 
	 * @param ft function
	 * @return schema's DDL
	 */
	public DBObject exportFunction(String ft) {
		if (StringUtils.isBlank(ft)) {
			return null;
		}
		String[] array = ft.split("\\.");
		String schema = null;
		String name = "";
		if (array.length == 1) {
			name = array[0];
		} else if (array.length > 1) {
			schema = array[0];
			name = array[1];
		}
		Function obj = config.getExpFunction(schema, name);
		return obj;
	}

	/**
	 * Default return schema's DDL
	 * 
	 * @param pd procedure
	 * @return schema's DDL
	 */
	public DBObject exportProcedure(String pd) {
		if (StringUtils.isBlank(pd)) {
			return null;
		}
		String[] array = pd.split("\\.");
		String schema = null;
		String name = "";
		if (array.length == 1) {
			name = array[0];
		} else if (array.length > 1) {
			schema = array[0];
			name = array[1];
		}
		Procedure obj = config.getExpProcedure(schema, name);
		return obj;
	}

	/**
	 * Default return schema's DDL
	 * 
	 * @param tg trigger
	 * @return schema's DDL
	 */
	public DBObject exportTrigger(String tg) {
		if (StringUtils.isBlank(tg)) {
			return null;
		}
		String[] array = tg.split("\\.");
		String schema = null;
		String name = "";
		if (array.length == 1) {
			name = array[0];
		} else if (array.length > 1) {
			schema = array[0];
			name = array[1];
		}
		Trigger obj = config.getExpTrigger(schema, name);
		return obj;
	}

	/**
	 * Change interrupt flag
	 */
	public void interrupt() {
		interrupted = true;
	}

	public MigrationConfiguration getConfig() {
		return config;
	}

	public void setConfig(MigrationConfiguration config) {
		this.config = config;
	}

	public IMigrationEventHandler getEventHandler() {
		return eventHandler;
	}

	public void setEventHandler(IMigrationEventHandler eventHandler) {
		this.eventHandler = eventHandler;
	}
}
