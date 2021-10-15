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
package com.cubrid.cubridmigration.core.engine.importer;

import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.engine.IMigrationEventHandler;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.event.CreateObjectEvent;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;

/**
 * Importer responses to execute importing commands.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-3 created by Kevin Cao
 */
public abstract class Importer implements
		IMigrationImporter {
	protected static final String ERROR_RECORD_MSG = "Invalid record and please check the log for more information.";

	protected final IMigrationEventHandler eventHandler;
	protected final MigrationContext mrManager;

	public Importer(MigrationContext mrManager) {
		this.mrManager = mrManager;
		this.eventHandler = mrManager.getEventsHandler();
	}

	/**
	 * Should be called when create database objects successfully;
	 * 
	 * @param obj database object
	 */
	protected void createObjectSuccess(DBObject obj) {
		eventHandler.handleEvent(new CreateObjectEvent(obj));
	}

	/**
	 * Should be called when create database objects failed;
	 * 
	 * @param obj database object
	 * @param error the exception
	 */
	protected void createObjectFailed(DBObject obj, Throwable error) {
		eventHandler.handleEvent(new CreateObjectEvent(obj, error));
	}

	/**
	 * Not support default
	 * 
	 * @param function Function
	 */
	public void createFunction(Function function) {
		throw new NormalMigrationException(
				"Function migration is not supported.");
	}

	/**
	 * Not support default
	 * 
	 * @param procedure Procedure
	 */
	public void createProcedure(Procedure procedure) {
		throw new NormalMigrationException(
				"Procedure migration is not supported.");

	}

	/**
	 * Not support default
	 * 
	 * @param trigger Trigger
	 */
	public void createTriggers(Trigger trigger) {
		throw new NormalMigrationException(
				"Trigger migration is not supported.");

	}
}
