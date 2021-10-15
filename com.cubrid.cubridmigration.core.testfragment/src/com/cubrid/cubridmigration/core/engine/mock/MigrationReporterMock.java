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
package com.cubrid.cubridmigration.core.engine.mock;

import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.event.CreateObjectEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.core.engine.event.SingleRecordErrorEvent;
import com.cubrid.cubridmigration.core.engine.report.DefaultMigrationReporter;

/**
 * 
 * MigrationReporterMock is used for running test cases.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-11 created by Kevin Cao
 */
public class MigrationReporterMock extends
		DefaultMigrationReporter {

	public MigrationReporterMock(MigrationConfiguration config, int startMode) {
		super(config, startMode);
	}

	private boolean error = false;

	public void addEvent(MigrationEvent event) {
		super.addEvent(event);
		if (event instanceof MigrationErrorEvent) {
			error = true;
		} else if (event instanceof CreateObjectEvent) {
			final boolean success = ((CreateObjectEvent) event).isSuccess();
			if (!success) {
				error = true;
			}
		} else if (event instanceof ImportRecordsEvent
				&& !((ImportRecordsEvent) event).isSuccess()) {
			error = true;
		} else if (event instanceof SingleRecordErrorEvent) {
			error = true;
		}
	}

	public boolean hasError() {
		return this.error;
	}

	@Override
	protected void getSummary() {

	}
}
