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
package com.cubrid.cubridmigration.core.engine;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.event.IMigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationCanceledEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationFinishedEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationStartEvent;
import com.cubrid.cubridmigration.core.engine.event.SingleRecordErrorEvent;
import com.cubrid.cubridmigration.core.engine.executors.IRunnableExecutor;
import com.cubrid.cubridmigration.core.engine.executors.SingleQueueExecutor;
import com.cubrid.cubridmigration.core.engine.report.IMigrationReporter;

/**
 * MigrationEventHandler responses to handle the events and errors of migration
 * process.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-3 created by Kevin Cao
 */
public class MigrationEventHandler implements
		IMigrationEventHandler {

	private static final Logger LOG = LogUtil.getLogger(MigrationEventHandler.class);
	private final IMigrationMonitor monitor;
	private final IMigrationReporter reporter;
	private final IMigrationBroker breaker;
	private final IRunnableExecutor handlerExecutor = new SingleQueueExecutor(
			1, false);
	private MigrationFinishedEvent mfe = null;

	/**
	 * Constructor
	 * 
	 * @param migrationMonitor IMigrationMonitor
	 * @param migraionReporter IMigrationReporter
	 * @param breaker IMigrationProcessManager
	 */
	public MigrationEventHandler(IMigrationMonitor migrationMonitor,
			IMigrationReporter migraionReporter, IMigrationBroker breaker) {
		this.monitor = migrationMonitor;
		this.reporter = migraionReporter;
		this.breaker = breaker;
	}

	/**
	 * Add event to handle list.
	 * 
	 * @param event MigrationEvent
	 */
	public void handleEvent(final MigrationEvent event) {
		handlerExecutor.execute(new EventHandlerRunnable(event));
	}

	/**
	 * Dispose and release resources
	 */
	public void dispose() {
		monitor.finished();
		reporter.finished();
		handlerExecutor.dispose();
	}

	/**
	 * EventHandlerRunnable responses to handle events.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2011-8-11 created by Kevin Cao
	 */
	protected class EventHandlerRunnable implements
			Runnable {

		private final MigrationEvent event;

		public EventHandlerRunnable(MigrationEvent event) {
			this.event = event;
		}

		/**
		 * Handle events.
		 */
		public void run() {
			try {
				//After finished event, new event will not be accepted.
				if (mfe != null) {
					LOG.info(event);
					return;
				}
				if (event instanceof MigrationCanceledEvent) {
					reporter.addEvent(event);
					return;
				}
				if (event instanceof IMigrationErrorEvent) {
					IMigrationErrorEvent evt = (IMigrationErrorEvent) event;
					//Details of errors will be written into LOG files. 
					if (evt.getError() != null) {
						LOG.error("", evt.getError());
					}
				}
				if (event instanceof MigrationErrorEvent) {
					MigrationErrorEvent ee = (MigrationErrorEvent) event;
					monitor.addEvent(event);
					reporter.addEvent(event);
					if (ee.isFatalError()) {
						handleEvent(new MigrationFinishedEvent(true));
					}
					return;
				}
				if (event instanceof MigrationFinishedEvent) {
					//Only receives the first MigrationFinishedEvent.
					mfe = (MigrationFinishedEvent) event;
					monitor.addEvent(event);
					reporter.addEvent(event);
					breaker.migrationStopped(mfe.isBroken());
					return;
				}
				if (event instanceof MigrationStartEvent) {
					monitor.start();
					mfe = null;
					monitor.addEvent(event);
					reporter.addEvent(event);
					return;
				}
				//Single record error doesn't be sent to monitor
				if (!(event instanceof SingleRecordErrorEvent)) {
					monitor.addEvent(event);
				}
				reporter.addEvent(event);
			} catch (Throwable ex) {
				LOG.error("", ex);
			}
		}
	}
}
