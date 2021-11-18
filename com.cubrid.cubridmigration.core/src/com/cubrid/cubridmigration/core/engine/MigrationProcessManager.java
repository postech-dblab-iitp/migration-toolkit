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

import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.event.MigrationCanceledEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationFinishedEvent;
import com.cubrid.cubridmigration.core.engine.event.MigrationStartEvent;
import com.cubrid.cubridmigration.core.engine.exception.BreakMigrationException;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.exporter.IMigrationExporter;
import com.cubrid.cubridmigration.core.engine.exporter.impl.CUBRIDJDBCExporter;
import com.cubrid.cubridmigration.core.engine.exporter.impl.JDBCExporter;
import com.cubrid.cubridmigration.core.engine.exporter.impl.MYSQLDumpXMLExporter;
import com.cubrid.cubridmigration.core.engine.exporter.impl.PerformMYSQLXMLDataReader;
import com.cubrid.cubridmigration.core.engine.importer.IMigrationImporter;
import com.cubrid.cubridmigration.core.engine.importer.impl.JDBCImporter;
import com.cubrid.cubridmigration.core.engine.importer.impl.LoadFileImporter;
import com.cubrid.cubridmigration.core.engine.report.IMigrationReporter;
import com.cubrid.cubridmigration.core.engine.scheduler.MigrationTasksScheduler;
import com.cubrid.cubridmigration.core.engine.task.MigrationTaskFactory;

/**
 * MigrationProcessManager responses to manage migration process including start
 * and stop migration.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-3 created by Kevin Cao
 */
public class MigrationProcessManager {

	/**
	 * 
	 * MigrationBroker is call-back class to breake the migration.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-9-10 created by Kevin Cao
	 */
	private static class MigrationBroker implements
			IMigrationBroker {

		private final MigrationProcessManager mpm;

		private MigrationBroker(MigrationProcessManager mpm) {
			this.mpm = mpm;
		}

		/**
		 * Migration stopped
		 * 
		 * @param isBroken or all works were done.
		 */
		public void migrationStopped(boolean isBroken) {
			mpm.setMigrationStop(isBroken);
		}
	}

	/**
	 * MigrationMainThread Description
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-7-4 created by Kevin Cao
	 */
	private class MigrationMainThread extends
			Thread {

		MigrationMainThread() {
			setName("Migration main thread");
		}

		/**
		 * Run
		 */
		public void run() {
			final IMigrationEventHandler eventsHandler = context.getEventsHandler();
			try {
				//Initialize
				if (isRunning()) {
					//Record start time
					eventsHandler.handleEvent(new MigrationStartEvent());
					eventsHandler.handleEvent(new MigrationCanceledEvent());
					throw new BreakMigrationException("Migration canceled");
				}
				setRunning(true);
				eventsHandler.handleEvent(new MigrationStartEvent());

				MigrationTasksScheduler scheduler = buildTaskScheduler();
				scheduler.schedule();

				eventsHandler.handleEvent(new MigrationFinishedEvent(false));
			} catch (NormalMigrationException ex) {
				eventsHandler.handleEvent(new MigrationErrorEvent(ex));
				eventsHandler.handleEvent(new MigrationFinishedEvent(true));
			} catch (Throwable er) {
				eventsHandler.handleEvent(new MigrationErrorEvent(er));
			}
		}

	}

	private static boolean isRunning = false;
	private static final Object RUNNING_LOCK = new Object();

	/**
	 * MigrationProcessManager create a new process manager.
	 * 
	 * @param config MigrationConfiguration
	 * @param monitor IMigrationMonitor
	 * @param reporter IMigrationReporter
	 * 
	 * @return MigrationProcessManager
	 */
	public static MigrationProcessManager getInstance(
			MigrationConfiguration config, IMigrationMonitor monitor,
			IMigrationReporter reporter) {
		MigrationProcessManager mpm = new MigrationProcessManager();
		MigrationEventHandler eh = new MigrationEventHandler(monitor, reporter,
				new MigrationBroker(mpm));
		MigrationContext context = MigrationContext.buildContext(config, eh);
		mpm.setContext(context);
		return mpm;
	}

	/**
	 * Retrieve whether the migration process is running.
	 * 
	 * @return true if it is running.
	 */
	public static boolean isRunning() {
		synchronized (RUNNING_LOCK) {
			return isRunning;
		}
	}

	/**
	 * Set migration process status
	 * 
	 * @param running true if is running
	 */
	private static void setRunning(boolean running) {
		synchronized (RUNNING_LOCK) {
			isRunning = running;
		}
	}

	private MigrationContext context;

	private Thread mainThread;

	private final Object threadLock = new Object();

	private MigrationProcessManager() {
		//Private constructor
	}

	/**
	 * buildTaskFactory
	 * 
	 * @return MigrationTaskFactory
	 */
	private MigrationTaskFactory buildTaskFactory() {
		MigrationTaskFactory taskFactory = new MigrationTaskFactory();
		taskFactory.setContext(context);
		//Exporter
		MigrationConfiguration config = context.getConfig();
		IMigrationExporter exporter;
		if (config.sourceIsOnline()) {
			JDBCExporter exp = config.getSourceType() == MigrationConfiguration.SOURCE_TYPE_CUBRID ? new CUBRIDJDBCExporter()
					: new JDBCExporter();
			exp.setConfig(config);
			exp.setConnManager(context.getConnManager());
			exp.setEventHandler(context.getEventsHandler());
			exp.setStatusManager(context.getStatusMgr());
			exporter = exp;
		} else if (config.sourceIsXMLDump()) {
			MYSQLDumpXMLExporter exp = new MYSQLDumpXMLExporter();
			exp.setConfig(config);
			exp.setEventHandler(context.getEventsHandler());

			PerformMYSQLXMLDataReader handler = new PerformMYSQLXMLDataReader();
			handler.setConfig(config);
			handler.setExecutor(context.getExportRecExe());
			handler.setStatusManager(context.getStatusMgr());
			exp.setHandler(handler);
			exporter = exp;
		} else {
			exporter = null;
		}
		taskFactory.setExporter(exporter);
		//Importer
		IMigrationImporter importer;
		if (config.targetIsFile()) {
			importer = new LoadFileImporter(context);
		} else if (config.targetIsOnline()) {
			importer = new JDBCImporter(context);
		} else {
			//importer = new LoadDBImporter(mrManager);
			throw new BreakMigrationException(
					"Offline migration is not supported any more.");
		}
		taskFactory.setImporter(importer);
		return taskFactory;
	}

	/**
	 * createTaskScheduler
	 * 
	 * @return MigrationTasksScheduler
	 */
	private MigrationTasksScheduler buildTaskScheduler() {
		MigrationTaskFactory taskFactory = buildTaskFactory();
		MigrationTasksScheduler scheduler = new MigrationTasksScheduler();
		scheduler.setTaskFactory(taskFactory);
		scheduler.setContext(context);
		return scheduler;
	}

	/**
	 * Interrupt the migration process by Users. It should be called in a
	 * progress dialog.
	 */
	public void interruptMigration() {
		context.getEventsHandler().handleEvent(new MigrationFinishedEvent(true));
		//waiting for stopping.
		while (mainThread != null) {
			ThreadUtils.threadSleep(1000, null);
		}
	}

	/**
	 * It should be called by object factory
	 * 
	 * @param context MigrationContext
	 */
	protected void setContext(MigrationContext context) {
		this.context = context;
	}

	/**
	 * Stop migration process and release resources.
	 * 
	 * @param isBroken true if migration is broken.
	 */
	private void setMigrationStop(boolean isBroken) {
		setRunning(false);
		synchronized (threadLock) {
			if (mainThread == null) {
				return;
			}
			try {
				context.dispose(isBroken);
				mainThread.interrupt();
			} finally {
				mainThread = null;
			}
		}
	}

	/**
	 * Start migration process.
	 * 
	 */
	public void startMigration() {
		synchronized (threadLock) {
			if (mainThread != null) {
				return;
			}
			mainThread = new MigrationMainThread();
			mainThread.start();
		}
	}
}