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

import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskExecutionContext;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;
import com.cubrid.cubridmigration.core.engine.template.MigrationTemplateParser;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizardFactory;
import com.cubrid.cubridmigration.ui.wizard.editor.MigrationProgressEditorInput;

/**
 * Migration Reservation Manager
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-12-10 created by Kevin Cao
 */
public class MigrationScriptSchedulerManager {

	private static final Logger LOG = LogUtil.getLogger(MigrationScriptSchedulerManager.class);

	private static Scheduler scheduler = new Scheduler();
	private static List<String> tasks = new ArrayList<String>();

	/**
	 * Add a migration script to reservation.Only one script will work.
	 * 
	 * @param script MigrationScript
	 */
	public static void addReservation(MigrationScript script) {
		//cancelAll();
		RunScriptTask task = new RunScriptTask(script);
		String id = scheduler.schedule(script.getCronPatten(), task);
		tasks.add(id);
		script.setReservationID(id);
		if (!scheduler.isStarted()) {
			scheduler.start();
		}
	}

	//	/**
	//	 * Cancel all scheduled migration task.
	//	 * 
	//	 */
	//	public static void cancelAll() {
	//		for (String task : tasks) {
	//			scheduler.deschedule(task);
	//		}
	//		tasks.clear();
	//	}

	/**
	 * RunScriptTask will execute scheduled migration task
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-12-10 created by Kevin Cao
	 */
	private static class RunScriptTask extends
			Task {
		private final MigrationScript script;

		RunScriptTask(MigrationScript script) {
			this.script = script;
		}

		/**
		 * Execute migration reservation
		 * 
		 * @param arg0 TaskExecutionContext
		 */
		public void execute(TaskExecutionContext arg0) {
			Display.getDefault().asyncExec(new Runnable() {

				public void run() {
					try {
						MigrationScriptManager.getInstance().save();
						//Parsing configuration
						MigrationConfiguration config = MigrationTemplateParser.parse(script.getAbstractConfigFileName());
						//Keeping configuration name as same as the script name
						config.setName(script.getName());
						//Building source DB schema
						Catalog catalog = config.buildRequiredSourceSchema();
						if (catalog != null) {
							config.setSrcCatalog(catalog, false);
						}
						//Start migration
						IWorkbench workbench = PlatformUI.getWorkbench();
						IWorkbenchPage activePage = workbench.getActiveWorkbenchWindow().getActivePage();
						MigrationProgressEditorInput progressEditorInput = new MigrationProgressEditorInput(
								config, script, MigrationBriefReport.SM_RESERVATION);

						String epID = MigrationWizardFactory.getProgressEditorPartID(config.getSourceType(), config.getDestType());
						activePage.openEditor(progressEditorInput, epID);
					} catch (Exception ex) {
						LOG.error("Migration task error.", ex);
					}
				}
			});
		}
	}

	/**
	 * Cancel scheduled script.
	 * 
	 * @param script MigrationScript
	 */
	public static void cancel(MigrationScript script) {
		scheduler.deschedule(script.getReservationID());
		tasks.remove(script.getReservationID());
		script.setReservationID("");
	}
}
