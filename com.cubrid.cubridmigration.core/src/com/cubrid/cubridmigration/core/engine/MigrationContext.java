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

import java.util.ArrayList;
import java.util.List;

import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.executors.IRunnableExecutor;
import com.cubrid.cubridmigration.core.engine.executors.ImmediateExecutor;
import com.cubrid.cubridmigration.core.engine.executors.MultiQueueExecutor;
import com.cubrid.cubridmigration.core.engine.executors.SingleQueueExecutor;
import com.cubrid.cubridmigration.cubrid.stmt.CUBRIDParameterSetter;

/**
 * MigrationResourceManager responses to manage the resources used by migration
 * process.
 * 
 * Such as configuration,connection manager, task executors, utilities and etc.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-6-6 created by Kevin Cao
 */
public class MigrationContext {

	private final List<IRunnableExecutor> executors = new ArrayList<IRunnableExecutor>();
	//private final Map<String, IRunnableExecutor> mergeDataFileExe = new HashMap<String, IRunnableExecutor>();
	private final List<ICanDispose> tobeDisposed = new ArrayList<ICanDispose>();
	private final MigrationConfiguration config;
	private final IMigrationEventHandler eventsHandler;

	private IRunnableExecutor mergeTaskExe;
	private IRunnableExecutor dbObjectExe;
	private IRunnableExecutor exportRecExe;
	private IRunnableExecutor importRecordExecutor;
	private CUBRIDParameterSetter paramSetter;
	private JDBCConManager connManager;
	private MigrationStatusManager statusMgr;
	private MigrationDirAndFilesManager dirAndFilesMgr;

	private MigrationContext(MigrationConfiguration config, IMigrationEventHandler eventsHandler) {
		this.config = config;
		this.eventsHandler = eventsHandler;
		addTobeDisposed(eventsHandler);
	}

	/**
	 * Build migration context.
	 * 
	 * @param config MigrationConfiguration
	 * @param eventsHandler IMigrationEventHandler
	 * @return MigrationContext
	 */
	public static MigrationContext buildContext(MigrationConfiguration config,
			IMigrationEventHandler eventsHandler) {
		final MigrationContext context = new MigrationContext(config, eventsHandler);

		context.setParamSetter(new CUBRIDParameterSetter(config));

		context.setConnManager(new JDBCConManager(config));

		final MigrationStatusManager msm = new MigrationStatusManager();
		msm.setHasOOMRisk(config.checkOOMRisk());
		//Adjust OOM control parameters
		final long maxMemory = Runtime.getRuntime().maxMemory() * 95 / 100;
		msm.setMaxMemory(maxMemory);
		msm.setWarningFreeMemory(maxMemory / 2);
		msm.setWarningCommitCount(Math.max(config.getCommitCount() / 10, 500));
		msm.setAlertFreeMemory(maxMemory / 5);
		msm.setAlertCommitCount(Math.max(config.getCommitCount() / 100, 100));
		context.setStatusMgr(msm);

		context.setExportRecExe(new SingleQueueExecutor(config.getExportThreadCount(), true));
		context.setImportRecordExecutor(new MultiQueueExecutor(config.getImportThreadCount(), true));

		context.setMergeTaskExe(new SingleQueueExecutor(1, false));

		context.setDbObjectExe(new ImmediateExecutor());

		MigrationDirAndFilesManager dirAndFilesMgr = new MigrationDirAndFilesManager(config);
		dirAndFilesMgr.initialize();
		context.setDirAndFilesMgr(dirAndFilesMgr);
		return context;
	}

	/**
	 * Add a resource should be disposed by resource manager
	 * 
	 * @param cd is a resource which will be disposed by this manager
	 */
	public void addTobeDisposed(ICanDispose cd) {
		if (!tobeDisposed.contains(cd)) {
			tobeDisposed.add(cd);
			if (cd instanceof IRunnableExecutor) {
				executors.add((IRunnableExecutor) cd);
			}
		}
	}

	/**
	 * Release resources
	 * 
	 * @param isBroken if true means the migration is stopped by exception or
	 *        user
	 */
	public void dispose(boolean isBroken) {

		for (ICanDispose cd : tobeDisposed) {
			if (isBroken && (cd instanceof ICanInterrupt)) {
				((ICanInterrupt) cd).interrupt();
			} else {
				cd.dispose();
			}
		}
	}

	public IRunnableExecutor getMergeTaskExe() {
		return mergeTaskExe;
	}

	public MigrationConfiguration getConfig() {
		return config;
	}

	public JDBCConManager getConnManager() {
		return connManager;
	}

	public IRunnableExecutor getDbObjectExe() {
		return dbObjectExe;
	}

	public MigrationDirAndFilesManager getDirAndFilesMgr() {
		return dirAndFilesMgr;
	}

	public IMigrationEventHandler getEventsHandler() {
		return eventsHandler;
	}

	public IRunnableExecutor getExportRecExe() {
		return exportRecExe;
	}

	public IRunnableExecutor getImportRecordExecutor() {
		return importRecordExecutor;
	}

	public CUBRIDParameterSetter getParamSetter() {
		return paramSetter;
	}

	public MigrationStatusManager getStatusMgr() {
		return statusMgr;
	}

	/**
	 * 
	 * Retrieves whether the executors are busy now.
	 * 
	 * @return true means busy now.
	 */
	public boolean isExecutorsBusy() {
		for (IRunnableExecutor re : executors) {
			if (re.isBusy()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Set cm task service
	 * 
	 * @param cmTaskService IRunnableExecutor
	 */
	protected void setMergeTaskExe(IRunnableExecutor cmTaskService) {
		this.mergeTaskExe = cmTaskService;
		addTobeDisposed(cmTaskService);
	}

	/**
	 * Set DB objects executor.
	 * 
	 * @param dbObjectExecutor IRunnableExecutor
	 */
	protected void setDbObjectExe(IRunnableExecutor dbObjectExecutor) {
		this.dbObjectExe = dbObjectExecutor;
	}

	/**
	 * Set export records executor
	 * 
	 * @param exportRecordsExecutor IRunnableExecutor
	 */
	protected void setExportRecExe(IRunnableExecutor exportRecordsExecutor) {
		this.exportRecExe = exportRecordsExecutor;
		addTobeDisposed(exportRecordsExecutor);
	}

	/**
	 * Set importing records executor 2
	 * 
	 * @param impRecExecutor2 IRunnableExecutor
	 */
	protected void setImportRecordExecutor(IRunnableExecutor impRecExecutor2) {
		this.importRecordExecutor = impRecExecutor2;
		addTobeDisposed(impRecExecutor2);
	}

	protected void setParamSetter(CUBRIDParameterSetter parameterSetter) {
		this.paramSetter = parameterSetter;

	}

	/**
	 * Set JDBC connection manager
	 * 
	 * @param connectionManager JDBCConManager
	 */
	protected void setConnManager(JDBCConManager connectionManager) {
		this.connManager = connectionManager;
		addTobeDisposed(connectionManager);

	}

	/**
	 * Set status manager
	 * 
	 * @param statusManager MigrationStatusManager
	 */
	protected void setStatusMgr(MigrationStatusManager statusManager) {
		this.statusMgr = statusManager;

	}

	/**
	 * Set dir and file manager
	 * 
	 * @param dirAndFilesMgr MigrationDirAndFilesManager
	 */
	protected void setDirAndFilesMgr(MigrationDirAndFilesManager dirAndFilesMgr) {
		this.dirAndFilesMgr = dirAndFilesMgr;
		addTobeDisposed(dirAndFilesMgr);
	}

}
