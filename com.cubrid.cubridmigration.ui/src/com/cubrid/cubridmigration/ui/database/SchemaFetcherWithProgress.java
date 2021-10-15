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

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbmetadata.DBSchemaInfoFetcherFactory;
import com.cubrid.cubridmigration.core.dbmetadata.IDBSchemaInfoFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IDBSource;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * Running fetching schema of database by JDBC driver, a progress dialog is
 * shown for users
 * 
 * @author moulinwang caoyilin
 * @version 1.0 - 2010-12-8 created by moulinwang
 */
public class SchemaFetcherWithProgress implements
		IRunnableWithProgress {
	private static final Logger LOG = LogUtil.getLogger(SchemaFetcherWithProgress.class);

	protected IDBSource dbSource;
	protected Catalog catalog;
	protected boolean isFinished;
	protected Exception exception;
	protected String errorMessage;

	protected SchemaFetcherWithProgress() {
		//
	}

	/**
	 * Run method of generating catalog, this method should be called by
	 * <ProgressMonitorDialog>, other client don't call this.
	 * 
	 * @param pm IProgressMonitor
	 * @throws InvocationTargetException e
	 */
	public void run(final IProgressMonitor pm) throws InvocationTargetException {
		if (pm == null) {
			return;
		}
		isFinished = false;
		exception = null;
		catalog = null;
		errorMessage = null;
		try {
			final IDBSchemaInfoFetcher fetcher = createFetcher();
			Thread thread = startFetchingThread(fetcher, pm);

			while (!isFinished) {
				if (pm.isCanceled()) {
					thread.interrupt();
					fetcher.cancel();
					return;
				}
				ThreadUtils.threadSleep(500, null);
			}
			if (exception != null) {
				if (dbSource instanceof ConnParameters) {
					errorMessage = Messages.errConnectDatabase;
					Throwable cause = exception.getCause();
					if (cause instanceof SQLException) {
						errorMessage = Messages.errMsgLoadSchemaFailed;
					}
				} else {
					errorMessage = Messages.errInvalidMysqlDumpFile;
				}
			}
		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			pm.done();
		}
	}

	/**
	 * Create fetcher which will run in progress dialog.
	 * 
	 * @return IDBSchemaInfoFetcher
	 */
	protected IDBSchemaInfoFetcher createFetcher() {
		return DBSchemaInfoFetcherFactory.createFetcher(dbSource);
	}

	/**
	 * Initialize the progress monitor and fetching schema thread.
	 * 
	 * @param fetcher IDBSchemaInfoFetcher
	 * @param pm IProgressMonitor
	 * @return fetching thread
	 */
	protected Thread startFetchingThread(final IDBSchemaInfoFetcher fetcher,
			final IProgressMonitor pm) {
		pm.beginTask(Messages.progressMetadata, IProgressMonitor.UNKNOWN);
		Thread thread = new Thread("Cancel progress") {
			public void run() {
				try {
					catalog = fetcher.fetchSchema(dbSource, null);
				} catch (Exception ex) {
					exception = ex;
					LOG.error("", ex);
				} finally {
					isFinished = true;
				}
			}
		};
		thread.start();
		return thread;
	}

	/**
	 * return catalog with progress dialog
	 * 
	 * @return Catalog
	 */
	public Catalog fetch() {
		CompositeUtils.runMethodInProgressBar(true, true, this);
		return catalog;
	}

	/**
	 * show error message to users
	 * 
	 * @param showMess String
	 * @param detailError error detail
	 */
	protected void openErrorDialog(final String showMess, final String detailError) {
		Display display = Display.getDefault();
		display.asyncExec(new Runnable() {
			public void run() {
				DetailMessageDialog.openError(
						PlatformUI.getWorkbench().getDisplay().getActiveShell(), Messages.msgError,
						showMess, detailError);
			}
		});
	}

	/**
	 * Update the Fetcher's data source object
	 * 
	 * @param ds IDBSource to fetch schema
	 */
	protected void setDBSource(IDBSource ds) {
		this.dbSource = ds;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public Exception getError() {
		return exception;
	}

	/**
	 * Return SchemaFetcherWithProgress's instance
	 * 
	 * @param ds ConnParameters
	 * @return SchemaFetcherWithProgress
	 */
	public static SchemaFetcherWithProgress getInstance(IDBSource ds) {
		SchemaFetcherWithProgress runnable = new SchemaFetcherWithProgress();
		runnable.setDBSource(ds);
		return runnable;
	}

	/**
	 * return Catalog object, the new catalog object will be added into
	 * <CubridNodeManager> automatically
	 * 
	 * @param ds ConnParameters
	 * @return Catalog
	 */
	public static Catalog fetch(IDBSource ds) {
		SchemaFetcherWithProgress runnable = new SchemaFetcherWithProgress();
		runnable.setDBSource(ds);
		Catalog catalog = runnable.fetch();
		if (catalog == null && runnable.exception != null) {
			runnable.openErrorDialog(runnable.errorMessage, runnable.exception.getMessage());
		}
		return catalog;
	}
}
