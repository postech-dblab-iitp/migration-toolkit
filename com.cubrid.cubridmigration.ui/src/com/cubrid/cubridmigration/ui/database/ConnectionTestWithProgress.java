package com.cubrid.cubridmigration.ui.database;

import java.awt.Component;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;

import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;

public class ConnectionTestWithProgress implements IRunnableWithProgress {
	
	protected Connection conn;
	protected MigrationWizard wizard;
	protected MigrationConfiguration config;
	protected Exception exception;
	protected boolean isFinished;
	protected String errorMessage;
	protected boolean isSuccess = false;
	
	public ConnectionTestWithProgress(MigrationConfiguration config) {
		this.config = config;
	}
	
	@Override
	public void run(IProgressMonitor monitor) throws InvocationTargetException,
			InterruptedException {
		// TODO Auto-generated method stub
		try {
			isFinished = false;
			exception = null;
			conn = null;
			errorMessage = null;
			startTestThread(monitor);
			
			while(!isFinished) {
				if (monitor.isCanceled()) {
					isSuccess = false;
					return;
				}
			}
			
			if (exception == null) {
				isSuccess = true;
				return;
			}
			
		} catch (Exception e) {
			errorMessage = Messages.errConnectDatabase;
			isSuccess = false;
		} finally {
			monitor.done();
		}
	}
	
	protected void startTestThread(IProgressMonitor monitor) {
		monitor.beginTask(Messages.progressMetadata, IProgressMonitor.UNKNOWN);
		try {
			conn = config.getTargetConParams().createConnection();
			conn.close();
		} catch (Exception e) {
			exception = e;
		} finally {
			isFinished = true;
		}
	}
	
	public boolean launch() {
		CompositeUtils.runMethodInProgressBar(true, true, this);
		return isSuccess;
	}
}
