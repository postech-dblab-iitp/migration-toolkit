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
package com.cubrid.cubridmigration.ui.common;

import org.eclipse.core.runtime.Assert;
import org.eclipse.core.runtime.IStatus;

import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * 
 * Status
 * 
 * @author moulinwang
 */
public class Status implements
		IStatus {
	public static final IStatus OK_STATUS = new Status();

	private String fStatusMessage;
	private int fSeverity;

	/**
	 * Creates a status set to OK (no message)
	 */
	public Status() {
		this(OK, null);
	}

	/**
	 * Creates a status .
	 * 
	 * @param severity The status severity: ERROR, WARNING, INFO and OK.
	 * @param message The message of the status. Applies only for ERROR, WARNING
	 *        and INFO.
	 */
	public Status(int severity, String message) {
		fStatusMessage = message;
		fSeverity = severity;
	}

	/**
	 * Returns if the status' severity is OK.
	 * 
	 * @return boolean
	 */
	public boolean isOK() {
		return fSeverity == IStatus.OK;
	}

	/**
	 * Returns if the status' severity is WARNING.
	 * 
	 * @return boolean
	 */
	public boolean isWarning() {
		return fSeverity == IStatus.WARNING;
	}

	/**
	 * Returns if the status' severity is INFO.
	 * 
	 * @return boolean
	 */
	public boolean isInfo() {
		return fSeverity == IStatus.INFO;
	}

	/**
	 * Returns if the status' severity is ERROR.
	 * 
	 * @return boolean
	 */
	public boolean isError() {
		return fSeverity == IStatus.ERROR;
	}

	/**
	 * @see IStatus#getMessage
	 * @return String
	 */
	public String getMessage() {
		return fStatusMessage;
	}

	/**
	 * Sets the status to ERROR.
	 * 
	 * @param errorMessage The error message (can be empty, but not null)
	 */
	public void setError(String errorMessage) {
		Assert.isNotNull(errorMessage);
		fStatusMessage = errorMessage;
		fSeverity = IStatus.ERROR;
	}

	/**
	 * Sets the status to WARNING.
	 * 
	 * @param warningMessage The warning message (can be empty, but not null)
	 */
	public void setWarning(String warningMessage) {
		Assert.isNotNull(warningMessage);
		fStatusMessage = warningMessage;
		fSeverity = IStatus.WARNING;
	}

	/**
	 * Sets the status to INFO.
	 * 
	 * @param infoMessage The info message (can be empty, but not null)
	 */
	public void setInfo(String infoMessage) {
		Assert.isNotNull(infoMessage);
		fStatusMessage = infoMessage;
		fSeverity = IStatus.INFO;
	}

	/**
	 * Sets the status to OK.
	 */
	public void setOK() {
		fStatusMessage = null;
		fSeverity = IStatus.OK;
	}

	/**
	 * @see IStatus#matches(int)
	 * @param severityMask int
	 * @return boolean
	 */
	public boolean matches(int severityMask) {
		return (fSeverity & severityMask) != 0;
	}

	/**
	 * Returns always <code>false</code>.
	 * 
	 * @see IStatus#isMultiStatus()
	 * @return boolean
	 */
	public boolean isMultiStatus() {
		return false;
	}

	/*
	 * @see IStatus#getSeverity()
	 */
	public int getSeverity() {
		return fSeverity;
	}

	/*
	 * @see IStatus#getPlugin()
	 */
	public String getPlugin() {
		return MigrationUIPlugin.PLUGIN_ID;
	}

	/**
	 * Returns always <code>null</code>.
	 * 
	 * @see IStatus#getException()
	 * @return Throwable
	 */
	public Throwable getException() {
		return null;
	}

	/**
	 * Returns always the error severity.
	 * 
	 * @see IStatus#getCode()
	 * @return int
	 */
	public int getCode() {
		return fSeverity;
	}

	/**
	 * Returns always an empty array.
	 * 
	 * @see IStatus#getChildren()
	 * @return IStatus[]
	 */
	public IStatus[] getChildren() {
		return new IStatus[0];
	}

	/**
	 * Returns a string representation of the status, suitable for debugging
	 * purposes only.
	 * 
	 * @return String
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("StatusInfo "); //$NON-NLS-1$
		if (fSeverity == OK) {
			buf.append("OK"); //$NON-NLS-1$
		} else if (fSeverity == ERROR) {
			buf.append("ERROR"); //$NON-NLS-1$
		} else if (fSeverity == WARNING) {
			buf.append("WARNING"); //$NON-NLS-1$
		} else if (fSeverity == INFO) {
			buf.append("INFO"); //$NON-NLS-1$
		} else {
			buf.append("severity="); //$NON-NLS-1$
			buf.append(fSeverity);
		}
		buf.append(": "); //$NON-NLS-1$
		buf.append(fStatusMessage);
		return buf.toString();
	}
}
