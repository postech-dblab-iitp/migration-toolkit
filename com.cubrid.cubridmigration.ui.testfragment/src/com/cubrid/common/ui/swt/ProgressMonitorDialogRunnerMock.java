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
package com.cubrid.common.ui.swt;

import org.eclipse.jface.operation.IRunnableWithProgress;

/**
 * ProgressMonitorDialogRunner to run a runnable in progress dialog.
 * 
 * @author Kevin Cao
 * 
 */
public class ProgressMonitorDialogRunnerMock extends
		ProgressMonitorDialogRunner {

	/**
	 * 
	 * This implementation of IRunnableContext#run(boolean, boolean,
	 * IRunnableWithProgress) runs the given <code>IRunnableWithProgress</code>
	 * using the progress monitor for this progress dialog and blocks until the
	 * runnable has been run, regardless of the value of <code>fork</code>. The
	 * dialog is opened before the runnable is run, and closed after it
	 * completes. It is recommended that <code>fork</code> is set to true in
	 * most cases. If <code>fork</code> is set to <code>false</code>, the
	 * runnable will run in the UI thread and it is the runnable's
	 * responsibility to call <code>Display.readAndDispatch()</code> to ensure
	 * UI responsiveness.
	 * 
	 * @param fork boolean
	 * @param cancelable boolean
	 * @param runnable IRunnableWithProgress
	 */
	public void run(boolean fork, boolean cancelable, IRunnableWithProgress runnable) {
		try {
			runnable.run(new ProgressMonitorMock());
		} catch (Exception ex) {
			throw new RuntimeException("Run with progress error.", ex);
		}
	}
}
