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
package com.cubrid.cubridmigration.core.engine.executors;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.cubrid.cubridmigration.core.engine.ThreadUtils;

/**
 * SingleQueueExecutor: tasks will be pushed into a single queue waiting for
 * dispatching
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-4 created by Kevin Cao
 */
public class SingleQueueExecutor implements
		IRunnableExecutor {

	protected final ThreadPoolExecutor executor;

	protected int poolSize;

	protected final boolean limitTaskCount;

	protected long taskCount;

	protected Object lockObj = new Object();

	protected boolean interrupted;

	public SingleQueueExecutor(int threadSize, boolean limitTaskCount) {
		poolSize = threadSize + Math.max(2, threadSize / 2);
		this.limitTaskCount = limitTaskCount;
		executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadSize);
	}

	/**
	 * Release threads pool.
	 * 
	 */
	public void dispose() {
		executor.shutdown();
	}

	/**
	 * Add task to scheduler.
	 * 
	 * @param tk is the migration task to be executed.
	 */
	public void execute(Runnable tk) {
		synchronized (lockObj) {
			taskCount++;
			while (limitTaskCount && tooManyTasks()) {
				if (interrupted) {
					throw new RuntimeException("Interrupted.");
				}
				ThreadUtils.threadSleep(200, null);
			}
			if (interrupted) {
				throw new RuntimeException("Interrupted.");
			}
			executor.execute(tk);
		}

	}

	/**
	 * True if there are too many tasks in queue.
	 * 
	 * @return True if there are too many tasks in queue.
	 */
	protected boolean tooManyTasks() {
		return (taskCount - executor.getCompletedTaskCount()) > poolSize;
	}

	/**
	 * Interrupted
	 */
	public void interrupt() {
		interrupted = true;
		executor.shutdownNow();
	}

	/**
	 * The executor is busy
	 * 
	 * @return true if has tasks not executed
	 */
	public boolean isBusy() {
		if (interrupted) {
			return false;
		}
		return ((taskCount - executor.getCompletedTaskCount()) != 0);
	}
}
