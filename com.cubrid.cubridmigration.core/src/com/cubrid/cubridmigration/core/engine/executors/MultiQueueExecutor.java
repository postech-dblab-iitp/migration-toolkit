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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * MultiQueueExecutor will execute the tasks in a thread pool which only has one
 * thread.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-4 created by Kevin Cao
 */
public class MultiQueueExecutor implements
		IRunnableExecutor {

	private final Map<Long, IRunnableExecutor> executors = Collections.synchronizedMap(new HashMap<Long, IRunnableExecutor>());

	private boolean interrupted;

	private final boolean limitTaskCount;

	private long totalTask = 0;

	private Object lockObj = new Object();

	private int executorPoolSize;

	public MultiQueueExecutor(int poolSize, boolean limitTaskCount) {
		this.limitTaskCount = limitTaskCount;
		this.executorPoolSize = poolSize;
	}

	/**
	 * Add task to scheduler.
	 * 
	 * @param tk is the migration task to be executed.
	 */
	public void execute(Runnable tk) {
		IRunnableExecutor es = getExecutor();
		if (interrupted) {
			throw new RuntimeException("Interrupted");
		}
		totalTask++;
		es.execute((Runnable) tk);
	}

	/**
	 * Retrieves executor of current thread
	 * 
	 * @return IRunnableExecutor
	 */
	private IRunnableExecutor getExecutor() {
		synchronized (lockObj) {
			final long id = Thread.currentThread().getId();
			IRunnableExecutor es = executors.get(id);
			if (es == null) {
				es = new SingleQueueExecutor(executorPoolSize, limitTaskCount);
				executors.put(id, es);
			}
			return es;
		}
	}

	/**
	 * Release thread pools
	 */
	public void dispose() {
		for (IRunnableExecutor et : executors.values()) {
			et.dispose();
		}
	}

	/**
	 * Release thread pools
	 */
	public void interrupt() {
		interrupted = true;
		for (IRunnableExecutor et : executors.values()) {
			et.interrupt();
		}
	}

	/**
	 * @return true if executors are busy.
	 */
	public boolean isBusy() {
		if (interrupted) {
			return false;
		}
		for (IRunnableExecutor et : executors.values()) {
			if (et.isBusy()) {
				return true;
			}
		}
		return false;
	}
}
