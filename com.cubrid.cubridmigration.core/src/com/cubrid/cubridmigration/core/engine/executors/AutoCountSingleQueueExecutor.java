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

import com.cubrid.cubridmigration.core.common.CommonUtils;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;

/**
 * AutoCountSingleQueueExecutor will automatically change thread pool's size to
 * get the best performance.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-1-29 created by Kevin Cao
 */
public class AutoCountSingleQueueExecutor extends
		SingleQueueExecutor {

	private final static int MAX_THREAD = 40;
	private long lastAdjustTime = System.currentTimeMillis();

	public AutoCountSingleQueueExecutor(int threadSize, boolean limitTaskCount) {
		super(threadSize, limitTaskCount);
		executor.setMaximumPoolSize(MAX_THREAD);
	}

	/**
	 * Add task to scheduler. Executor's thread pool size will be adjusted
	 * automatically. The pool size will be increased every minute and decreased
	 * every 3 minutes.
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
				if ((executor.getPoolSize() >= MAX_THREAD)
						|| CommonUtils.oomWarning()) {
					ThreadUtils.threadSleep(200, null);
				} else if (System.currentTimeMillis() - lastAdjustTime >= 60000) {
					lastAdjustTime = System.currentTimeMillis();
					poolSize++;
					executor.setCorePoolSize(executor.getPoolSize() + 1);
				} else {
					ThreadUtils.threadSleep(200, null);
				}
			}
			if (interrupted) {
				throw new RuntimeException("Interrupted.");
			}
			executor.execute(tk);

			if (System.currentTimeMillis() - lastAdjustTime <= 180000) {
				return;
			}
			long idleCount = executor.getCompletedTaskCount()
					+ executor.getPoolSize() - taskCount;
			if (idleCount >= 3) {
				lastAdjustTime = System.currentTimeMillis();
				poolSize--;
				executor.setCorePoolSize(executor.getPoolSize() - 1);
			}
		}

	}

}
