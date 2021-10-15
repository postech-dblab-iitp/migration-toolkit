/*
 * Copyright (C) 2013 Search Solution Corporation. All rights reserved by Search
 * Solution.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */
package com.cubrid.common.update.p2.handler;

import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.equinox.p2.ui.LoadMetadataRepositoryJob;

/**
 * InstallPluginHandler invokes the install wizard
 *
 * @author pangqiren
 * @version 1.0 - 2011-7-7 created by pangqiren
 */
public class InstallPluginHandler extends PreloadingRepositoryHandler {
	public InstallPluginHandler() {
		super();
	}

	protected void doExecute(LoadMetadataRepositoryJob job) {
		getProvisioningUI().openInstallWizard(null, null, job);
	}

	protected boolean preloadRepositories() {
		return true;
	}

	protected boolean waitForPreload() {
		return !getProvisioningUI().getPolicy().getRepositoriesVisible();
	}

	protected void setLoadJobProperties(Job loadJob) {
		super.setLoadJobProperties(loadJob);

		if (!waitForPreload()) {
			loadJob.setProperty(
					LoadMetadataRepositoryJob.SUPPRESS_AUTHENTICATION_JOB_MARKER,
					Boolean.toString(true));
			loadJob.setProperty(
					LoadMetadataRepositoryJob.SUPPRESS_REPOSITORY_EVENTS,
					Boolean.toString(true));
		}
	}
}
