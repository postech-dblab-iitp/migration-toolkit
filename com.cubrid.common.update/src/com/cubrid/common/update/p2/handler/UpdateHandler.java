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

import org.eclipse.core.runtime.IStatus;
import org.eclipse.equinox.internal.p2.ui.dialogs.UpdateSingleIUWizard;
import org.eclipse.equinox.p2.operations.RepositoryTracker;
import org.eclipse.equinox.p2.operations.UpdateOperation;
import org.eclipse.equinox.p2.ui.LoadMetadataRepositoryJob;
import org.eclipse.jface.wizard.WizardDialog;

/**
 * UpdateHandler invokes the check for updates UI
 *
 * @author pangqiren
 * @version 1.0 - 2011-7-7 created by pangqiren
 */
@SuppressWarnings("restriction")
public class UpdateHandler extends PreloadingRepositoryHandler {
	private boolean isNoRepos = false;
	private final boolean isAutoCheckUpdate;

	public UpdateHandler() {
		isAutoCheckUpdate = false;
	}

	public UpdateHandler(boolean isAutoUpdateCheck) {
		this.isAutoCheckUpdate = isAutoUpdateCheck;
	}

	protected void doExecute(LoadMetadataRepositoryJob job) {
		if (isNoRepos) {
			return;
		}

		UpdateOperation operation = getProvisioningUI().getUpdateOperation(null, null);

		// check for updates
		IStatus status = operation.resolveModal(null);

		// AUTO check update and there is not update
		if (isAutoCheckUpdate) {
			// user cancelled
			if (status.getSeverity() == IStatus.CANCEL) {
				return;
			}

			// Special case those statuses where we would never want to open a wizard
			if (status.getCode() == UpdateOperation.STATUS_NOTHING_TO_UPDATE) {
				return;
			}

			// there is no plan, so we can't continue.  Report any reason found
			if (operation.getProvisioningPlan() == null && !status.isOK()) {
				return;
			}
		}

		if (getProvisioningUI().getPolicy().continueWorkingWithOperation(operation, getShell())) {
			if (UpdateSingleIUWizard.validFor(operation)) {
				// Special case for only updating a single root
				UpdateSingleIUWizard wizard = new UpdateSingleIUWizard(getProvisioningUI(), operation);
				WizardDialog dialog = new WizardDialog(getShell(), wizard);
				dialog.create();
				dialog.open();
			} else {
				// Open the normal version of the update wizard
				getProvisioningUI().openUpdateWizard(false, operation, job);
			}
		}
	}

	/**
	 * Return whether to preload repositories
	 *
	 * @return boolean
	 */
	protected boolean preloadRepositories() {
		isNoRepos = false;
		RepositoryTracker repoTracker = getProvisioningUI().getRepositoryTracker();
		if (repoTracker.getKnownRepositories(getProvisioningUI().getSession()).length == 0) {
			isNoRepos = true;
			return false;
		}
		return true;
	}

	/**
	 * Return whether to wait for preload
	 *
	 * @return boolean
	 */
	protected boolean waitForPreload() {
		return true;
	}
}
