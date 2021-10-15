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

import org.eclipse.equinox.p2.query.IQuery;
import org.eclipse.equinox.p2.query.QueryUtil;
import org.eclipse.equinox.p2.ui.Policy;

/**
 * Update policy
 *
 * @author pangqiren
 */
public class UpdatePolicy extends
		Policy {
	private final static String[] FILTERED_FEATURE_IDS = {
			"com.cubrid.cubridmanager.app.product.root.feature.feature.group",
			"com.cubrid.cubridquery.app.product.root.feature.feature.group",
			"com.cubrid.cubridmigration.app.cubrid_migration_product.root.feature.feature.group",
			"org.eclipse.rcp.feature.group", "org.eclipse.help.feature.group",
			"org.eclipse.equinox.executable.feature.group" };

	@SuppressWarnings({"unchecked", "rawtypes" })
	public UpdatePolicy() {
		setRepositoriesVisible(false);
		setGroupByCategory(true);
		setShowDrilldownRequirements(false);
		setShowLatestVersionsOnly(true);
		setRestartPolicy(RESTART_POLICY_PROMPT);

		StringBuffer expressBuffer = new StringBuffer();
		for (int i = 0; i < FILTERED_FEATURE_IDS.length; i++) {
			expressBuffer.append("id != $" + i);
			if (i != FILTERED_FEATURE_IDS.length - 1) {
				expressBuffer.append(" && ");
			}
		}

		IQuery matchedQuery = QueryUtil.createMatchQuery(
				expressBuffer.toString(), (Object[]) FILTERED_FEATURE_IDS);
		IQuery availableIU = this.getVisibleAvailableIUQuery();
		IQuery installedIU = this.getVisibleInstalledIUQuery();
		this.setVisibleAvailableIUQuery(QueryUtil.createCompoundQuery(
				matchedQuery, availableIU, true));
		this.setVisibleInstalledIUQuery(QueryUtil.createCompoundQuery(
				matchedQuery, installedIU, true));
	}
}
