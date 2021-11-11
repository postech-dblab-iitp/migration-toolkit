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
package com.cubrid.common.update.p2;

import org.eclipse.equinox.p2.ui.Policy;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import com.cubrid.common.update.p2.handler.UpdatePolicy;

/**
 * Update plugin bundle
 * 
 * @author pangqiren
 * @version 1.0 - 2011-7-7 created by pangqiren
 */
public class UpdatePlugin extends AbstractUIPlugin {
	public static final String PLUGIN_ID = "com.cubrid.common.update.p2"; //$NON-NLS-1$
	private static UpdatePlugin plugin;
	
	private ServiceRegistration policyRegistration;

	public void start(BundleContext context) throws Exception {
		super.start(context);
		plugin = this;
		registerP2Policy(context);
	}

	public void stop(BundleContext context) throws Exception {
		plugin = null;
		policyRegistration.unregister();
		policyRegistration = null;
		super.stop(context);
	}

	/**
	 * Get plug-in instance
	 * 
	 * @return UpdatePlugin
	 */
	public static UpdatePlugin getDefault() {
		return plugin;
	}

	/**
	 * Register the p2 policy
	 * 
	 * @param context BundleContext
	 */
	private void registerP2Policy(BundleContext context) {
		policyRegistration = context.registerService(Policy.class.getName(), new UpdatePolicy(), null);
	}
}
