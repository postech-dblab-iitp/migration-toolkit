/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
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
package com.cubrid.cubridmigration.ui.preference;

import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
import org.eclipse.jface.preference.IPreferenceStore;

import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * 
 * General preference store the value from general preference page
 * 
 * @author pangqiren
 * @version 1.0 - 2009-12-23 created by pangqiren
 */
public final class GeneralPreference {

	private static final Logger LOGGER = LogUtil.getLogger(GeneralPreference.class);
	public static final String MAXIMIZE_WINDOW_ON_START_UP = ".maximize_window_on_start_up";
	public static final String CHECK_NEW_INFO_ON_START_UP = ".check_new_information_on_start_up";
	public static final String USE_CLICK_SINGLE = ".use_click_single";
	public static final String IS_ALWAYS_EXIT = ".is_always_exit";
	public static final String IS_AUTO_CHECK_UPDATE = ".is_auto_check_update";
	public static final String ALWAYS = MessageDialogWithToggle.ALWAYS;
	public static final String PROMPT = MessageDialogWithToggle.PROMPT;

	private static IPreferenceStore pref = null;

	static {
		pref = MigrationUIPlugin.getDefault().getPreferenceStore();
		pref.setDefault(CHECK_NEW_INFO_ON_START_UP, true);
		pref.setDefault(IS_AUTO_CHECK_UPDATE, ALWAYS);
		pref.setDefault(USE_CLICK_SINGLE, false);
	}

	/**
	 * The constructor
	 */
	private GeneralPreference() {
		//empty
	}

	/**
	 * Return whether window is maximized when start up
	 * 
	 * @return <code>true</code>if maximize;<code>false</code> otherwise
	 */
	public static boolean isMaximizeWindowOnStartUp() {
		try {
			return pref.getBoolean(MAXIMIZE_WINDOW_ON_START_UP);
		} catch (Exception ignored) {
			return false;
		}
	}

	/**
	 * Set whether window is maximized when start up
	 * 
	 * @param isMax boolean
	 */
	public static void setMaximizeWindowOnStartUp(boolean isMax) {
		try {
			pref.setValue(GeneralPreference.MAXIMIZE_WINDOW_ON_START_UP, isMax);
		} catch (Exception ignored) {
			LOGGER.error(ignored.getMessage());
		}
	}

	/**
	 * 
	 * Return whether check new information of CUBRID
	 * 
	 * @return <code>true</code>if check;<code>false</code> otherwise
	 */
	public static boolean isCheckNewInfoOnStartUp() {
		try {
			return pref.getBoolean(CHECK_NEW_INFO_ON_START_UP);
		} catch (Exception ignored) {
			return false;
		}
	}

	/**
	 * 
	 * Set whether check new information of CUBRID
	 * 
	 * @param isShowWelcomePage boolean
	 */
	public static void setCheckNewInfoOnStartUp(boolean isShowWelcomePage) {
		try {
			pref.setValue(GeneralPreference.CHECK_NEW_INFO_ON_START_UP,
					isShowWelcomePage);
		} catch (Exception ignored) {
			LOGGER.error(ignored.getMessage());
		}
	}

	/**
	 * 
	 * Return whether use click once
	 * 
	 * @return <code>true</code>if user click once;<code>false</code> otherwise
	 */
	public static boolean isUseClickOnce() {
		try {
			return pref.getBoolean(USE_CLICK_SINGLE);
		} catch (Exception ignored) {
			return false;
		}
	}

	/**
	 * 
	 * Set whether use click once
	 * 
	 * @param isUseClickOnce boolean
	 */
	public static void setUseClickOnce(boolean isUseClickOnce) {
		try {
			pref.setValue(GeneralPreference.USE_CLICK_SINGLE, isUseClickOnce);
		} catch (Exception ignored) {
			LOGGER.error(ignored.getMessage());
		}
	}

	/**
	 * 
	 * Return whether always exit
	 * 
	 * @return <code>true</code>if user click once;<code>false</code> otherwise
	 */
	public static boolean isAlwaysExit() {
		try {
			return ALWAYS.equals(pref.getString(IS_ALWAYS_EXIT));
		} catch (Exception ignored) {
			return false;
		}
	}

	/**
	 * 
	 * Set whether always exit
	 * 
	 * @param isAlwaysExit boolean
	 */
	public static void setAlwaysExit(boolean isAlwaysExit) {
		try {
			pref.setValue(GeneralPreference.IS_ALWAYS_EXIT,
					isAlwaysExit ? GeneralPreference.ALWAYS
							: GeneralPreference.PROMPT);
		} catch (Exception ignored) {
			LOGGER.error(ignored.getMessage());
		}
	}

	/**
	 * 
	 * Return whether auto check update when start
	 * 
	 * @return <code>true</code>if user click once;<code>false</code> otherwise
	 */
	public static boolean isAutoCheckUpdate() {
		try {
			return ALWAYS.equals(pref.getString(IS_AUTO_CHECK_UPDATE));
		} catch (Exception ignored) {
			return false;
		}
	}

	/**
	 * 
	 * Set whether auto check update when start
	 * 
	 * @param isAutoCheckUpdate boolean
	 */
	public static void setAutoCheckUpdate(boolean isAutoCheckUpdate) {
		try {
			pref.setValue(GeneralPreference.IS_AUTO_CHECK_UPDATE,
					isAutoCheckUpdate ? GeneralPreference.ALWAYS
							: GeneralPreference.PROMPT);
		} catch (Exception ignored) {
			LOGGER.error(ignored.getMessage());
		}
	}
}
