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
package com.cubrid.common.configuration.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * JDBCChangingManager maintains all the subjects about changing JDBC
 * information and all the observers about responding changing.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-2-10 created by Kevin Cao
 */
public final class JDBCDriverChangingManager {

	private static final JDBCDriverChangingManager INSTANCE = new JDBCDriverChangingManager();

	/**
	 * Single ton
	 * 
	 * @return JDBCDriverChangingManager
	 */
	public static final JDBCDriverChangingManager getInstance() {
		return INSTANCE;
	}

	private List<String> driverFiles = new ArrayList<String>();

	private List<IJDBCDriverChangedSubject> subjects = new ArrayList<IJDBCDriverChangedSubject>();

	private List<IJDBCDriverChangedObserver> observers = new ArrayList<IJDBCDriverChangedObserver>();

	private JDBCDriverChangingManager() {
		registObservor(new Observer());
	}

	/**
	 * Register a driver changed subject.
	 * 
	 * @param subject IJDBCDriverChangedSubject
	 */
	public void registSubject(IJDBCDriverChangedSubject subject) {
		if (!subjects.contains(subject)) {
			subjects.add(subject);
			for (IJDBCDriverChangedObserver ob : observers) {
				subject.addObservor(ob);
			}
		}
	}

	/**
	 * Register a driver changed observer.
	 * 
	 * @param ob IJDBCDriverChangedObserver
	 */
	public void registObservor(IJDBCDriverChangedObserver ob) {
		if (!observers.contains(ob)) {
			observers.add(ob);
			for (IJDBCDriverChangedSubject sub : subjects) {
				sub.addObservor(ob);
			}
		}
	}

	/**
	 * Get All drivers.
	 * 
	 * @return List<String>
	 */
	public List<String> getAllDrivers() {
		return new ArrayList<String>(driverFiles);
	}

	/**
	 * 
	 * Observer to maintain a list for other subjects' synchronizing.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2014-2-10 created by Kevin Cao
	 */
	private class Observer implements
			IJDBCDriverChangedObserver {
		/**
		 * Add a driver to management
		 * 
		 * @param initiator who triggered the event.
		 * @param df driver file full path
		 */
		public void afterAdd(IJDBCDriverChangedSubject initiator, String df) {
			if (!driverFiles.contains(df)) {
				driverFiles.add(df);
			}
		}

		/**
		 * Delete a driver from management
		 * 
		 * @param initiator who triggered the event.
		 * @param df driver file full path
		 */
		public void afterDelete(IJDBCDriverChangedSubject initiator,
				String delDf) {
			driverFiles.remove(delDf);
		}
	}
}
