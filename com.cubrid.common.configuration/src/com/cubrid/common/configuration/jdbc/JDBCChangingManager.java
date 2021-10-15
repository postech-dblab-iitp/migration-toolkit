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

import org.apache.commons.lang.StringUtils;

/**
 * JDBCChangingManager maintains all the subjects about changing JDBC
 * information and all the observers about responding changing.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-2-10 created by Kevin Cao
 */
public final class JDBCChangingManager {

	private static final JDBCChangingManager instance = new JDBCChangingManager();

	public static final JDBCChangingManager getInstance() {
		return instance;
	}

	private List<IJDBCConnecInfo> connections = new ArrayList<IJDBCConnecInfo>();

	private List<IJDBCInfoChangedSubject> subjects = new ArrayList<IJDBCInfoChangedSubject>();

	private List<IJDBCConnectionChangedObserver> observers = new ArrayList<IJDBCConnectionChangedObserver>();

	private JDBCChangingManager() {
		registerObservor(new Observer());
	}

	/**
	 * Register a subject which will fire the connection changed event.
	 * 
	 * @param subject IJDBCInfoChangedSubject
	 */
	public void registerSubject(IJDBCInfoChangedSubject subject) {
		if (!subjects.contains(subject)) {
			subjects.add(subject);
			for (IJDBCConnectionChangedObserver ob : observers) {
				subject.addObservor(ob);
			}
		}
	}

	/**
	 * Register an observer into all subjects to responding the connection
	 * changed events.
	 * 
	 * @param ob IJDBCConnectionChangedObserver
	 */
	public void registerObservor(IJDBCConnectionChangedObserver ob) {
		if (!observers.contains(ob)) {
			observers.add(ob);
			for (IJDBCInfoChangedSubject sub : subjects) {
				sub.addObservor(ob);
			}
		}
	}

	/**
	 * All connections' information shared.
	 * 
	 * @return List<IJDBCConnecInfo>
	 */
	public List<IJDBCConnecInfo> getAllConnections() {
		return new ArrayList<IJDBCConnecInfo>(connections);
	}

	/**
	 * 
	 * Observer to maintain a list for other subjects' synchronizing.
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2014-2-10 created by Kevin Cao
	 */
	private class Observer implements
			IJDBCConnectionChangedObserver {

		/**
		 * Retrieves if the two connections are same.
		 * 
		 * @param oldCon IJDBCConnecInfo
		 * @param newCon IJDBCConnecInfo
		 * @return true if same.
		 */
		private boolean isSameConnection(IJDBCConnecInfo oldCon,
				IJDBCConnecInfo newCon) {
			if (oldCon == null && newCon == null) {
				return true;
			}
			if (oldCon == null || newCon == null) {
				return false;
			}
			if (oldCon.getDbType() != newCon.getDbType()) {
				return false;
			}
			if (!StringUtils.equalsIgnoreCase(oldCon.getHost(),
					newCon.getHost())) {
				return false;
			}
			if (oldCon.getPort() != newCon.getPort()) {
				return false;
			}
			if (!StringUtils.equals(oldCon.getDbName(), newCon.getDbName())) {
				return false;
			}
			if (!StringUtils.equals(oldCon.getConUser(), newCon.getConUser())) {
				return false;
			}
			if (!StringUtils.equals(oldCon.getConUser(), newCon.getConUser())) {
				return false;
			}
			//			if (!StringUtils.equals(oldCon.getSchema(), newCon.getSchema())) {
			//				return false;
			//			}
			return true;
		}

		/**
		 * If the connection is exists in the list.
		 * 
		 * @param con IJDBCConnecInfo
		 * @return not null if exists.
		 */
		private IJDBCConnecInfo isExists(IJDBCConnecInfo con) {
			for (IJDBCConnecInfo cc : connections) {
				if (isSameConnection(cc, con)) {
					return cc;
				}
			}
			return null;
		}

		/**
		 * When add a new connection
		 * 
		 * @param initiator IJDBCInfoChangedSubject
		 * @param newCon IJDBCConnecInfo
		 */
		public void afterAdd(IJDBCInfoChangedSubject initiator,
				IJDBCConnecInfo newCon) {
			IJDBCConnecInfo con = isExists(newCon);
			if (con != null) {
				return;
			}
			connections.add(newCon);
		}

		/**
		 * When modify an existed connection.
		 * 
		 * @param initiator IJDBCInfoChangedSubject
		 * @param oldCon IJDBCConnecInfo
		 * @param newCon IJDBCConnecInfo
		 */
		public void afterModify(IJDBCInfoChangedSubject initiator,
				IJDBCConnecInfo oldCon, IJDBCConnecInfo newCon) {
			IJDBCConnecInfo con = isExists(oldCon);
			if (con != null) {
				connections.remove(con);
			}
			connections.add(newCon);
		}

		/**
		 * Delete a connection.
		 * 
		 * @param initiator IJDBCInfoChangedSubject
		 * @param delCon IJDBCConnecInfo
		 */
		public void afterDelete(IJDBCInfoChangedSubject initiator,
				IJDBCConnecInfo delCon) {
			IJDBCConnecInfo con = isExists(delCon);
			if (con != null) {
				connections.remove(con);
			}
		}
	}
}
