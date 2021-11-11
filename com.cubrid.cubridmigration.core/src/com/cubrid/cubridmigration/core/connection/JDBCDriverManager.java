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
package com.cubrid.cubridmigration.core.connection;

import java.util.ArrayList;
import java.util.List;

import com.cubrid.common.configuration.jdbc.IJDBCDriverChangedObserver;
import com.cubrid.common.configuration.jdbc.IJDBCDriverChangedSubject;
import com.cubrid.common.configuration.jdbc.JDBCDriverChangingManager;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;

/**
 * JDBCDriverManager is responsible for adding and deleting JDBC driver files.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-2-11 created by Kevin Cao
 */
public final class JDBCDriverManager implements
		IJDBCDriverChangedSubject {
	private static final JDBCDriverManager MANAGER;

	static {
		MANAGER = new JDBCDriverManager();
		JDBCDriverChangingManager instance = JDBCDriverChangingManager.getInstance();
		instance.registSubject(MANAGER);
	}

	private List<IJDBCDriverChangedObserver> observers = new ArrayList<IJDBCDriverChangedObserver>();

	private JDBCDriverManager() {
		//Do nothing here.
	}

	/**
	 * Single-ton instance
	 * 
	 * @return JDBCDriverManager
	 */
	public static final JDBCDriverManager getInstance() {
		return MANAGER;
	}

	/**
	 * Add observer
	 * 
	 * @param ob IJDBCDriverChangedObserver
	 */
	public void addObservor(IJDBCDriverChangedObserver ob) {
		if (!observers.contains(ob)) {
			observers.add(ob);
		}
	}

	/**
	 * Add a driver file.
	 * 
	 * @param driverFile full path of driver file
	 * @param silence if true the event will not be triggered.
	 * 
	 * @return true if added successfully
	 */
	public boolean addDriver(String driverFile, boolean silence) {
		DatabaseType[] allTypes = DatabaseType.getAllTypes();
		for (DatabaseType dt : allTypes) {
			if (dt.getJDBCData(driverFile) != null) {
				return false;
			}
			if (dt.addJDBCData(driverFile)) {
				if (silence) {
					return true;
				}
				for (IJDBCDriverChangedObserver ob : observers) {
					ob.afterAdd(this, driverFile);
				}
				return true;
			}
		}
		return false;
	}

	/**
	 * @param driverFile full path of driver file
	 * 
	 * @return true if driver is already existed in the list
	 */
	public boolean isDriverDuplicated(String driverFile) {
		DatabaseType[] allTypes = DatabaseType.getAllTypes();
		for (DatabaseType dt : allTypes) {
			if (dt.getJDBCData(driverFile) != null) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Delete a driver file.
	 * 
	 * @param driverFile full path of driver file
	 * @param silence if true the event will not be triggered.
	 */
	public void deleteDriver(String driverFile, boolean silence) {
		DatabaseType[] allTypes = DatabaseType.getAllTypes();
		for (DatabaseType dt : allTypes) {
			JDBCData jdbcData = dt.getJDBCData(driverFile);
			if (jdbcData == null) {
				continue;
			}
			dt.removeJDBCData(jdbcData);
			if (silence) {
				return;
			}
			for (IJDBCDriverChangedObserver ob : observers) {
				ob.afterDelete(this, driverFile);
			}
			return;
		}
	}
}
