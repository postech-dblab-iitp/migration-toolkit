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

import java.util.List;

import com.cubrid.common.configuration.jdbc.IJDBCConnecInfo;
import com.cubrid.common.configuration.jdbc.IJDBCConnectionChangedObserver;
import com.cubrid.common.configuration.jdbc.IJDBCInfoChangedSubject;

/**
 * JDBCInfoChangedObserver is responsible for watching the connection changing.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-2-10 created by Kevin Cao
 */
class JDBCInfoChangedObserver implements
		IJDBCConnectionChangedObserver {

	/**
	 * When add a new connection
	 * 
	 * @param initiator IJDBCInfoChangedSubject who triggered the event.
	 * @param newCon IJDBCConnecInfo
	 */
	public void afterAdd(IJDBCInfoChangedSubject initiator,
			IJDBCConnecInfo newCon) {
		if (CMTConParamManager.getInstance().equals(initiator)) {
			return;
		}
		ConnParameters cp = ConnParameters.getConParamByInfo(newCon);
		CMTConParamManager.getInstance().addConnection(cp, true);
	}

	/**
	 * When modify an existed connection.
	 * 
	 * @param initiator IJDBCInfoChangedSubject who triggered the event.
	 * @param oldCon IJDBCConnecInfo
	 * @param newCon IJDBCConnecInfo
	 */
	public void afterModify(IJDBCInfoChangedSubject initiator,
			IJDBCConnecInfo oldCon, IJDBCConnecInfo newCon) {
		if (CMTConParamManager.getInstance().equals(initiator)) {
			return;
		}
		ConnParameters oldCP = CMTConParamManager.getInstance().getConnection(
				oldCon.getConName());
		if (oldCP == null) {
			CMTConParamManager.getInstance().addConnection(
					ConnParameters.getConParamByInfo(newCon), true);
		} else {
			CMTConParamManager.getInstance().updateConnection(
					oldCon.getConName(),
					ConnParameters.getConParamByInfo(newCon), true);
		}
	}

	/**
	 * Delete a connection.
	 * 
	 * @param initiator IJDBCInfoChangedSubject who triggered the event.
	 * @param delCon IJDBCConnecInfo
	 */
	public void afterDelete(IJDBCInfoChangedSubject initiator,
			IJDBCConnecInfo delCon) {
		CMTConParamManager instance = CMTConParamManager.getInstance();
		if (instance.equals(initiator)) {
			return;
		}
		ConnParameters cp = instance.getConnection(delCon.getConName());
		if (cp == null) {
			cp = ConnParameters.getConParamByInfo(delCon);
			List<ConnParameters> connections = instance.getConnections();
			for (ConnParameters cnp : connections) {
				if (cnp.isSameDB(cp)) {
					cp = cnp;
					break;
				}
			}
		}
		instance.removeConnection(cp.getConName(), true);
	}
}