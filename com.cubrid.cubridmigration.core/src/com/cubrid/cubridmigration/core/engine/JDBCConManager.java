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
package com.cubrid.cubridmigration.core.engine;

import java.sql.Connection;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.connection.IConnHelper;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.exception.JDBCConnectErrorException;

/**
 * JDBCConnectionManager is a JDBC connection pool.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-3 created by Kevin Cao
 */
public class JDBCConManager implements
		ICanDispose {

	//private static final Logger LOG = LogUtil.getLogger(JDBCConManager.class);

	protected final MigrationConfiguration config;
	//Thread safe
	//private final List<Connection> sourceConnections = new ArrayList<Connection>();
	//Thread safe
	//private final List<Connection> targetConnections = new ArrayList<Connection>();
	//Thread safe
	//private final List<Connection> inUseSrcConnections = new ArrayList<Connection>();
	//Thread safe
	//private final List<Connection> inUseTarConnections = new ArrayList<Connection>();

	//private final Object tarLock = new Object();
	//private final Object srcLock = new Object();

	protected boolean isDisposed = false;

	public JDBCConManager(MigrationConfiguration config2) {
		this.config = config2;
	}

	/**
	 * Retrieves a target JDBC connection
	 * 
	 * @return Connection
	 */
	public Connection getTargetConnection() {
		if (isDisposed) {
			throw new JDBCConnectErrorException("Disposed.");
		}
		try {
			ConnParameters cp = config.getTargetConParams().clone();
			IConnHelper chelper;
			if (config.targetIsGraph()) {
				chelper = DatabaseType.NEO4J.getConHelper();
			} else {
				chelper = DatabaseType.CUBRID.getConHelper();
			}
			Connection tc = chelper.createConnection(cp); //NOPMD
			return tc;
		} catch (Exception e) {
			throw new JDBCConnectErrorException(e);
		}
		//		synchronized (tarLock) {
		//			try {
		//				Connection tc;
		//				if (targetConnections.isEmpty()) {
		//					ConnParameters cp = config.getTargetConParams().clone();
		//					IConnHelper chelper = DatabaseType.CUBRID.getConHelper();
		//					tc = chelper.createConnection(cp); //NOPMD
		//				} else {
		//					tc = targetConnections.get(0);
		//					targetConnections.remove(tc);
		//					if (!testCubridConnection(tc)) {
		//						ConnParameters cp = config.getTargetConParams().clone();
		//						IConnHelper chelper = DatabaseType.CUBRID.getConHelper();
		//						tc = chelper.createConnection(cp); //NOPMD
		//					}
		//				}
		//				inUseTarConnections.add(tc);
		//				return tc;
		//			} catch (Exception e) {
		//				throw new JDBCConnectErrorException(e);
		//			}
		//		}
	}

	//	/**
	//	 * Test the validation of cubrid connection
	//	 * 
	//	 * @param con cubrid connection
	//	 * @return true if connection is valid.
	//	 */
	//	private boolean testCubridConnection(Connection con) {
	//		Statement stmt = null;
	//		try {
	//			stmt = con.createStatement();
	//			stmt.execute("select * from db_root");
	//			return true;
	//		} catch (SQLException e) {
	//			return false;
	//		} finally {
	//			Closer.close(stmt);
	//		}
	//
	//	}

	/**
	 * Retrieves a source JDBC connection
	 * 
	 * @return Connection
	 */
	public Connection getSourceConnection() {
		if (isDisposed) {
			throw new JDBCConnectErrorException("Disposed.");
		}
		try {
			ConnParameters cp = config.getSourceConParams().clone();
			IConnHelper chelper = config.getSourceDBType().getConHelper();
			Connection sc = chelper.createConnection(cp); //NOPMD
			return sc;
		} catch (Exception e) {
			throw new JDBCConnectErrorException(e);
		}
		//		synchronized (srcLock) {
		//			try {
		//				Connection sc = null;
		//				if (sourceConnections.isEmpty()) {
		//					ConnParameters cp = config.getSourceConParams().clone();
		//					IConnHelper chelper = config.getSourceDBType().getConHelper();
		//					sc = chelper.createConnection(cp); //NOPMD
		//				} else {
		//					sc = sourceConnections.get(0);
		//					sourceConnections.remove(sc);
		//				}
		//				inUseSrcConnections.add(sc);
		//				return sc;
		//			} catch (Exception e) {
		//				throw new JDBCConnectErrorException(e);
		//			}
		//		}

	}

	/**
	 * Close connection
	 * 
	 * @param conn Connection
	 */
	public void closeSrc(Connection conn) {
		if (conn == null) {
			return;
		}
		Closer.close(conn);
		//		if (isDisposed) {
		//			return;
		//		}
		//		synchronized (srcLock) {
		//			try {
		//				inUseSrcConnections.remove(conn);
		//				sourceConnections.add(conn);
		//			} catch (Exception ex) {
		//				LOG.error("", ex);
		//			}
		//		}
	}

	/**
	 * Close connection
	 * 
	 * @param conn Connection
	 */
	public void closeTar(Connection conn) {
		if (conn == null) {
			return;
		}
		if (isDisposed) {
			return;
		}
		Closer.close(conn);
		//		synchronized (tarLock) {
		//			try {
		//				inUseTarConnections.remove(conn);
		//				targetConnections.add(conn);
		//			} catch (Exception ex) {
		//				LOG.error("", ex);
		//			}
		//		}
	}

	/**
	 * Release all connections
	 * 
	 */
	public void releaseAll() {
		//		synchronized (tarLock) {
		//			List<Connection> cons = new ArrayList<Connection>();
		//			cons.addAll(targetConnections);
		//			cons.addAll(inUseTarConnections);
		//			targetConnections.clear();
		//			inUseTarConnections.clear();
		//
		//			for (Connection conn : cons) {
		//				Closer.close(conn);
		//			}
		//		}

		//		synchronized (srcLock) {
		//			List<Connection> cons = new ArrayList<Connection>(sourceConnections);
		//			cons.addAll(inUseSrcConnections);
		//			sourceConnections.clear();
		//			inUseSrcConnections.clear();
		//
		//			for (Connection conn : cons) {
		//				Closer.close(conn);
		//			}
		//		}
	}

	/**
	 * Dispose
	 */
	public void dispose() {
		isDisposed = true;
		releaseAll();
	}
}
