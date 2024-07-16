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
package com.cubrid.cubridmigration.core.engine.importer.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.DBUtils;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.JDBCConManager;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.SingleRecordErrorEvent;
import com.cubrid.cubridmigration.core.engine.exception.JDBCConnectErrorException;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.exception.UserDefinedHandlerException;
import com.cubrid.cubridmigration.core.engine.importer.ErrorRecords2SQLFileWriter;
import com.cubrid.cubridmigration.core.engine.importer.Importer;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDSQLHelper;
import com.cubrid.cubridmigration.cubrid.stmt.CUBRIDParameterSetter;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.tibero.TiberoSQLHelper;

/**
 * OnlineImporter responses to import database objects to target through JDBC
 * driver.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-3 created by Kevin Cao
 */
public class JDBCImporter extends
		Importer {

	private final JDBCConManager connectionManager;
	private final MigrationConfiguration config;
	private final CUBRIDParameterSetter parameterSetter;
	private final ErrorRecords2SQLFileWriter errorRecordsWriter;
	
	private SQLHelper sqlHelper;

	public JDBCImporter(MigrationContext mrManager) {
		super(mrManager);
		this.parameterSetter = mrManager.getParamSetter();
		this.config = mrManager.getConfig();
		this.connectionManager = mrManager.getConnManager();
		this.errorRecordsWriter = new ErrorRecords2SQLFileWriter(mrManager);
		
		sqlHelper = setSqlHelper();
	}
	
	private SQLHelper setSqlHelper() {
//		if (config.getDestType() == DatabaseType.CUBRID.getID()) {
//			return CUBRIDSQLHelper.getInstance(null);
//		} else if (config.getDestType() == DatabaseType.TIBERO.getID()) {
//			return TiberoSQLHelper.getInstance(null);
//		}
		
		if (config.getDestType() == DatabaseType.CUBRID.getID()) {
			return CUBRIDSQLHelper.getInstance(null);
		} else {
			return TiberoSQLHelper.getInstance(null);
		}
	}

	/**
	 * Execute DDL.
	 * 
	 * @param sql String
	 */
	public void executeDDL(String sql) {
		Connection conn = connectionManager.getTargetConnection(); //NOPMD
		Statement stmt = null; //NOPMD
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
		} catch (SQLException ex) {
			throw new NormalMigrationException(ex);
		} finally {
			DBUtils.commit(conn);
			Closer.close(stmt);
			connectionManager.closeTar(conn);
		}
	}

	/**
	 * 
	 * Create table
	 * 
	 * @param table Table
	 */
	public void createTable(Table table) {
		String sql = sqlHelper.getTableDDL(table);
		table.setDDL(sql);
		try {
			executeDDL(sql);
			createObjectSuccess(table);
		} catch (RuntimeException e) {
			createObjectFailed(table, e);
			return;
		}
	}

	/**
	 * Create View
	 * 
	 * @param view View
	 */
	public void createView(View view) {
		String viewDDL = CUBRIDSQLHelper.getInstance(null).getViewDDL(view);
		view.setDDL(viewDDL);
		try {
			executeDDL(viewDDL);
			createObjectSuccess(view);
		} catch (RuntimeException e) {
			createObjectFailed(view, e);
		}
	}

	/**
	 * Create primary key
	 * 
	 * @param pk primary key
	 */
	public void createPK(PK pk) {
		String ddl = CUBRIDSQLHelper.getInstance(null).getPKDDL(pk.getTable().getName(),
				pk.getName(), pk.getPkColumns());
		pk.setDDL(ddl);
		try {
			executeDDL(ddl);
			createObjectSuccess(pk);
		} catch (RuntimeException e) {
			createObjectFailed(pk, e);
		}

	}

	/**
	 * Create foreign key
	 * 
	 * @param fk foreign key
	 */
	public void createFK(FK fk) {
		String ddl = CUBRIDSQLHelper.getInstance(null).getFKDDL(fk.getTable().getName(), fk);
		fk.setDDL(ddl);
		try {
			executeDDL(ddl);
			createObjectSuccess(fk);
		} catch (RuntimeException e) {
			createObjectFailed(fk, e);
		}
	}

	/**
	 * Create index
	 * 
	 * @param index Index
	 */
	public void createIndex(Index index) {
		String ddl = CUBRIDSQLHelper.getInstance(null).getIndexDDL(index.getTable().getName(),
				index, "");
		index.setDDL(ddl);
		try {
			executeDDL(ddl);
			createObjectSuccess(index);
		} catch (RuntimeException e) {
			createObjectFailed(index, e);
		}

	}

	/**
	 * Create sequence
	 * 
	 * @param sq sequence
	 */
	public void createSequence(Sequence sq) {
		String ddl = CUBRIDSQLHelper.getInstance(null).getSequenceDDL(sq);
		sq.setDDL(ddl);
		try {
			executeDDL(ddl);
			createObjectSuccess(sq);
		} catch (RuntimeException e) {
			createObjectFailed(sq, e);
		}
	}

	/**
	 * Import records ,if connection lost, it will be retry 3 times.
	 * 
	 * @param stc Table
	 * @param records List<Record>
	 * @return success count
	 */
	public int importRecords(SourceTableConfig stc, List<Record> records) {
		int retryCount = 0;
		mrManager.getStatusMgr().addImpCount(stc.getOwner(), stc.getName(), records.size());
		while (true) {
			try {
				return simpleImportRecords(stc, records);
			} catch (JDBCConnectErrorException ex) {
				if (retryCount < 3) {
					retryCount++;
					ThreadUtils.threadSleep(2000, eventHandler);
				} else {
					eventHandler.handleEvent(new ImportRecordsEvent(stc, records.size(), ex, null));
					return 0;
				}
			} catch (Exception e) {
				eventHandler.handleEvent(new ImportRecordsEvent(stc, records.size(), e, null));
				return 0;
			}
		}
	}

	/**
	 * If database connect is closed by server, it needs retry 5 times
	 * 
	 * @param ex the exception raised.
	 * @return true:need retry.
	 */
	private boolean isConnectionCutDown(SQLException ex) {
		String message = ex.getMessage();
		return message.indexOf("Connection or Statement might be closed") >= 0
				|| message.indexOf("Cannot communicate with the broker") >= 0
				|| ex.getErrorCode() == -2019 || ex.getErrorCode() == -21003
				&& ex.getErrorCode() == -2003;
	}

	/**
	 * get Insert DML
	 * 
	 * @param tt target table object
	 * @return SQL string with parameters
	 */
	public String getTargetInsertDML(SourceTableConfig tt) {
		StringBuffer nameBuf = new StringBuffer("insert into ").append(
				CUBRIDSQLHelper.getInstance(null).getQuotedObjName(tt.getTarget())).append(" (");
		StringBuffer valueBuf = new StringBuffer(" values (");
		List<SourceColumnConfig> columns = tt.getColumnConfigList();
		int len = columns.size();
		for (int i = 0; i < len; i++) {
			if (i > 0) {
				nameBuf.append(", ");
				valueBuf.append(", ");
			}
			String columnName = columns.get(i).getTarget();
			nameBuf.append('"').append(columnName).append('"');
			valueBuf.append('?');
		}
		nameBuf.append(')');
		valueBuf.append(')');
		nameBuf.append(valueBuf);
		return nameBuf.toString();
	}

	/**
	 * Import with no retry.
	 * 
	 * @param stc Table
	 * @param records List<Record>
	 * @return success record count
	 * @throws SQLException when SQL error
	 */
	private int simpleImportRecords(SourceTableConfig stc, List<Record> records) throws SQLException {
		//Auto commit is false by default.
		Connection conn = connectionManager.getTargetConnection(); //NOPMD
		PreparedStatement stmt = null; //NOPMD
		int result = 0;
		try {
			//get target table
			final Table tt = config.getTargetTableSchema(stc.getTarget());
			if (tt == null) {
				return 0;
			}
			String sql = getTargetInsertDML(stc);
			try {
				stmt = conn.prepareStatement(sql);

				for (Record rc : records) {
					if (rc == null) {
						continue;
					}
					try {
						Record trec = createTargetRecord(stc, tt, rc);
						parameterSetter.setRecord2Statement(trec, stmt);
						stmt.addBatch();
					} catch (SQLException ex) {
						if (isConnectionCutDown(ex)) {
							throw new JDBCConnectErrorException(ex);
						}
						eventHandler.handleEvent(new SingleRecordErrorEvent(rc, ex));
					} catch (Exception ex) {
						eventHandler.handleEvent(new SingleRecordErrorEvent(rc, ex));
					}
				}
				int[] exers = stmt.executeBatch();
				DBUtils.commit(conn);
				for (int rs : exers) {
					result += rs;
				}
				if (result != records.size()) {
					eventHandler.handleEvent(new ImportRecordsEvent(stc, records.size() - result,
							new NormalMigrationException(ERROR_RECORD_MSG), null));
				}
				if (result > 0) {
					eventHandler.handleEvent(new ImportRecordsEvent(stc, result));
				}
			} catch (SQLException ex) {
				if (isConnectionCutDown(ex)) {
					throw new JDBCConnectErrorException(ex);
				}
				DBUtils.rollback(conn);
				//If SQL has errors, write the records to a SQL files.
				String file = null;
				if (config.isWriteErrorRecords()) {
					List<Record> errorRecords = new ArrayList<Record>();
					for (Record rc : records) {
						if (rc == null) {
							continue;
						}
						Record trec = createTargetRecord(stc, tt, rc);
						if (trec != null) {
							errorRecords.add(trec);
						}
					}
					file = errorRecordsWriter.writeSQLRecords(stc, errorRecords);
				}
				eventHandler.handleEvent(new ImportRecordsEvent(stc, records.size(), ex, file));
			}
		} finally {
			Closer.close(stmt);
			connectionManager.closeTar(conn);
		}
		return result;
	}

	/**
	 * Create a target record by source record
	 * 
	 * @param stc SourceTableConfig
	 * @param tt Target Table
	 * @param rrec source record
	 * @return Target record
	 */
	private Record createTargetRecord(SourceTableConfig stc, Table tt, Record rrec) {
		Record trec = new Record();
		Map<String, Object> recordMap = rrec.getColumnValueMap();
		DBTransformHelper dbHelper = config.getDBTransformHelper();
		for (Record.ColumnValue cv : rrec.getColumnValueList()) {
			SourceColumnConfig scc = stc.getColumnConfig(cv.getColumn().getName());
			if (scc == null) {
				continue;
			}
			Column targetColumn = tt.getColumnByName(scc.getTarget());
			if (targetColumn == null) {
				continue;
			}
			Object targetValue;
			try {
				targetValue = dbHelper.convertValueToTargetDBValue(config, recordMap, scc,
						cv.getColumn(), targetColumn, cv.getValue());
			} catch (UserDefinedHandlerException ex) {
				targetValue = cv.getValue();
				eventHandler.handleEvent(new SingleRecordErrorEvent(rrec, ex));
			}
			trec.addColumnValue(targetColumn, targetValue);
		}
		return trec;
	}

	public int importVertex(Vertex v, List<Record> records) {
		return 0;
	}
    
	public int importEdge(Edge e, List<Record> records) {
		return 0;
	}

	public int importVertexHeader(Vertex v) {
		return 0;
	}

	public int importEdgeHeader(Edge e) {
		return 0;
	}

	public int importCDCObject(Vertex vertex, Edge e, List<Record> records) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int importQuickScript() {
		// TODO Auto-generated method stub
		return 0;
	}

}
