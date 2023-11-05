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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Record.ColumnValue;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.ImportGraphRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.SingleRecordErrorEvent;
import com.cubrid.cubridmigration.core.engine.exception.BreakMigrationException;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.exception.UserDefinedHandlerException;
import com.cubrid.cubridmigration.core.engine.executors.IRunnableExecutor;
import com.cubrid.cubridmigration.core.engine.importer.Importer;
import com.cubrid.cubridmigration.core.engine.task.RunnableResultHandler;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDSQLHelper;
import com.cubrid.cubridmigration.cubrid.Data2StrTranslator;
import com.cubrid.cubridmigration.graph.GraphSQLHelper;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

/**
 * LoadDBImporter : Use LoadDB and CSQL commands to import database objects.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-3 created by Kevin Cao
 */
public abstract class OfflineImporter extends
		Importer {
	private final static Logger LOG = LogUtil.getLogger(OfflineImporter.class);
	protected static final int FILE_SCHEMA_OBJ = 0;
	protected static final int FILE_DATA_RCD = 1;
	protected static final int FILE_SCHEMA_IDX = 2;
	protected static final int FILE_DATA_LOB = 3;

	protected MigrationConfiguration config;

	//LoadDB command can not support multi-thread. LoadDBFile task runs in this pool.
	protected final IRunnableExecutor cmTaskService;

	//Default path of migration load DB file 
	protected Data2StrTranslator unloadFileUtil;

	/**
	 * ImportFileWriter writes data to file with specified format
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-10-10 created by Kevin Cao
	 */
	protected interface ImportFileWriter {
		/**
		 * Write data to a CSV file
		 * 
		 * @param stc SourceTableConfig
		 * @param records List<Record> records
		 * @param file File
		 * @param tt Table
		 * @return total count
		 * @throws Exception ex
		 */
		int writeData(final SourceTableConfig stc, final List<Record> records, File file,
				final Table tt) throws Exception;
	}

	/**
	 * 
	 * UnloadFileWriter responses to write data to UnloadDB file
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-10-10 created by Kevin Cao
	 */
	protected class UnloadFileWriter implements
			ImportFileWriter {
		/**
		 * Write unloadDB data to file
		 * 
		 * @param stc SourceTableConfig
		 * @param records List<Record>
		 * @param file File
		 * @param tt Table
		 * @return total count
		 * @throws FileNotFoundException ex
		 * @throws UnsupportedEncodingException ex
		 * @throws IOException ex
		 */
		public int writeData(final SourceTableConfig stc, final List<Record> records, File file,
				final Table tt) throws FileNotFoundException,
				UnsupportedEncodingException,
				IOException {
			if (LOG.isDebugEnabled()) {
				LOG.debug("[IN]writeData()");
			}
			Writer pw = new BufferedWriter(new PrintWriter(file, config.getTargetCharSet()),
					CUBRIDIOUtils.DEFAULT_MEMORY_CACHE_SIZE);
			try {
				String header = getDataFileHeader(stc);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]header=" + header);
				}
				pw.write(header);
				// The template LOB files path in local.
				List<String> lobFiles = new ArrayList<String>();
				int total = 0;
				for (Record re : records) {
					if (re == null) {
						continue;
					}
					String res = unloadFileUtil.getRecordString(re.getColumnValueList(),
							getRecordString(stc, tt, re, lobFiles));
					if (res == null) {
						continue;
					}
					pw.write(res);
					pw.write("\n");
					total++;
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]total=" + total);
				}
				pw.flush();
				if (LOG.isDebugEnabled()) {
					LOG.debug("[VAR]lobFiles.size=" + (lobFiles == null ? null : lobFiles.size()));
				}
				for (String lobFile : lobFiles) {
					sendLOBFile(lobFile, stc.getTarget());
				}
				return total;
			} finally {
				pw.close();
			}
		}
	}

	/**
	 * CSVFileWriter responses to write data to CSV
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-10-10 created by Kevin Cao
	 */
	protected class CSVFileWriter implements
			ImportFileWriter {
		/**
		 * Write data to a CSV file
		 * 
		 * @param stc SourceTableConfig
		 * @param records List<Record> records
		 * @param file File
		 * @param tt Table
		 * @return total count
		 * @throws Exception ex
		 */
		public int writeData(final SourceTableConfig stc, final List<Record> records, File file,
				final Table tt) throws Exception {
			CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(file),
					config.getTargetCharSet()), config.getCsvSettings().getSeparateChar(),
					config.getCsvSettings().getQuoteChar(), config.getCsvSettings().getEscapeChar());
			try {
				List<String> lobFiles = new ArrayList<String>();
				int total = 0;
				for (Record re : records) {
					if (re == null) {
						continue;
					}
					List<String> res = getRecordString(stc, tt, re, lobFiles);
					if (res == null) {
						continue;
					}
					writer.writeNext(res.toArray(new String[res.size()]));
					total++;
				}
				writer.flush();
				return total;
			} finally {
				writer.close();
			}
		}
	}

	/**
	 * XLSFileWriter responses to write data to XLS
	 * 
	 * @author PCraft
	 * @version 1.0 - 2013-01-18 created by PCraft
	 */
	protected class XLSFileWriter implements
			ImportFileWriter {
		/**
		 * Write data to a XLS file
		 * 
		 * @param stc SourceTableConfig
		 * @param records List<Record> records
		 * @param file File
		 * @param tt Table
		 * @return total count
		 * @throws Exception ex
		 */
		public int writeData(final SourceTableConfig stc, final List<Record> records, File file,
				final Table tt) throws Exception {
			WorkbookSettings workbookSettings = new WorkbookSettings();
			workbookSettings.setEncoding(config.getTargetCharSet());
			WritableWorkbook workbook = Workbook.createWorkbook(file, workbookSettings);
			WritableSheet sheet = workbook.createSheet(tt.getName(), 0);

			try {
				List<String> lobFiles = new ArrayList<String>();
				int total = 0;
				for (Record re : records) {
					if (re == null) {
						continue;
					}
					List<String> res = getRecordString(stc, tt, re, lobFiles);
					if (res == null) {
						continue;
					}

					int index = 0;
					for (String val : res) {
						sheet.addCell(new jxl.write.Label(index++, total, val));
					}

					total++;
				}

				workbook.write();
				return total;
			} finally {
				workbook.close();
			}
		}
	}

	/**
	 * SQLFileWriter responses to write SQL to file
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2012-10-10 created by Kevin Cao
	 */
	protected class SQLFileWriter implements
			ImportFileWriter {
		/**
		 * Write data to a CSV file
		 * 
		 * @param stc SourceTableConfig
		 * @param records List<Record> records
		 * @param file File
		 * @param tt Table
		 * @return total count
		 * @throws Exception ex
		 */
		public int writeData(final SourceTableConfig stc, final List<Record> records, File file,
				final Table tt) throws Exception {
			Writer pw = new BufferedWriter(new PrintWriter(file, config.getTargetCharSet()),
					CUBRIDIOUtils.DEFAULT_MEMORY_CACHE_SIZE);
			try {
				List<String> lobFiles = new ArrayList<String>();
				int total = 0;
				for (Record re : records) {
					if (re == null) {
						continue;
					}
					StringBuffer sb = new StringBuffer("INSERT INTO \"").append(stc.getTarget()).append(
							"\"(");
					boolean isFirst = true;
					for (ColumnValue cv : re.getColumnValueList()) {
						//Find target column configuration 
						SourceColumnConfig tColCfg = stc.getColumnConfig(cv.getColumn().getName());
						if (tColCfg == null) {
							continue;
						}
						if (isFirst) {
							isFirst = false;
						} else {
							sb.append(',');
						}
						sb.append('"').append(tColCfg.getTarget()).append('"');
					}
					sb.append(")VALUES(");
					List<String> values = getRecordString(stc, tt, re, lobFiles);
					if (CollectionUtils.isEmpty(values)) {
						continue;
					}
					isFirst = true;
					for (String vv : values) {
						if (isFirst) {
							isFirst = false;
						} else {
							sb.append(',');
						}
						sb.append(vv);
					}
					sb.append(");");
					pw.write(sb.toString());
					pw.write("\n");
					total++;
				}
				pw.flush();
				return total;
			} finally {
				pw.close();
			}
		}
	}

	private final ImportFileWriter importFileWriter;

	public OfflineImporter(MigrationContext mrManager) {
		super(mrManager);
		this.config = mrManager.getConfig();
		cmTaskService = mrManager.getMergeTaskExe();
		if (config.targetIsCSV()) {
			importFileWriter = new CSVFileWriter();
		} else if (config.targetIsXLS()) {
			importFileWriter = new XLSFileWriter();
		} else if (config.targetIsSQL()) {
			importFileWriter = new SQLFileWriter();
		} else {
			importFileWriter = new UnloadFileWriter();
		}
	}

	/**
	 * Get the LOB's file path
	 * 
	 * @param tableName String
	 * @return base path+table name
	 */
	protected abstract String getLOBDir(String tableName);

	/**
	 * Send schema file and data file to server for loadDB command.
	 * 
	 * @param fileName the file to be sent.
	 * @param stc source table configuration.
	 * @param impCount the count of records in file.
	 * @param expCount Exported record count this time
	 */
	protected abstract void handleDataFile(String fileName, final SourceTableConfig stc,
			final int impCount, int expCount);
	
	protected abstract void handleDataFile(String fileName, final Edge e, final int impCount, final int expCount);
	
	protected abstract void handleDataFile(String fileName, final Vertex v, final int impCount, final int expCount);
	
	protected abstract void handleDataFileHeader(String fileName, final Edge e, final int impCount, final int expCount);
	
	protected abstract void handleDataFileHeader(String fileName, final Vertex v, final int impCount, final int expCount);

	/**
	 * Send schema file and data file to server for loadDB command.
	 * 
	 * @param fileName the file to be sent.
	 * @param tableName tableName
	 * 
	 */
	protected abstract void sendLOBFile(String fileName, String tableName);

	/**
	 * Send schema file and data file to server for loadDB command.
	 * 
	 * @param fileName the file to be sent.
	 * @param listener a call interface.
	 * @param isIndex true if the DDL is about index
	 */
	protected abstract void sendSchemaFile(String fileName, RunnableResultHandler listener,
			boolean isIndex);

	/**
	 * Write content to file.
	 * 
	 * @param fileName to be write
	 * @param content to be write
	 */
	protected void writeFile(String fileName, String content) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]writeFile()");
		}
		File file = new File(fileName);
		if (LOG.isDebugEnabled()) {
			LOG.debug("[VAR]fileName=" + fileName);
		}
		try {
			PathUtils.createFile(file);
			PrintWriter pw = new PrintWriter(file, "utf8");
			pw.write(content);
			pw.flush();
			pw.close();
		} catch (FileNotFoundException e) {
			LOG.error("", e);
			throw new BreakMigrationException(e);
		} catch (IOException e) {
			LOG.error("", e);
			throw new BreakMigrationException(e);
		}
	}

	/**
	 * Execute DDL
	 * 
	 * @param sql String to executed
	 */
	public void executeDDL(String sql) {
		executeDDL(sql, true, null);
	}

	/**
	 * Execute DDL SQLs.
	 * 
	 * @param sql to be executed.
	 * @param isIndex true if the sql is DDL of index
	 * @param listener to be called back
	 */
	protected void executeDDL(String sql, boolean isIndex, RunnableResultHandler listener) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[IN]executeDDL().sql=" + sql);
		}
		String fileName = getRandomTempFileName();
		if (LOG.isDebugEnabled()) {
			LOG.debug("[VAR]fileName=" + fileName);
		}
		writeFile(fileName, sql);
		sendSchemaFile(fileName, listener, isIndex);
	}

	/**
	 * Retrieves the data file's header
	 * 
	 * @param table Table
	 * @return String header
	 */
	protected String getDataFileHeader(SourceTableConfig table) {
		StringBuffer sb = new StringBuffer("%class [").append(table.getTarget()).append("] (");
		String spliter = "";
		for (SourceColumnConfig col : table.getColumnConfigList()) {
			sb.append(spliter).append("[").append(col.getTarget()).append("]");
			spliter = " ";
		}
		sb.append(")\n");
		return sb.toString();
	}

	/**
	 * Create a schema result handler
	 * 
	 * @param obj Object to be create
	 * @return RunnableResultHandler
	 */
	protected RunnableResultHandler createResultHandler(final DBObject obj) {
		return new RunnableResultHandler() {

			public void success() {
				createObjectSuccess(obj);
			}

			public void failed(String error) {
				createObjectFailed(obj, new NormalMigrationException(error));
			}
		};
	}
	
	protected RunnableResultHandler createResultHandler(final Vertex v, final int recCounter) {
		return new RunnableResultHandler() {
			ImportGraphRecordsEvent graphEvent = new ImportGraphRecordsEvent(v, recCounter);
			
			public void success() {
				//TODO here
				eventHandler.handleEvent(graphEvent);
			}
			
			public void failed(String error) {
				//TODO here
				eventHandler.handleEvent(graphEvent);
			}
		};
	}
	
	protected RunnableResultHandler createResultHandler(final Edge e, final int recCounter) {
		return new RunnableResultHandler() {
			ImportGraphRecordsEvent graphEvent = new ImportGraphRecordsEvent(e, recCounter);
			
			public void success() {
				eventHandler.handleEvent(graphEvent);
				
			}
			
			public void failed(String error) {
				graphEvent.isSuccess();
			}
		};
	}

	//	/**
	//	 * Retrieve the CM server object.
	//	 * 
	//	 * @return current CM server
	//	 */
	//	protected CMSInfo getCMServer() {
	//		return CMSManager.getInstance().findServer(
	//				config.getCmServer().getHost(), config.getCmServer().getPort(),
	//				config.getCmServer().getUser());
	//	}

	/**
	 * Retrieves the string for load DB command of record.
	 * 
	 * @param stc SourceTableConfig
	 * @param tt target Table
	 * @param re source Record
	 * @param lobFiles to be uploaded
	 * @return string of record
	 */
	protected List<String> getRecordString(SourceTableConfig stc, Table tt, Record re,
			List<String> lobFiles) {
		try {
			List<String> dataList = new ArrayList<String>();
			//get target table
			Map<String, Object> recordMap = re.getColumnValueMap();
			for (Record.ColumnValue cv : re.getColumnValueList()) {
				SourceColumnConfig scc = stc.getColumnConfig(cv.getColumn().getName());
				if (scc == null) {
					throw new NormalMigrationException("Column not found.");
				}
				Column targetColumn = tt.getColumnByName(scc.getTarget());
				if (targetColumn == null) {
					throw new NormalMigrationException("Column not found.");
				}
				DBTransformHelper dbHelper = config.getDBTransformHelper();
				Object targetValue;
				try {
					targetValue = dbHelper.convertValueToTargetDBValue(config, recordMap, scc,
							cv.getColumn(), targetColumn, cv.getValue());
				} catch (UserDefinedHandlerException ex) {
					targetValue = cv.getValue();
					eventHandler.handleEvent(new SingleRecordErrorEvent(re, ex));
				}

				String fileStr = unloadFileUtil.stringValueOf(targetValue, targetColumn, lobFiles);
				if (CollectionUtils.isNotEmpty(lobFiles)) {
					String lobDir = config.getTargetLOBRootPath();
					if (StringUtils.isBlank(lobDir)) {
						lobDir = getLOBDir(stc.getTarget());
					} else {
						lobDir = lobDir + "lob/" + stc.getTarget() + "/";
					}
					fileStr = fileStr.replace(Data2StrTranslator.LOBFILEPATH, lobDir);
				}
				dataList.add(fileStr);
			}
			return dataList;
		} catch (Exception ex) {
			eventHandler.handleEvent(new SingleRecordErrorEvent(re, ex));
		}
		return null;
	}

	/**
	 * Retrieves a random template file name.
	 * 
	 * @return template file name
	 */
	protected String getRandomTempFileName() {
		return mrManager.getDirAndFilesMgr().getNewTempFile();
	}

	/**
	 * Import records of tables. The records' content are including source
	 * columns + source values.
	 * 
	 * @param stc to be imported
	 * @param records to be imported
	 * @return success count
	 */
	public int importRecords(final SourceTableConfig stc, final List<Record> records) {
		String tmpDataFileName = getRandomTempFileName() + config.getDataFileExt();
		if (LOG.isDebugEnabled()) {
			LOG.debug("[VAR]tmpDataFileName=" + tmpDataFileName);
		}
		File file = new File(tmpDataFileName);
		int successCnt;
		try {
			final Table tt = config.getTargetTableSchema(stc.getTarget());
			if (null == tt) {
				throw new NormalMigrationException("Target Table " + stc.getTarget()
						+ " not found.");
			}
			PathUtils.createFile(file);
			//Cache to get better performance
			successCnt = importFileWriter.writeData(stc, records, file, tt);
			if (successCnt != records.size()) {
				eventHandler.handleEvent(new ImportRecordsEvent(stc, records.size() - successCnt,
						new NormalMigrationException(ERROR_RECORD_MSG), null));
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("[VAR]successCnt=" + successCnt);
			}
			handleDataFile(tmpDataFileName, stc, successCnt, records.size());
		} catch (Exception ex) {
			throw new BreakMigrationException(ex);
		}
		return successCnt;
	}

	/**
	 * Create table
	 * 
	 * @param table to be created
	 */
	public void createTable(final Table table) {
		StringBuffer sql = new StringBuffer();
		//Create new table
		String ddl = CUBRIDSQLHelper.getInstance(null).getTableDDL(table);
		table.setDDL(ddl);
		sql.append(ddl).append("\n");
		executeDDL(sql.toString(), false, createResultHandler(table));
	}

	/**
	 * Create view
	 * 
	 * @param view to be created
	 */
	public void createView(View view) {
		String viewDDL = CUBRIDSQLHelper.getInstance(null).getViewDDL(view);
		view.setDDL(viewDDL);
		executeDDL(viewDDL + "\n", false, createResultHandler(view));
	}

	/**
	 * Create primary key
	 * 
	 * @param pk to be created
	 */
	public void createPK(PK pk) {
		String ddl = CUBRIDSQLHelper.getInstance(null).getPKDDL(pk.getTable().getName(),
				pk.getName(), pk.getPkColumns());
		pk.setDDL(ddl);
		executeDDL(ddl + ";\n", true, createResultHandler(pk));
	}

	/**
	 * Create foreign key
	 * 
	 * @param fk to be created
	 */
	public void createFK(FK fk) {
		String ddl = CUBRIDSQLHelper.getInstance(null).getFKDDL(fk.getTable().getName(), fk);
		fk.setDDL(ddl);
		executeDDL(ddl + ";\n", true, createResultHandler(fk));
	}

	/**
	 * Create index
	 * 
	 * @param index to be created
	 */
	public void createIndex(Index index) {
		String ddl = CUBRIDSQLHelper.getInstance(null).getIndexDDL(index.getTable().getName(),
				index, "");
		index.setDDL(ddl);
		executeDDL(ddl + ";\n", true, createResultHandler(index));
	}

	/**
	 * Create sequence
	 * 
	 * @param sq the sequence to be created.
	 */
	public void createSequence(Sequence sq) {
		String ddl = CUBRIDSQLHelper.getInstance(null).getSequenceDDL(sq);
		sq.setDDL(ddl);
		executeDDL(ddl + ";\n", false, createResultHandler(sq));
	}
	
	public int importVertexHeader(Vertex v) {
		importVertexToCSV(v);
		
		return 0;
	}
	
	public int importVertex(Vertex v, List<Record> records) {
		//TODO need vertex ddl
		//TODO executeDDL(ddl, true, resultHandler)
		
		if (config.targetIsCSV()) {
			importVertexToCSV(records, v);
			
			return 0;
		} else {
		
			int recCounter = 0;
			int commitCount = config.getCommitCount();
			
			StringBuffer buffer = new StringBuffer();
			
			for (Record rec : records) {
				
				String ddl = GraphSQLHelper.getInstance(null).getVertexDDL(v, rec);
				buffer.append(ddl);
				buffer.append(";\n");
				recCounter++;
				
				if (recCounter == commitCount) {
					executeDDL(buffer.toString(), false, createResultHandler(v, recCounter));
					
					recCounter = 0;
				}
			}
			executeDDL(buffer.toString() + ";\n", false, createResultHandler(v, recCounter));
			
			return 0;
		}
	}
	
	public int importEdgeHeader(Edge e) {
		importEdgeToCSV(e);
		
		return 0;
	}
	
	public int importEdge(Edge e, List<Record> records) {
		//TODO need edge ddl
		//TODO executeDDL(ddl, true, resultHandler)
		
		if (config.targetIsCSV()) {
			importEdgeToCSV(records, e);
			
			return 0;
		} else {
			int commitCount = config.getCommitCount();
			int recCounter = 0;
			
			StringBuffer buffer = new StringBuffer();
			
			if (records == null) {
				for (int i=0 ; i < e.getfkCol2RefMappingSize(); i++) {
					String ddl = GraphSQLHelper.getInstance(null).getEdgeDDL(e, i);
					
					buffer.append(ddl);
					buffer.append(";\n");
					recCounter++;
					
					if (recCounter == commitCount) {
						executeDDL(buffer.toString(), false, createResultHandler(e, recCounter));
						
						recCounter = 0;
						buffer = new StringBuffer();
					}
				}
				
				executeDDL(buffer.toString(), false, createResultHandler(e, recCounter));
				
			} else {
				for (Record rc : records) {
				
					if (rc == null) {
						continue;
					}
					String ddl = GraphSQLHelper.getInstance(null).getEdgeDDL(e, rc);
					buffer.append(ddl);
					buffer.append(";\n");
					recCounter++;
					
					if (recCounter == commitCount) {
						executeDDL(buffer.toString(), false, createResultHandler(e, recCounter));
						
						recCounter = 0;
						buffer = new StringBuffer();
					}
				}
				executeDDL(buffer.toString(), false, createResultHandler(e, recCounter));
			}
			
			return 0;
		}
	}

	public void importVertexToCSV(Vertex v) {
		String tmpDataFileName = getRandomTempFileName() + config.getDataFileExt();
		File file = new File(tmpDataFileName);
		
		try {
			if (v != null) {
				int counter = writeGraphHeader(file, v);
				
//				RunnableResultHandler resultHandler = createResultHandler(v, counter);
				
				handleDataFileHeader(tmpDataFileName, v, counter, 1);
			}
		} catch (Exception exception) {
			// TODO Auto-generated catch block
			exception.printStackTrace();
		}
	}
	
	public void importVertexToCSV(List<Record> records, Vertex v) {
		String tmpDataFileName = getRandomTempFileName() + config.getDataFileExt();
		File file = new File(tmpDataFileName);
		
		try {
			if (records != null) {
				int counter = writeGraphData(records, file, v);
				
				RunnableResultHandler resultHandler = createResultHandler(v, counter);
				
				handleDataFile(tmpDataFileName, v, counter, records.size());
			}
		} catch (Exception exception) {
			// TODO Auto-generated catch block
			exception.printStackTrace();
		}
	}
	
	public void importEdgeToCSV(Edge e) {
		String tmpDataFileName = getRandomTempFileName() + config.getDataFileExt();
		File file = new File(tmpDataFileName);
		
		try {
			if (e != null) {
				int counter = writeGraphHeader(file, e);
				
//				RunnableResultHandler resultHandler = createResultHandler(e, counter);
				
				handleDataFileHeader(tmpDataFileName, e, counter, 1);
			}
		} catch (Exception exception) {
			// TODO Auto-generated catch block
			exception.printStackTrace();
		}
	}
	
	public void importEdgeToCSV(List<Record> records, Edge e) {
		String tmpDataFileName = getRandomTempFileName() + config.getDataFileExt();
		File file = new File(tmpDataFileName);
		
		try {
			if (records != null) {
				int counter = writeGraphData(records, file, e);
				
				RunnableResultHandler resultHandler = createResultHandler(e, counter);
				
				handleDataFile(tmpDataFileName, e, counter, records.size());
			}
		} catch (Exception exception) {
			// TODO Auto-generated catch block
			exception.printStackTrace();
		}
	}
	
	public int writeGraphHeader(File file, Object obj) throws Exception {
		CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(file),
				config.getTargetCharSet()), config.getCsvSettings().getSeparateChar(),
				config.getCsvSettings().getQuoteChar(), config.getCsvSettings().getEscapeChar());
		
		if (obj instanceof Edge) {
			Edge edge = (Edge) obj;
			
			List<String> header = new ArrayList<String>();
			
			List<Column> edgeColumnList = edge.getColumnList();
			for (Column col : edgeColumnList) {
				
				if (col.getGraphDataType().equals("not support")) {
					continue;
				}
				
				StringBuffer sb = new StringBuffer();
				
				if (col.getDataType().equals("ID")) {
					sb.append(col.getName());
				} else {
					sb.append(col.getName());
					sb.append(":");
					sb.append(col.getGraphDataType().toUpperCase());
				}
				
				header.add(sb.toString());
			}
			
			writer.writeNext(header.toArray(new String[header.size()]));
			
		} else if (obj instanceof Vertex) {
			Vertex vertex = (Vertex) obj;
			
			List<String> header = new ArrayList<String>();
			
			List<Column> vertexColumnList = vertex.getColumnList();
			for (Column col : vertexColumnList) {
				
				if (col.getGraphDataType().equals("not support")) {
					continue;
				}
				
				StringBuffer sb = new StringBuffer();
				
				sb.append(col.getName());
				sb.append(":");
				sb.append(col.getGraphDataType().toUpperCase());
				
				header.add(sb.toString());
			}
			
			writer.writeNext(header.toArray(new String[header.size()]));
		}
		
		writer.flush();
		
		return 0;
	}
	
	public int writeGraphData(List<Record> records, File file, Object obj) throws Exception {
		
		CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(file),
				config.getTargetCharSet()), config.getCsvSettings().getSeparateChar(),
				config.getCsvSettings().getQuoteChar(), config.getCsvSettings().getEscapeChar());
		
		try {
			int total = 0;
			for (Record re : records) {
				if (re == null) {
					continue;
				}
				List<String> res = getGraphRecords(re);
				if (res == null) {
					continue;
				}
				writer.writeNext(res.toArray(new String[res.size()]));
				total++;
			}
			writer.flush();
			return total;
		} finally {
			writer.close();
		}
	}
	
	public List<String> getGraphRecords(Record record) {
		List<String> recordList = new ArrayList<String>();
		
		List<ColumnValue> colValList = record.getColumnValueList();
		
		for (ColumnValue colVal : colValList) {
			if (colVal.getValue() == null) {
				continue;
			}
			
			recordList.add(colVal.getValue().toString());
		}
		
		return recordList;
	}
}
