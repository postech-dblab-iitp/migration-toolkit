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
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.dbobject.Record;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.engine.MigrationContext;
import com.cubrid.cubridmigration.core.engine.MigrationDirAndFilesManager;
import com.cubrid.cubridmigration.core.engine.MigrationStatusManager;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.event.ImportGraphRecordsEvent;
import com.cubrid.cubridmigration.core.engine.event.ImportRecordsEvent;
import com.cubrid.cubridmigration.core.engine.exception.NormalMigrationException;
import com.cubrid.cubridmigration.core.engine.task.FileMergeRunnable;
import com.cubrid.cubridmigration.core.engine.task.RunnableResultHandler;
import com.cubrid.cubridmigration.cubrid.Data2StrTranslator;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

/**
 * LoadDBImporter : Use LoadDB and CSQL commands to import database objects.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-8-3 created by Kevin Cao
 */
public class LoadFileImporter extends
		OfflineImporter {
	/**
	 * ExportStatus records the export status
	 * 
	 * @author Kevin Cao
	 * @version 1.0 - 2013-3-5 created by Kevin Cao
	 */
	private static class CurrentDataFileInfo {
		String fileHeader;
		String fileFullName;
		int currentFileNO = 1;
		String fileExt;

		public CurrentDataFileInfo(String header, String ext) {
			this.fileHeader = header;
			this.fileExt = ext;
			this.fileFullName = header + ext;
		}

		/**
		 * Create next file.
		 * 
		 */
		public void nextFile() {
			final StringBuffer sb = new StringBuffer(fileHeader);
			currentFileNO++;
			sb.append("_").append(currentFileNO);
			fileFullName = sb.append(fileExt).toString();
			//If has old file ,remove it firstly
			PathUtils.deleteFile(new File(fileFullName));
		}
	}

	protected final static Logger LOGGER = LogUtil.getLogger(LoadFileImporter.class);

	private final Map<String, CurrentDataFileInfo> tableFiles = new HashMap<String, CurrentDataFileInfo>();

	private final Object lockObj = new Object();

	public LoadFileImporter(MigrationContext mrManager) {
		super(mrManager);
		unloadFileUtil = new Data2StrTranslator(mrManager.getDirAndFilesMgr().getMergeFilesDir(),
				config, config.getDestType());
		createGraphListFile();
		
	}

	/**
	 * 
	 * Execute merge file tasks
	 * 
	 * @param sourceFile is file to be read.
	 * @param targetFile specify schema or data
	 * @param listener call back method
	 * @param deleteFile delete file after task finished
	 * @param isSchemaFile true if file is schema file
	 */
	private void executeTask(String sourceFile, String targetFile, RunnableResultHandler listener,
			boolean deleteFile, boolean isSchemaFile) {
		cmTaskService.execute(new FileMergeRunnable(sourceFile, targetFile,
				config.getTargetCharSet(), listener, deleteFile, isSchemaFile
						|| !config.targetIsXLS()));
	}

	/**
	 * Send schema file and data file to server for loadDB command.
	 * 
	 * @param fileName the file to be sent.
	 * @param stc source table configuration.
	 * @param impCount the count of records in file.
	 * @param expCount exported record count
	 */
	protected void handleDataFile(String fileName, final SourceTableConfig stc, final int impCount,
			final int expCount) {
		synchronized (lockObj) {
			MigrationDirAndFilesManager mdfm = mrManager.getDirAndFilesMgr();
			CurrentDataFileInfo es = tableFiles.get(stc.getName());
			if (es == null) {
				final StringBuffer sb = new StringBuffer(
						mrManager.getDirAndFilesMgr().getMergeFilesDir()).append(
						config.getFullTargetFilePrefix()).append(stc.getTarget());
				es = new CurrentDataFileInfo(sb.toString(), config.getDataFileExt());
				PathUtils.deleteFile(new File(es.fileFullName));
				tableFiles.put(stc.getName(), es);
			}
			//If the target file is full. 
			if (mdfm.isDataFileFull(es.fileFullName)) {
				//Full name will be changed.
				es.nextFile();
			}
			final String fileFullName = es.fileFullName;
			mdfm.addDataFile(fileFullName, impCount);
			executeTask(fileName, fileFullName, new RunnableResultHandler() {

				public void success() {
					eventHandler.handleEvent(new ImportRecordsEvent(stc, impCount));
					final MigrationStatusManager sm = mrManager.getStatusMgr();
					sm.addImpCount(stc.getOwner(), stc.getName(), expCount);
					//CSV, XLS file will not be merged into one data file.
					if (config.targetIsCSV() || config.targetIsXLS()) {
						return;
					}
					if (config.isOneTableOneFile()) {
						return;
					}
					final Table st = config.getSrcTableSchema(stc.getOwner(), stc.getName());
					if (null == st) {
						return;
					}
					final long totalEc = sm.getExpCount(stc.getOwner(), stc.getName());
					final long totalIc = sm.getImpCount(stc.getOwner(), stc.getName());
					final boolean expEnd = sm.getExpFlag(stc.getOwner(), stc.getName());
					//If it is the last merging,Merge data files to one data file
					if (expEnd && totalEc == totalIc) {
						executeTask(fileFullName, config.getTargetDataFileName(), null, true, false);
					}
				}

				public void failed(String error) {
					mrManager.getStatusMgr().addImpCount(stc.getOwner(), stc.getName(), expCount);
					eventHandler.handleEvent(new ImportRecordsEvent(stc, impCount,
							new NormalMigrationException(error), null));
				}
			}, config.isDeleteTempFile(), false);
		}
	}

	protected void handleDataFile(String fileName, final Edge e, final int impCount,
			final int expCount) {
		synchronized (lockObj) {
			
			String edgeLabel;
			
			if (e.getEdgeType() == Edge.TWO_WAY_TYPE || e.getEdgeType() == Edge.JOIN_TWO_WAY_TYPE) {
				edgeLabel = e.getEdgeLabel() + "_rev";
			} else {
				edgeLabel = e.getEdgeLabel();
			}
			
			MigrationDirAndFilesManager mdfm = mrManager.getDirAndFilesMgr();
			CurrentDataFileInfo es = tableFiles.get(removeSpace(edgeLabel));
			if (es == null) {
				final StringBuffer sb = new StringBuffer(
						mrManager.getDirAndFilesMgr().getMergeFilesDir()).append(
						config.getFullTargetFilePrefix()).append(removeSpace(edgeLabel + "_Edge"));
				es = new CurrentDataFileInfo(sb.toString(), config.getDataFileExt());
				PathUtils.deleteFile(new File(es.fileFullName));
				tableFiles.put(removeSpace(edgeLabel), es);
			}
			//If the target file is full. 
			if (mdfm.isDataFileFull(es.fileFullName)) {
				//Full name will be changed.
				es.nextFile();
			}
			final String fileFullName = es.fileFullName;
			mdfm.addDataFile(fileFullName, impCount);
			executeTask(fileName, fileFullName, new RunnableResultHandler() {
				
				public void success() {
					
					String edgeLabel;
					
					if (e.getEdgeType() == Edge.TWO_WAY_TYPE || e.getEdgeType() == Edge.JOIN_TWO_WAY_TYPE) {
						edgeLabel = e.getEdgeLabel() + "_rev";
					} else {
						edgeLabel = e.getEdgeLabel();
					}
					
					eventHandler.handleEvent(new ImportGraphRecordsEvent(e, impCount));
					final MigrationStatusManager sm = mrManager.getStatusMgr();
					sm.addImpCount(e.getOwner(), e.getEdgeLabel(), expCount);
					//CSV, XLS file will not be merged into one data file.
					if (config.targetIsCSV() || config.targetIsXLS()) {
						return;
					}
					if (config.isOneTableOneFile()) {
						return;
					}
					final Table st = config.getSrcTableSchema(e.getOwner(), e.getEdgeLabel());
					if (null == st) {
						return;
					}
					final long totalEc = sm.getExpCount(e.getOwner(), edgeLabel);
					final long totalIc = sm.getImpCount(e.getOwner(), edgeLabel);
					final boolean expEnd = sm.getExpFlag(e.getOwner(), edgeLabel);
					//If it is the last merging,Merge data files to one data file
					if (expEnd && totalEc == totalIc) {
						executeTask(fileFullName, config.getTargetDataFileName(), null, true, false);
					}
				}

				public void failed(String error) {
					
					String edgeLabel;
					
					if (e.getEdgeType() == Edge.TWO_WAY_TYPE || e.getEdgeType() == Edge.JOIN_TWO_WAY_TYPE) {
						edgeLabel = e.getEdgeLabel() + "_rev";
					} else {
						edgeLabel = e.getEdgeLabel();
					}
					
					mrManager.getStatusMgr().addImpCount(e.getOwner(), edgeLabel, expCount);
					eventHandler.handleEvent(new ImportGraphRecordsEvent(e, impCount,
							new NormalMigrationException(error), null));
				}
			}, config.isDeleteTempFile(), false);
		}
	}
	
	protected void handleDataFile(String fileName, final Vertex v, final int impCount,
			final int expCount) {
		synchronized (lockObj) {
			MigrationDirAndFilesManager mdfm = mrManager.getDirAndFilesMgr();
			CurrentDataFileInfo es = tableFiles.get(removeSpace(v.getVertexLabel()));
			if (es == null) {
				final StringBuffer sb = new StringBuffer(
						mrManager.getDirAndFilesMgr().getMergeFilesDir()).append(
						config.getFullTargetFilePrefix()).append(removeSpace(v.getVertexLabel() + "_Node"));
				es = new CurrentDataFileInfo(sb.toString(), config.getDataFileExt());
				PathUtils.deleteFile(new File(es.fileFullName));
				tableFiles.put(removeSpace(v.getVertexLabel()), es);
			}
			//If the target file is full. 
			if (mdfm.isDataFileFull(es.fileFullName)) {
				//Full name will be changed.
				es.nextFile();
			}
			final String fileFullName = es.fileFullName;
			mdfm.addDataFile(fileFullName, impCount);
			executeTask(fileName, fileFullName, new RunnableResultHandler() {

				public void success() {
					eventHandler.handleEvent(new ImportGraphRecordsEvent(v, impCount));
					final MigrationStatusManager sm = mrManager.getStatusMgr();
					sm.addImpCount(v.getOwner(), v.getVertexLabel(), expCount);
					//CSV, XLS file will not be merged into one data file.
					if (config.targetIsCSV() || config.targetIsXLS()) {
						return;
					}
					if (config.isOneTableOneFile()) {
						return;
					}
					final Table st = config.getSrcTableSchema(v.getOwner(), v.getVertexLabel());
					if (null == st) {
						return;
					}
					final long totalEc = sm.getExpCount(v.getOwner(), v.getVertexLabel());
					final long totalIc = sm.getImpCount(v.getOwner(), v.getVertexLabel());
					final boolean expEnd = sm.getExpFlag(v.getOwner(), v.getVertexLabel());
					//If it is the last merging,Merge data files to one data file
					if (expEnd && totalEc == totalIc) {
						executeTask(fileFullName, config.getTargetDataFileName(), null, true, false);
					}
				}

				public void failed(String error) {
					mrManager.getStatusMgr().addImpCount(v.getOwner(), v.getVertexLabel(), expCount);
					eventHandler.handleEvent(new ImportGraphRecordsEvent(v, impCount,
							new NormalMigrationException(error), null));
				}
			}, config.isDeleteTempFile(), false);
		}
	}
	
	@Override
	protected void handleListFileHeaderTurboGraph() {
		synchronized (lockObj) {
			MigrationDirAndFilesManager mdfm = mrManager.getDirAndFilesMgr();
			
			File file = new File(mdfm.getGraphListFile());
			FileWriter fileWriter = null;
			BufferedWriter bufferWriter = null;
			
			try {
				if (file.exists()) {
					fileWriter = new FileWriter(file, true);
					bufferWriter = new BufferedWriter(fileWriter);
					
					StringBuffer buffer = new StringBuffer();
					buffer.append("#!/bin/bash");
					buffer.append("\n");
					buffer.append("\n");
					
					buffer.append("shell_dir=\"$( cd \"$( dirname \"$0\")\" && pwd -P )\"");
					buffer.append("\n");
					buffer.append("base_dir=$shell_dir/");
					buffer.append("\n");
					buffer.append("execute_tools=\"(enter execute tools path here)\"");
					buffer.append("\n");
					buffer.append("\n");
					
					buffer.append("echo \"base_dir : $base_dir\"");
					buffer.append("\n");
					buffer.append("echo \"execute_tools : $execute_tools\"");
					buffer.append("\n");
					buffer.append("\n");
					
					buffer.append("${execute_tools} \\");
					buffer.append("\n");
					
					buffer.append("\t--output_dir:\"(enter output dir path here)\"");
					
					bufferWriter.write(buffer.toString());
					
					bufferWriter.close();
					
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Closer.close(bufferWriter);
				Closer.close(fileWriter);
			}
		}
	}
	
	@Override
	protected void handleListFileHeaderForNEO4J() {
		synchronized (lockObj) {
			MigrationDirAndFilesManager mdfm = mrManager.getDirAndFilesMgr();
			
			File file = new File(mdfm.getGraphListFile());
			FileWriter fileWriter = null;
			BufferedWriter bufferWriter = null;
			
			try {
				if (file.exists()) {
					fileWriter = new FileWriter(file, true);
					bufferWriter = new BufferedWriter(fileWriter);
					
					StringBuffer buffer = new StringBuffer();
					buffer.append("NEO4J_HOME=\n");
					buffer.append("OUTPUT_DIR=\n");
					buffer.append("DATABASE_NAME=\n");
					buffer.append("${NEO4J_HOME}bin" + File.separator + "neo4j-admin import --database ${DATABASE_NAME}" );
					
					bufferWriter.write(buffer.toString());
					
					bufferWriter.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Closer.close(bufferWriter);
				Closer.close(fileWriter);
			}
		}
	}
	
	protected void handleDataFileHeader(String fileName, final Edge e, final int impCount,
			final int expCount) {
		synchronized (lockObj) {
			MigrationDirAndFilesManager mdfm = mrManager.getDirAndFilesMgr();
			
			String edgeLabelCheck;
			
			if (e.getEdgeType() == Edge.TWO_WAY_TYPE || e.getEdgeType() == Edge.JOIN_TWO_WAY_TYPE) {
				edgeLabelCheck = e.getEdgeLabel() + "_rev";
			} else {
				edgeLabelCheck = e.getEdgeLabel();
			}
			
			CurrentDataFileInfo es = tableFiles.get(removeSpace(edgeLabelCheck));
			if (es == null) {
				final StringBuffer sb = new StringBuffer(
						mrManager.getDirAndFilesMgr().getMergeFilesDir()).append(
						config.getFullTargetFilePrefix());
						sb.append(removeSpace(edgeLabelCheck + "_Edge"));
						
				es = new CurrentDataFileInfo(sb.toString(), config.getDataFileExt());
				PathUtils.deleteFile(new File(es.fileFullName));
				tableFiles.put(removeSpace(edgeLabelCheck), es);
				
				if (config.targetIsCSV()) {
					String inputFileName = config.getFullTargetFilePrefix() + removeSpace(edgeLabelCheck + "_Edge") + config.getDataFileExt();						
					
					if (config.getGraphSubTypeForCSV() == 1) {
						writeGraphListFileForTurboGraph(mdfm.getGraphListFile(), inputFileName, e);
					} else {
						writeGraphListFileForNEO4J(mdfm.getGraphListFile(), inputFileName, e);
					}
				}
			}
			//If the target file is full. 
			if (mdfm.isDataFileFull(es.fileFullName)) {
				//Full name will be changed.
				es.nextFile();
			}
			final String fileFullName = es.fileFullName;
			mdfm.addDataFile(fileFullName, impCount);
			executeTask(fileName, fileFullName, new RunnableResultHandler() {

				public void success() {
					
					String edgeLabel;
					
					if (e.getEdgeType() == Edge.TWO_WAY_TYPE || e.getEdgeType() == Edge.JOIN_TWO_WAY_TYPE) {
						edgeLabel = e.getEdgeLabel() + "_rev";
					} else {
						edgeLabel = e.getEdgeLabel();
					}
					
					final MigrationStatusManager sm = mrManager.getStatusMgr();
					sm.addImpCount(e.getOwner(), e.getEdgeLabel(), expCount);
					//CSV, XLS file will not be merged into one data file.
					if (config.targetIsCSV() || config.targetIsXLS()) {
						return;
					}
					if (config.isOneTableOneFile()) {
						return;
					}
					final Table st = config.getSrcTableSchema(e.getOwner(), edgeLabel);
					if (null == st) {
						return;
					}
					final long totalEc = sm.getExpCount(e.getOwner(), edgeLabel);
					final long totalIc = sm.getImpCount(e.getOwner(), edgeLabel);
					final boolean expEnd = sm.getExpFlag(e.getOwner(), edgeLabel);
					//If it is the last merging,Merge data files to one data file
					if (expEnd && totalEc == totalIc) {
						executeTask(fileFullName, config.getTargetDataFileName(), null, true, false);
					}
				}

				public void failed(String error) {
					
					String edgeLabel;
					
					if (e.getEdgeType() == Edge.TWO_WAY_TYPE || e.getEdgeType() == Edge.JOIN_TWO_WAY_TYPE) {
						edgeLabel = e.getEdgeLabel() + "_rev";
					} else {
						edgeLabel = e.getEdgeLabel();
					}
					
					mrManager.getStatusMgr().addImpCount(e.getOwner(), edgeLabel, expCount);
					eventHandler.handleEvent(new ImportGraphRecordsEvent(e, impCount,
							new NormalMigrationException(error), null));
				}
			}, config.isDeleteTempFile(), false);
		}
	}
	
	protected void handleDataFileHeader(String fileName, final Vertex v, final int impCount,
			final int expCount) {
		synchronized (lockObj) {
			MigrationDirAndFilesManager mdfm = mrManager.getDirAndFilesMgr();
			CurrentDataFileInfo es = tableFiles.get(removeSpace(v.getVertexLabel()));
			if (es == null) {
				final StringBuffer sb = new StringBuffer(
						mrManager.getDirAndFilesMgr().getMergeFilesDir()).append(
						config.getFullTargetFilePrefix()).append(removeSpace(v.getVertexLabel() + "_Node"));
				es = new CurrentDataFileInfo(sb.toString(), config.getDataFileExt());
				PathUtils.deleteFile(new File(es.fileFullName));
				tableFiles.put(removeSpace(v.getVertexLabel()), es);
				
				if (config.targetIsCSV()) {
					final String inputFileName = 
							config.getFullTargetFilePrefix() + removeSpace(v.getVertexLabel() + "_Node") + config.getDataFileExt();
					
					if (config.getGraphSubTypeForCSV() == 1) {
						writeGraphListFileForTurboGraph(mdfm.getGraphListFile(), inputFileName, v);
					} else {
						writeGraphListFileForNEO4J(mdfm.getGraphListFile(), inputFileName, v);
					}
				}
			}
			//If the target file is full. 
			if (mdfm.isDataFileFull(es.fileFullName)) {
				//Full name will be changed.
				es.nextFile();
			}
			final String fileFullName = es.fileFullName;
			mdfm.addDataFile(fileFullName, impCount);
			executeTask(fileName, fileFullName, new RunnableResultHandler() {

				public void success() {
					final MigrationStatusManager sm = mrManager.getStatusMgr();
					sm.addImpCount(v.getOwner(), v.getVertexLabel(), expCount);
					//CSV, XLS file will not be merged into one data file.
					if (config.targetIsCSV() || config.targetIsXLS()) {
						return;
					}
					if (config.isOneTableOneFile()) {
						return;
					}
					final Table st = config.getSrcTableSchema(v.getOwner(), v.getVertexLabel());
					if (null == st) {
						return;
					}
					final long totalEc = sm.getExpCount(v.getOwner(), v.getVertexLabel());
					final long totalIc = sm.getImpCount(v.getOwner(), v.getVertexLabel());
					final boolean expEnd = sm.getExpFlag(v.getOwner(), v.getVertexLabel());
					//If it is the last merging,Merge data files to one data file
					if (expEnd && totalEc == totalIc) {
						executeTask(fileFullName, config.getTargetDataFileName(), null, true, false);
					}
				}

				public void failed(String error) {
					mrManager.getStatusMgr().addImpCount(v.getOwner(), v.getVertexLabel(), expCount);
					eventHandler.handleEvent(new ImportGraphRecordsEvent(v, impCount,
							new NormalMigrationException(error), null));
				}
			}, config.isDeleteTempFile(), false);
		}
	}
	
	/**
	 * Send schema file and data file to server for loadDB command.
	 * 
	 * @param fileName the file to be sent.
	 * @param tableName tableName
	 * 
	 */
	protected void sendLOBFile(String fileName, String tableName) {
		///home/cmt/CUBRID/database/mt/log/blob1/blob.xxxxxxx
		String targetFile = getLOBDir(tableName) + new File(fileName).getName();
		//Copy to target if it is local
		try {
			CUBRIDIOUtils.mergeFile(fileName, targetFile);
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}
	}

	/**
	 * Send schema file and data file to server for loadDB command.
	 * 
	 * @param fileName the file to be sent.
	 * @param listener a call interface.
	 * @param isIndex true if the DDL is about index
	 */
	protected void sendSchemaFile(String fileName, RunnableResultHandler listener, boolean isIndex) {
		executeTask(fileName,
				isIndex ? config.getTargetIndexFileName() : config.getTargetSchemaFileName(),
				listener, config.isDeleteTempFile(), true);
	}

	/**
	 * Get lob files directory
	 * 
	 * @param tableName string
	 * @return lob directory
	 */
	protected String getLOBDir(String tableName) {
		return mrManager.getDirAndFilesMgr().getLobFilesDir() + tableName + File.separatorChar;
	}
	
	protected String removeSpace(String label) {
		return label.trim().replaceAll(" ", "_"); 
	}

	public int importCDCObject(Vertex vertex, Edge e, List<Record> records) {
		return 0;
	}

	
	protected void createGraphListFile () {
		final String listFile =	mrManager.getDirAndFilesMgr().getGraphListFile();
		File file = new File(listFile);
		try {
			if (file.exists()) {
				file.delete();
				file.createNewFile();
			} else {
				file.createNewFile();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected void writeGraphListFileForTurboGraph (String listFile, String inputFile, Object gInstance) {
		File file = new File(listFile);
		try {
			if (file.exists()) {
				 FileWriter fw = new FileWriter(file, true);
				 BufferedWriter writer = new BufferedWriter(fw);
				 StringBuffer sb = new StringBuffer();
				 if (gInstance instanceof Vertex) {
					 Vertex v = (Vertex) gInstance;
					 sb.append("\t--nodes:")
					 .append(v.getVertexLabel().replaceAll(" ", "_").toUpperCase());
				 } else if (gInstance instanceof Edge) {
					 Edge e = (Edge) gInstance;
					 
					 if (e.getEdgeType() == Edge.JOIN_TWO_WAY_TYPE)
						 sb.append("\t--relationships_backward:");
					 else
						 sb.append("\t--relationships:")
					 .append(e.getEdgeLabel().replaceAll(" ", "_").toUpperCase());
				 }
				 sb.append(" ${base_dir}")
				 .append(inputFile);
				 writer.write(" \\\n" + sb.toString());

				 writer.close();
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected void writeGraphListFileForNEO4J (String listFile, String inputFile, Object gInstance) {
		File file = new File(listFile);
		try {
			if (file.exists()) {
				 FileWriter fw = new FileWriter(file, true);
				 BufferedWriter writer = new BufferedWriter(fw);
				 StringBuffer sb = new StringBuffer();
				 if (gInstance instanceof Vertex) {
					 Vertex v = (Vertex) gInstance;
					 sb.append("\t--nodes=")
					 .append(v.getVertexLabel().replaceAll(" ", "_"))
					 .append("=");
				 } else if (gInstance instanceof Edge) {
					 Edge e = (Edge) gInstance;
					 sb.append("\t--relationships=")
					 .append(e.getEdgeLabel().replaceAll(" ", "_"))
					 .append("=");
				 }
				 sb.append(" ${OUTPUT_DIR}")
				 .append(inputFile);
				 writer.write(" \\\n" + sb.toString());

				 writer.close();
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
