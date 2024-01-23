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
package com.cubrid.cubridmigration.command.handler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.command.CmdMigrationMonitor;
import com.cubrid.cubridmigration.command.ConsoleCommandHandler;
import com.cubrid.cubridmigration.command.ConsoleMigrationReporter;
import com.cubrid.cubridmigration.command.ConsoleUtils;
import com.cubrid.cubridmigration.command.DoMigration;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.TimeZoneUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.connection.JDBCDriverManager;
import com.cubrid.cubridmigration.core.dbmetadata.DBSchemaInfoFetcherFactory;
import com.cubrid.cubridmigration.core.dbmetadata.IDBSchemaInfoFetcher;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.MigrationProcessManager;
import com.cubrid.cubridmigration.core.engine.ThreadUtils;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.report.DBObjMigrationResult;
import com.cubrid.cubridmigration.core.engine.report.DataFileImportResult;
import com.cubrid.cubridmigration.core.engine.report.MigrationBriefReport;
import com.cubrid.cubridmigration.core.engine.report.MigrationOverviewResult;
import com.cubrid.cubridmigration.core.engine.report.MigrationReport;
import com.cubrid.cubridmigration.core.engine.report.RecordMigrationResult;
import com.cubrid.cubridmigration.core.engine.template.MigrationTemplateParser;
import com.cubrid.cubridmigration.cubrid.CUBRIDTimeUtil;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.mysql.trans.MySQL2CUBRIDMigParas;

/**
 * StartCommandHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-1-2 created by Kevin Cao
 */
public class StartCommandHandler implements
		ConsoleCommandHandler {
	private static Logger LOG = LogUtil.getLogger(DoMigration.class);

	//Source DB name from db.conf
	private String srcDBName;
	//Target DB name from db.conf
	private String destDBName;
	private String sourceDriverPath;
	private String targetDriverPath;
	private String targetPath;
	private String sourceXML;
	private boolean migrateDataOnly;

	//0:default, only progress; 1:errors and progress; 2:all messages and events.
	private int monitorMode = MigrationConfiguration.RPT_LEVEL_BRIEF;
	private int reportMode = -1;

	private PrintStream outPrinter = System.out;

	private Properties dbProperties = new Properties();

	/**
	 * Build source database's schema for migration
	 * 
	 * @param config read from template file.
	 * @throws Exception error JDBC
	 */
	private Catalog buildSourceSchema(MigrationConfiguration config) {
		Catalog result = null;
		try {
			if (migrateDataOnly) {
				result = config.buildSourceSchemaForDataMigration();
			} else {
				result = config.buildRequiredSourceSchema();
			}
		} catch (Exception ex) {
			outPrinter.println("Get schema information error:" + ex.getMessage());
			LOG.error("", ex);
			return null;
		}
		if (result == null) {
			outPrinter.println("Can not get schema information.");
		}
		return result;
	}

	/**
	 * Check and add jdbc driver.
	 * 
	 * @param dt DatabaseType
	 * @param driverPath driverPath
	 * @return true if successfully
	 */
	private boolean checkJDBCDriver(DatabaseType dt, String driverPath) {
		if (dt.getJDBCData(driverPath) == null
				&& !JDBCDriverManager.getInstance().addDriver(driverPath, false)) {
			return false;
		}
		return true;
	}

	/**
	 * Parsing migration configuration and initialize the parameters of
	 * configuration.
	 * 
	 * @param file configuration file
	 * @return MigrationConfiguration
	 */
	private MigrationConfiguration getConfig(String file) {
		try {
			MigrationConfiguration config = MigrationTemplateParser.parse(file);
			config.setName(new File(file).getName());
			if (!initializeSource(config) || !initializeTarget(config)) {
				printHelp();
				return null;
			}
			if (config.sourceIsOnline() || config.sourceIsXMLDump()) {
				outPrinter.println("Reading source database schema ...");

				Catalog catalog = buildSourceSchema(config);
				if (catalog == null) {
					return null;
				}
				config.setSrcCatalog(catalog, false);
				config.getSourceDBType().getExportHelper().fillTablesRowCount(config);
			} else if (config.sourceIsCSV()) {
				IDBSchemaInfoFetcher bcf = DBSchemaInfoFetcherFactory.createFetcher(config.getTargetConParams());
				Catalog cl = bcf.fetchSchema(config.getTargetConParams(), null);
				if (cl == null || cl.getSchemas().isEmpty()) {
					outPrinter.println("Invalid target database.");
					return null;
				}
				config.reparseCSVFiles(cl.getSchemas().get(0));
				if (config.getCSVConfigs().isEmpty()) {
					outPrinter.println("There is no CSV file found.");
					return null;
				}
			}
			config.cleanNoUsedConfigForStart();
			if (config.hasOtherParam()) {
				String s1 = config.getOtherParam(MySQL2CUBRIDMigParas.UNPARSED_TIME);
				MySQL2CUBRIDMigParas.putMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_TIME,
						StringUtils.isEmpty(s1) ? null : s1);
				String s2 = config.getOtherParam(MySQL2CUBRIDMigParas.UNPARSED_DATE);
				MySQL2CUBRIDMigParas.putMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_DATE,
						StringUtils.isEmpty(s2) ? null : s2);
				String s3 = config.getOtherParam(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP);
				MySQL2CUBRIDMigParas.putMigrationParamter(MySQL2CUBRIDMigParas.UNPARSED_TIMESTAMP,
						StringUtils.isEmpty(s3) ? null : s3);
				String s4 = config.getOtherParam(MySQL2CUBRIDMigParas.REPLAXE_CHAR0);
				MySQL2CUBRIDMigParas.putMigrationParamter(MySQL2CUBRIDMigParas.REPLAXE_CHAR0,
						StringUtils.isEmpty(s4) ? null : s4);
			}
			if (reportMode >= 0) {
				config.setReportLevel(reportMode);
			}
			return config;
		} catch (Exception ex) {
			ex.printStackTrace();
			outPrinter.println("Create migration configuration error: " + ex.getMessage());
			LOG.error("", ex);
			return null;
		}

	}

	/**
	 * Print help.
	 * 
	 */
	private void printHelp() {
		ConsoleUtils.printHelp("/com/cubrid/cubridmigration/command/help_start.txt");
	}

	/**
	 * Retrieves the script file from args
	 * 
	 * @param args from parameter
	 * @return File
	 * @throws IOException ex
	 */
	private String getScriptFile(List<String> args) {
		String script = null;
		if (args.isEmpty()) {
			//Input full file name 
			outPrinter.print("Please specify the migration script file:");
			try {
				script = ConsoleUtils.readingInput();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} else {
			script = args.get(args.size() - 1);
		}
		script = script.trim();
		//Check parameters
		if (StringUtils.isEmpty(script)) {
			outPrinter.println("the migration script file isn't specified !");
			return null;
		}
		outPrinter.println("Reading <" + script + ">");

		File file = new File(script);
		if (!file.exists()) {
			outPrinter.println("The migration script isn't exists!");
			return null;
		}
		if (!file.isFile()) {
			outPrinter.println("The migration script should be a file!");
			return null;
		}
		return file.getAbsolutePath();
	}

	/**
	 * Initialize source database settings
	 * 
	 * @param config MigrationConfiguration
	 * @return true if successfully
	 * @throws IOException ex
	 */
	private boolean initializeSource(MigrationConfiguration config) throws IOException {
		if (config.sourceIsOnline()) {
			ConnParameters scp = config.getSourceConParams();
			if (StringUtils.isNotBlank(srcDBName)) {
				ConnParameters scpNew = getConParamFromDBProperties(srcDBName);
				if (scpNew == null) {
					outPrinter.println("Invalid DB configuration : " + srcDBName);
					return false;
				}
				if (!scpNew.getDatabaseType().equals(scp.getDatabaseType())) {
					outPrinter.println("The source database type should be : "
							+ scp.getDatabaseType().getName());
					return false;
				}
				config.setSourceConParams(scpNew);
				scp = scpNew;
			}
			if (sourceDriverPath != null) {
				scp.setDriverFileName(sourceDriverPath);
			}
			LOG.info("Source driver name:" + scp.getDriverFileName());
			if (!checkJDBCDriver(scp.getDatabaseType(), scp.getDriverFileName())) {
				outPrinter.println("Invalid driver : " + scp.getDriverFileName());
				outPrinter.println("Please specify the source database's JDBC driver by parameter [-sd].");
				outPrinter.println();
				return false;
			}
			LOG.info("Source driver name2:" + scp.getDriverFileName());
		} else if (config.sourceIsXMLDump()) {
			if (sourceXML != null) {
				config.setSourceFileName(sourceXML);
			}
			if (!new File(config.getSourceFileName()).exists()) {
				outPrinter.println("Invalid file : " + config.getSourceFileName());
				outPrinter.println("Please specify the MYSQL XML Dump File by parameter [-xml].");
				outPrinter.println();
				return false;
			}
		}
		return true;
	}

	/**
	 * initialize Target
	 * 
	 * @param config MigrationConfiguration
	 * @throws IOException ex
	 * @throws MalformedURLException ex
	 */
	private boolean initializeTarget(MigrationConfiguration config) throws IOException,
			MalformedURLException {
		if (config.targetIsOnline()) {
			//Parameter first
			ConnParameters tcp = config.getTargetConParams();
			if (StringUtils.isNotBlank(destDBName)) {
				ConnParameters tcpNew = getConParamFromDBProperties(destDBName);
				if (tcpNew == null) {
					outPrinter.println("Invalid DB configuration : " + destDBName);
					return false;
				}
				if (!tcpNew.getDatabaseType().equals(tcp.getDatabaseType())) {
					outPrinter.println("The source database type should be : "
							+ tcp.getDatabaseType().getName());
					return false;
				}
				config.setTargetConParams(tcpNew);
				tcp = tcpNew;
			}

			if (targetDriverPath != null) {
				tcp.setDriverFileName(targetDriverPath);
			}
			if (!checkJDBCDriver(tcp.getDatabaseType(), tcp.getDriverFileName())) {
				outPrinter.println("Invalid driver : " + tcp.getDriverFileName());
				outPrinter.println("Please specify the target database's JDBC driver by parameter [-td].");
				outPrinter.println();
				return false;
			}
		} else {
			if (targetPath != null) {
				config.changeTargetFilePath(targetPath);
			}
			boolean confirm;
			String tempPath = config.getFileRepositroyPath();
			if (SystemUtils.IS_OS_WINDOWS) {
				confirm = !tempPath.matches("^[A-Za-z]:\\\\.*");
			} else {
				confirm = tempPath.matches("^[A-Za-z]:\\\\.*");
			}
			if (confirm) {
				outPrinter.println("Invalid path : " + tempPath);
				outPrinter.println("Please specify the path where you want to save exported files by parameter [-tp]");
				outPrinter.println();
				return false;
			}
		}

		return true;
	}

	/**
	 * handleMigrationStart
	 * 
	 * @param argList parameters
	 * @throws IOException errors
	 */
	public void handleCommand(List<String> argList) {
		//get parameters from command line
		sourceDriverPath = ConsoleUtils.getParameter(argList, "-sd");
		targetDriverPath = ConsoleUtils.getParameter(argList, "-td");
		targetPath = ConsoleUtils.getParameter(argList, "-tp");
		sourceXML = ConsoleUtils.getParameter(argList, "-xml");
		srcDBName = ConsoleUtils.getParameter(argList, "-s");
		destDBName = ConsoleUtils.getParameter(argList, "-t");

		final String mmP = ConsoleUtils.getParameter(argList, "-mm");
		if ("error".equalsIgnoreCase(mmP)) {
			monitorMode = MigrationConfiguration.RPT_LEVEL_ERROR;
		} else if ("info".equalsIgnoreCase(mmP)) {
			monitorMode = MigrationConfiguration.RPT_LEVEL_INFO;
		} else if ("debug".equalsIgnoreCase(mmP)) {
			monitorMode = MigrationConfiguration.RPT_LEVEL_DEBUG;
		} else {
			monitorMode = MigrationConfiguration.RPT_LEVEL_BRIEF;
		}

		final String rmp = ConsoleUtils.getParameter(argList, "-rm");
		if ("error".equalsIgnoreCase(rmp)) {
			reportMode = MigrationConfiguration.RPT_LEVEL_ERROR;
		} else if ("info".equalsIgnoreCase(rmp)) {
			reportMode = MigrationConfiguration.RPT_LEVEL_INFO;
		} else if ("debug".equalsIgnoreCase(rmp)) {
			reportMode = MigrationConfiguration.RPT_LEVEL_DEBUG;
		}

		if ("yes".equalsIgnoreCase(ConsoleUtils.getParameter(argList, "-do"))) {
			migrateDataOnly = true;
		} else {
			migrateDataOnly = false;
		}

		String scriptFile = getScriptFile(argList);
		if (scriptFile == null) {
			printHelp();
			return;
		}
		loadDBProperties();
		//Create migration configuration and initialize it.
		MigrationConfiguration config = getConfig(scriptFile);
		
		if (config == null) {
			return;
		}
		
		if (config.targetIsCSV()) {
			mapVertexToEdge(config);
		}
		
		//Start migration 
		final ConsoleMigrationReporter migrationReporter = new ConsoleMigrationReporter(config,
				MigrationBriefReport.SM_USER);
		MigrationProcessManager mpm = MigrationProcessManager.getInstance(config,
				new CmdMigrationMonitor(config, monitorMode), migrationReporter);
		mpm.startMigration();
		//Waiting for migration starting, and 10s will be time out.
		int i = 0;
		while (!MigrationProcessManager.isRunning()) {
			ThreadUtils.threadSleep(1000, null);
			i++;
			if (i >= 10) {
				break;
			}
		}
		//Waiting for migration finished.
		while (MigrationProcessManager.isRunning()) {
			ThreadUtils.threadSleep(2000, null);
		}
		//print report
		printReport(migrationReporter);
	}

	/**
	 * Load db.conf configuration at the start up.
	 */
	private void loadDBProperties() {
		//dbProperties
		File dbProFile = new File(PathUtils.getInstallPath() + "db.conf");
		if (!dbProFile.exists() || dbProFile.isDirectory()) {
			return;
		}
		try {
			dbProperties.load(new FileInputStream(dbProFile));
		} catch (Exception ex) {
			outPrinter.println("Load db.conf error.");
			LOG.error(ex);
		}

	}

	/**
	 * Print report information onto screen after migration.
	 * 
	 * @param migrationReporter DefaultMigrationReporter
	 */
	private void printReport(ConsoleMigrationReporter migrationReporter) {
		MigrationReport mr = migrationReporter.getReport();
		outPrinter.println("Migration Report summary:");
		outPrinter.print("    Time used: ");
		outPrinter.print(TimeZoneUtils.format(mr.getTotalEndTime() - mr.getTotalStartTime()));
		outPrinter.println();
		List<DataFileImportResult> sqlresult = mr.getDataFileResults();
		if (sqlresult.isEmpty()) {
			for (MigrationOverviewResult mor : mr.printOverviewResults()) {
				outPrinter.print("    ");
				outPrinter.print(mor.getObjType());
				outPrinter.print(":");
				outPrinter.print(" Exported[");
				outPrinter.print(mor.getExpCount());
				outPrinter.print("]");
				outPrinter.print("; Imported[");
				outPrinter.print(mor.getImpCount());
				outPrinter.print("]");
				outPrinter.println();
			}
		} else {
			for (DataFileImportResult mor : sqlresult) {
				outPrinter.print("    ");
				outPrinter.print(mor.getFileName());
				outPrinter.print(":");
				outPrinter.print(" Exported[");
				outPrinter.print(mor.getExportCount());
				outPrinter.print("]");
				outPrinter.print("; Imported[");
				outPrinter.print(mor.getImportCount());
				outPrinter.print("]");
				outPrinter.println();
			}
		}
		//Write report to a local text file 
		String txtFile = PathUtils.getReportDir()
				+ CUBRIDTimeUtil.formatDateTime(mr.getTotalStartTime(), "yyyy_MM_dd_HH_mm_ss_SSS",
						TimeZone.getDefault()) + ".txt";
		File file = new File(txtFile);
		PathUtils.deleteFile(file);
		try {
			PathUtils.createFile(file);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(file), "utf8"));
			try {
				if (!sqlresult.isEmpty()) {
					bw.append("[Files]\r\n");
					for (DataFileImportResult sir : sqlresult) {
						bw.append(sir.getFileName()).append(":").append(" Exported:[").append(
								Long.toString(sir.getExportCount())).append("] Imported:[").append(
								Long.toString(sir.getImportCount())).append("]\r\n");
					}
					bw.flush();
					return;
				}
				bw.append("[Overview]\r\n");
				for (MigrationOverviewResult mor : mr.getOverviewResults()) {
					bw.append("    ");
					bw.append(mor.getObjType());
					bw.append(":");
					bw.append(" Exported[");
					bw.append(Long.toString(mor.getExpCount()));
					bw.append("]");
					bw.append("; Imported[");
					bw.append(Long.toString(mor.getImpCount()));
					bw.append("]\r\n");
				}
				bw.append("\r\n[Objects]\r\n");
				for (DBObjMigrationResult omr : mr.getDbObjectsResult()) {
					bw.append("    ");
					bw.append("[");
					bw.append(omr.getObjType());
					bw.append("]");
					bw.append("[");
					bw.append(omr.getObjName());
					bw.append("]");
					bw.append(": ");
					bw.append(omr.isSucceed() ? "successfully" : "failed");
					bw.append("\r\n");
					if (!omr.isSucceed()) {
						bw.append("        [DDL]:");
						bw.append(omr.getDdl());
					}
				}
				bw.append("\r\n[Records]\r\n");
				for (RecordMigrationResult rmr : mr.getRecMigResults()) {
					bw.append("    ");
					bw.append("[");
					bw.append(rmr.getSource());
					bw.append(" >> ");
					bw.append(rmr.getTarget());
					bw.append("]");
					bw.append(":");
					bw.append(" Exported:[");
					bw.append(Long.toString(rmr.getExpCount()));
					bw.append("] Imported:[");
					bw.append(Long.toString(rmr.getImpCount()));
					bw.append("]\r\n");
				}
				bw.flush();
			} finally {
				bw.close();
			}
		} catch (IOException ex) {
			LOG.error("", ex);
		}
	}

	/**
	 * Read JDBC connection information from db.conf.
	 * 
	 * @param cpname connection name
	 * @return ConnParameters
	 */
	private ConnParameters getConParamFromDBProperties(String cpname) {
		try {
			String hostIp = dbProperties.getProperty(cpname + ".host");
			int port = Integer.parseInt(dbProperties.getProperty(cpname + ".port"));
			String dbName = dbProperties.getProperty(cpname + ".dbname");
			String dtStr = dbProperties.getProperty(cpname + ".type");
			DatabaseType dt = DatabaseType.getDatabaseTypeIDByDBName(dtStr);
			String charSet = dbProperties.getProperty(cpname + ".charset");
			String username = dbProperties.getProperty(cpname + ".user");
			String password = dbProperties.getProperty(cpname + ".password");
			String driverPath = dbProperties.getProperty(cpname + ".driver");
			ConnParameters cp = ConnParameters.getConParam(cpname, hostIp, port, dbName, dt,
					charSet, username, password, driverPath, null);
			return cp;
		} catch (Exception ex) {
			LOG.error(ex);
			return null;
		}

	}
	
	private void mapVertexToEdge(MigrationConfiguration config) {
		GraphDictionary gdbDict = config.getGraphDictionary();
		List<Edge> edgeList = gdbDict.getMigratedEdgeList();
		List<Vertex> vertexList = gdbDict.getMigratedVertexList();
		
		for (Edge edge : edgeList) {
			String startVertexName = edge.getStartVertexName();
			String endVertexName = edge.getEndVertexName();
			
			for (Vertex vertex : vertexList) {
				if (vertex.getTableName().equals(startVertexName)) {
					edge.setStartVertex(vertex);
					
					if (edge.getEdgeType() == 4) {
						edge.setEndVertex(vertex);
					}
					
				} else if (vertex.getTableName().equals(endVertexName)) {
					edge.setEndVertex(vertex);
				}
			}
		}
	}
}
