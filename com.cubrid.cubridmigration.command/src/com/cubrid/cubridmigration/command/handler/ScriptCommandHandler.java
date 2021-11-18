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

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.command.ConsoleCommandHandler;
import com.cubrid.cubridmigration.command.ConsoleUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbmetadata.DBSchemaInfoFetcherFactory;
import com.cubrid.cubridmigration.core.dbmetadata.IDBSchemaInfoFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IDBSource;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.template.MigrationTemplateParser;

/**
 * LogCommandHandler Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-1-2 created by Kevin Cao
 */
public class ScriptCommandHandler implements
		ConsoleCommandHandler {

	private final static Logger LOG = LogUtil.getLogger(ScriptCommandHandler.class);
	private final static List<String> COMMANDS = new ArrayList<String>();

	static {
		String[] cmds = new String[] {"-s", "-t", "-schema", "-o"};
		for (String cmd : cmds) {
			COMMANDS.add(cmd);
		}
	}

	protected PrintStream outPrinter = System.out;
	private Properties dbProperties = new Properties();

	/**
	 * If the target table is already in the target database, the create new
	 * table option will be disabled.
	 * 
	 * @param config to be configured
	 */
	private void configObjectMapping(MigrationConfiguration config) {
		if (!config.targetIsOnline()) {
			return;
		}
		IDBSource ds = config.getTargetConParams();
		if (ds == null) {
			return;
		}
		IDBSchemaInfoFetcher bcf = DBSchemaInfoFetcherFactory.createFetcher(ds);
		Catalog cl = bcf.fetchSchema(ds, null);
		if (cl == null || cl.getSchemas().isEmpty()) {
			return;
		}
		Schema tarSchema = cl.getSchemas().get(0);
		List<SourceEntryTableConfig> tables = config.getExpEntryTableCfg();
		for (SourceEntryTableConfig setc : tables) {
			Table tt = tarSchema.getTableByName(setc.getTarget());
			if (tt == null) {
				continue;
			}
			setc.setCreateNewTable(false);
			setc.setReplace(false);
			setc.setCreatePK(false);
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

	/**
	 * Create a configuration template
	 * 
	 * @param tmpArgs the input parameters
	 * @return MigrationConfiguration
	 */
	private MigrationConfiguration getInitConfig(List<String> tmpArgs) {
		MigrationConfiguration config = null;
		String tpFile = getParameter(tmpArgs, "-template");
		if (StringUtils.isNotBlank(tpFile)) {
			try {
				config = MigrationTemplateParser.parse(tpFile);
			} catch (Exception ex) {
				config = null;
			}
		}
		return config;
	}

	/**
	 * Get parameter from input parameter list
	 * 
	 * @param params input parameter list
	 * @param paraName to be get
	 * @return parameter value
	 */
	private String getParameter(List<String> params, String paraName) {
		int index = params.indexOf(paraName);
		if (index >= 0 && (index + 1) < params.size()) {
			return params.get(index + 1);
		}
		return null;
	}

	/**
	 * Print migration history's log
	 * 
	 * @param args The parameters input by user
	 * 
	 */
	public void handleCommand(List<String> args) {
		try {
			List<String> tmpArgs = initParameters(args);
			if (tmpArgs.isEmpty()) {
				printHelp();
				return;
			}
			String outputFileName = getParameter(tmpArgs, "-o");
			if (StringUtils.isBlank(outputFileName)) {
				outPrinter.println("Output file should be specified with '-o'.");
				return;
			}
			loadDBProperties();
			boolean isNeedReset = false;
			MigrationConfiguration config = getInitConfig(tmpArgs);
			if (config == null) {
				isNeedReset = true;
				config = new MigrationConfiguration();
				File outputFile = new File(outputFileName);
				config.setName(PathUtils.getFileNameWithoutExtendName(outputFile.getName()));
			}
			if (!setSource(config, tmpArgs)) {
				return;
			}
			if (!setTarget(config, tmpArgs)) {
				return;
			}
			setOtherOptions(config, tmpArgs);
			Catalog srcCat = config.buildSourceSchema();
			if (srcCat == null) {
				outPrinter.println("Build source schema error.");
				return;
			}
			config.setSrcCatalog(srcCat, isNeedReset);
			if (isNeedReset) {
				config.setAll(true);
			}
			configObjectMapping(config);
			MigrationTemplateParser.save(config, outputFileName,
					"yes".equalsIgnoreCase(getParameter(tmpArgs, "-schema")));
			outPrinter.println(outputFileName + " was created successfully.");
		} catch (Exception ex) {
			outPrinter.println("Unexpected error. Please check the log for more information.");
			LOG.error(ex);
		}
	}

	/**
	 * Initialize the input parameters
	 * 
	 * @param args origin input parameter
	 * @return standard parameter list
	 */
	private List<String> initParameters(List<String> args) {
		List<String> tmpArgs = new ArrayList<String>();
		Iterator<String> iterator = args.iterator();
		while (iterator.hasNext()) {
			String paraName = iterator.next();
			tmpArgs.add(paraName);
			//If current value is a parameter name
			if (COMMANDS.indexOf(paraName) >= 0) {
				//If args has next value as parameter value
				if (iterator.hasNext()) {
					String paramValue = iterator.next();
					//If next value is a parameter name
					if (COMMANDS.indexOf(paramValue) >= 0) {
						tmpArgs.add("");
					}
					tmpArgs.add(paramValue);
				} else {
					tmpArgs.add("");
				}
			}
		}
		return tmpArgs;
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
	 * printHelp
	 * 
	 */
	protected void printHelp() {
		ConsoleUtils.printHelp("/com/cubrid/cubridmigration/command/help_script.txt");
	}

	/**
	 * Set other configuration of script
	 * 
	 * @param config to be set
	 * @param tmpArgs input parameters
	 */
	private void setOtherOptions(MigrationConfiguration config, List<String> tmpArgs) {
		String haValue = getParameter(tmpArgs, "-ha");
		config.setCreateConstrainsBeforeData("yes".equalsIgnoreCase(haValue));
		String errValue = getParameter(tmpArgs, "-err");
		config.setWriteErrorRecords("yes".equalsIgnoreCase(errValue));
		String tcValue = getParameter(tmpArgs, "-tc");
		if (StringUtils.isNumeric(tcValue)) {
			config.setExportThreadCount(Integer.parseInt(tcValue));
		}
		String pfcValue = getParameter(tmpArgs, "-pfc");
		if (StringUtils.isNumeric(pfcValue)) {
			config.setPageFetchCount(Integer.parseInt(pfcValue));
		}
		String ccValue = getParameter(tmpArgs, "-cc");
		if (StringUtils.isNumeric(ccValue)) {
			config.setCommitCount(Integer.parseInt(ccValue));
		}
	}

	/**
	 * Set source configuration to migration script.
	 * 
	 * @param config to be set
	 * @param args parameters
	 * @return true if set successfully
	 */
	private boolean setSource(MigrationConfiguration config, List<String> args) {
		String svalue = getParameter(args, "-s");
		if (StringUtils.isBlank(svalue)) {
			outPrinter.println("Please specify source with '-s'.");
			return false;
		}
		String type = dbProperties.getProperty(svalue + ".type");
		if (StringUtils.isBlank(type)) {
			return false;
		}
		try {
			//Set source type
			config.setSourceType(type);
		} catch (Exception ex) {
			outPrinter.println("Invalid type in the db.conf of " + svalue);
			return false;
		}
		if (config.sourceIsOnline()) {
			ConnParameters scp = getConParamFromDBProperties(svalue);
			if (scp == null) {
				outPrinter.println("Read JDBC configuration error:" + svalue);
				return false;
			}
			try {
				Connection con = scp.createConnection();
				con.close();
			} catch (Exception e) {
				outPrinter.println("Can't connect database:" + svalue);
				LOG.error(e);
				return false;
			}
			config.setSourceConParams(scp);
		} else if (config.sourceIsXMLDump()) {
			String charSet = dbProperties.getProperty(svalue + ".charset");
			charSet = StringUtils.isBlank(charSet) ? "utf-8" : charSet;
			String xmlfile = dbProperties.getProperty(svalue + ".file");
			File xF = new File(xmlfile);
			if (!xF.exists() || xF.isDirectory()) {
				outPrinter.println("Invalid MySQL XML dump file:" + xmlfile);
				return false;
			}
			config.setSourceFileName(xmlfile);
			config.setSourceFileEncoding(charSet);
			config.setSourceFileTimeZone("Default");
		} else if (config.sourceIsCSV()) {
			//TODO:list all csv files
			outPrinter.println("CSV source is not supported.");
			return false;
		} else if (config.sourceIsSQL()) {
			//TODO:list all csv files
			outPrinter.println("SQL source is not supported.");
			return false;
		}
		return true;
	}

	/**
	 * Set target configuration of migration script.
	 * 
	 * @param config to be set
	 * @param tmpArgs parameters
	 * @return true if set successfully.
	 */
	private boolean setTarget(MigrationConfiguration config, List<String> tmpArgs) {
		String tvalue = getParameter(tmpArgs, "-t");
		if (StringUtils.isBlank(tvalue)) {
			outPrinter.println("Please specify target with '-t'.");
			return false;
		}
		String tType = dbProperties.getProperty(tvalue + ".type");
		if (StringUtils.isBlank(tType)) {
			return false;
		}
		try {
			config.setDestTypeName(tType);
		} catch (Exception ex) {
			outPrinter.println("Invalid target type in the db.conf of " + tvalue);
			return false;
		}
		if (config.targetIsOnline()) {
			ConnParameters tcp = getConParamFromDBProperties(tvalue);
			if (tcp == null) {
				outPrinter.println("Read JDBC configuration error:" + tvalue);
				return false;
			}
			try {
				Connection con = tcp.createConnection();
				con.close();
			} catch (Exception e) {
				outPrinter.println("Can't connect database:" + tvalue);
				LOG.error(e);
				return false;
			}
			config.setTargetConParams(tcp);
		} else if (config.targetIsFile()) {
			String ouputDir = dbProperties.getProperty(tvalue + ".output");
			String prefix = dbProperties.getProperty(tvalue + ".prefix");
			String charset = dbProperties.getProperty(tvalue + ".charset");
			config.setExp2FileOuput(prefix, ouputDir, charset);
			config.setTargetFileTimeZone("Default");
		}
		return true;
	}

	//	public static void main(String[] args) {
	//		ScriptCommandHandler handler = new ScriptCommandHandler();
	//		List<String> ars = new ArrayList<String>();
	//		//--------------------------------------------
	//		System.out.println("1");
	//		handler.handleCommand(ars);
	//		//--------------------------------------------
	//		System.out.println("2");
	//		ars.add("-s");
	//		ars.add("s");
	//		ars.add("-t");
	//		ars.add("t");
	//		ars.add("-o");
	//		ars.add("o");
	//		handler.handleCommand(ars);
	//		//--------------------------------------------
	//		System.out.println("3");
	//		ars.clear();
	//		ars.add("-s");
	//		ars.add("s");
	//		ars.add("-turl");
	//		ars.add("turl");
	//		ars.add("-o");
	//		ars.add("o");
	//		handler.handleCommand(ars);
	//		//--------------------------------------------
	//		System.out.println("4");
	//		ars.clear();
	//		ars.add("-surl");
	//		ars.add("surl");
	//		ars.add("-t");
	//		ars.add("t");
	//		ars.add("-o");
	//		ars.add("o");
	//		handler.handleCommand(ars);
	//		//--------------------------------------------
	//		System.out.println("5");
	//		ars.clear();
	//		ars.add("-surl");
	//		ars.add("surl");
	//		ars.add("-turl");
	//		ars.add("turl");
	//		ars.add("-o");
	//		ars.add("o");
	//		handler.handleCommand(ars);
	//		//--------------------------------------------
	//		System.out.println("6");
	//		ars.clear();
	//		ars.add("-s");
	//		ars.add("s");
	//		ars.add("-surl");
	//		ars.add("surl");
	//		ars.add("-t");
	//		ars.add("");
	//		ars.add("-turl");
	//		ars.add("");
	//		ars.add("-o");
	//		ars.add("o");
	//		handler.handleCommand(ars);
	//		//--------------------------------------------
	//		System.out.println("7");
	//		ars.clear();
	//		ars.add("-s");
	//		ars.add("s");
	//		ars.add("-t");
	//		ars.add("t");
	//		ars.add("-turl");
	//		ars.add("turl");
	//		ars.add("-o");
	//		ars.add("o");
	//		handler.handleCommand(ars);
	//		//--------------------------------------------
	//		System.out.println("8");
	//		ars.clear();
	//		ars.add("-s");
	//		ars.add("s");
	//		ars.add("-t");
	//		ars.add("t");
	//		ars.add("-o");
	//		ars.add("");
	//		handler.handleCommand(ars);
	//	}
}
