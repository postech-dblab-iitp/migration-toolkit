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
package com.cubrid.cubridmigration.ui.wizard.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.osgi.util.NLS;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.datatype.DBDataTypeHelper;
import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.TableOrView;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceCSVConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceColumnConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceEntryTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceFKConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceIndexConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSQLTableConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceSequenceConfig;
import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
import com.cubrid.cubridmigration.core.engine.exception.JDBCConnectErrorException;
import com.cubrid.cubridmigration.core.mapping.model.VerifyInfo;
import com.cubrid.cubridmigration.core.sql.SQLHelper;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.IMigrationWizardStatus;

/**
 * Verify validation of migration configuration and other utilities.
 * 
 * @author caoyilin
 * 
 */
public class MigrationCfgUtils {

	private static final String LINE_SEP = System.getProperty("line.separator");

	/**
	 * Change the column order according to input list.
	 * 
	 * @param setc SourceEntryTableConfig
	 * @param tt target table
	 * @param columns List<SourceColumnConfig>
	 */
	public static void changeColumnOrder(SourceEntryTableConfig setc, Table tt,
			List<SourceColumnConfig> columns) {
		List<SourceColumnConfig> oldColumns = setc.getColumnConfigList();
		if (oldColumns.size() != columns.size()) {
			return;
		}
		setc.clearColumnList();
		setc.addAllColumnList(columns);
		List<Column> ttColumns = new ArrayList<Column>();
		for (SourceColumnConfig scc : columns) {
			Column col = tt.getColumnByName(scc.getTarget());
			if (col == null) {
				throw new RuntimeException("Invalid target column name:" + scc.getTarget());
			}
			tt.removeColumn(col);
			ttColumns.add(col);
		}
		ttColumns.addAll(tt.getColumns());
		tt.setColumns(ttColumns);
	}

	/**
	 * Check table configuration's condition setting
	 * 
	 * @param config MigrationConfiguration
	 * @param migrateData boolean
	 * @param tableName String
	 * @param condition String
	 * 
	 * @return boolean true if sql is valid.
	 */
	public static boolean checkEntryTableCondition(MigrationConfiguration config,
			boolean migrateData, String tableName, String condition) {
		if (migrateData && StringUtils.isNotBlank(condition)) {
			final DatabaseType sourceDBType = config.getSourceDBType();
			boolean hasWhere = condition.trim().toLowerCase(Locale.US).startsWith("where");
			final SQLHelper sqlHelper = sourceDBType.getSQLHelper(null);
			String sql = "select * from " + sqlHelper.getQuotedObjName(tableName) + " "
					+ (hasWhere ? condition : ("where " + condition));
			sql = sqlHelper.getTestSelectSQL(sql);
			try {
				config.validateExpSQLConfig(sql);
			} catch (JDBCConnectErrorException ex) {
				//If JDBC can't be connected, return true anyway.
				return true;
			} catch (Exception ex) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Check the target cubrid is running in HA mode
	 * 
	 * @param config MigrationConfiguration
	 * @return true if running in HA mode.
	 */
	public static boolean isHACUBRID(MigrationConfiguration config) {
		if (!config.targetIsOnline()) {
			return false;
		}
		ConnParameters tcp = config.getTargetConParams();
		if (tcp == null) {
			return false;
		}
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String sql = "SELECT COUNT(*) FROM db_ha_apply_info " + "WHERE db_name='"
					+ tcp.getDbName() + "'";
			con = tcp.createConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);
			return rs.next() && rs.getInt(1) > 0;
		} catch (Exception e) {
			return false;
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
			Closer.close(con);
		}
	}

	/**
	 * verify whether current table/view/column/index/fk/pk/serial name is valid
	 * 
	 * @param name String
	 * @return boolean
	 */
	public static boolean verifyTargetDBObjName(String name) {
		return !StringUtils.isEmpty(name)
				&& (name.length() <= CUBRIDDataTypeHelper.DB_OBJ_NAME_MAX_LENGTH)
				&& (name.indexOf('"') < 0) && (name.indexOf('[') < 0) && (name.indexOf(']') < 0);
	}

	private final CUBRIDDataTypeHelper cubridDataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
	private MigrationConfiguration config;

	private final List<String> existsViewNameList = new ArrayList<String>();
	private final List<String> existsTableNameList = new ArrayList<String>();
	private final List<String> existsSerialNameList = new ArrayList<String>();

	private IMigrationWizardStatus wizardStatus;

	/**
	 * Check the validation of migration configuration
	 * 
	 * @param config to be checked
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages checkAll(MigrationConfiguration config) {
		try {
			StringBuffer sbWarning = new StringBuffer();
			StringBuffer sbConfirm = new StringBuffer();
			if (!config.hasObjects2Export()) {
				throw new MigrationConfigurationCheckingErrorException(
						"There is no object to be migrated.");
			}
			VerifyResultMessages result;
			if (config.sourceIsCSV()) {
				result = checkCSVCfg(config);
			} else {
				result = checkEntryTableCfg(config);
				mergeVerifyResults(sbWarning, sbConfirm, result);
				result = checkSQLTableCfg(config);
				mergeVerifyResults(sbWarning, sbConfirm, result);
				result = checkViewCfg(config);
				mergeVerifyResults(sbWarning, sbConfirm, result);
				result = checkSerialCfg(config);
				mergeVerifyResults(sbWarning, sbConfirm, result);
			}
			result.setWarningMessage(sbWarning.toString().trim());
			result.setConfirmMessage(sbConfirm.toString().trim());
			return result;
		} catch (MigrationConfigurationCheckingErrorException ex) {
			return new VerifyResultMessages(ex.getMessage(), null, null);
		}
	}

	/**
	 * Check CSV configuration's validation
	 * 
	 * @param config MigrationConfiguration
	 * @return VerifyResultMessages
	 */
	protected VerifyResultMessages checkCSVCfg(MigrationConfiguration config) {
		final List<SourceCSVConfig> csvConfigs = config.getCSVConfigs();
		Set<String> csvOnlyCreateTarget = new HashSet<String>();
		Set<String> csvReplaceTarget = new HashSet<String>();
		Set<String> csvNoCreateTarget = new HashSet<String>();
		StringBuffer sbWarn = new StringBuffer();
		for (SourceCSVConfig scc : csvConfigs) {
			if (!verifyTargetDBObjName(scc.getTarget())) {
				throw new MigrationConfigurationCheckingErrorException(
						NLS.bind(Messages.objectMapPageTabFolderErrTableName, scc.getName(),
								scc.getTarget()));

			}
			if (scc.isCreate()) {
				csvOnlyCreateTarget.add(scc.getTarget());
				if (scc.isReplace()) {
					csvReplaceTarget.add(scc.getTarget());
					csvOnlyCreateTarget.remove(scc.getTarget());
				}
			}
		}
		for (SourceCSVConfig scc : csvConfigs) {
			if (!scc.isCreate() && !csvOnlyCreateTarget.contains(scc.getTarget())
					&& !csvReplaceTarget.contains(scc.getTarget())) {
				csvNoCreateTarget.add(scc.getTarget());
			}
		}

		for (String target : csvOnlyCreateTarget) {
			final boolean isExist = existsTableNameList.indexOf(target.toUpperCase(Locale.US)) >= 0;
			if (isExist) {
				throw new MigrationConfigurationCheckingErrorException(NLS.bind(
						Messages.objectMapPageErrMsgDuplicatedTable1, target));
			}
		}
		for (String target : csvNoCreateTarget) {
			final boolean isExist = existsTableNameList.indexOf(target.toUpperCase(Locale.US)) >= 0;
			if (!isExist) {
				throw new MigrationConfigurationCheckingErrorException(NLS.bind(
						Messages.objectMapPageErrMsgDuplicatedTable5, target));
			}
		}
		for (String target : csvReplaceTarget) {
			final boolean isExist = existsTableNameList.indexOf(target.toUpperCase(Locale.US)) >= 0;
			if (isExist) {
				sbWarn.append(Messages.bind(Messages.msgWarnTableRecreated, target)).append(
						LINE_SEP);
			}
		}
		return new VerifyResultMessages(null, sbWarn.toString(), null);
	}

	/**
	 * Check Entry table configuration's validation
	 * 
	 * @param config MigrationConfiguration
	 * @return VerifyResultMessages
	 */
	protected VerifyResultMessages checkEntryTableCfg(MigrationConfiguration config) {
		StringBuffer sbWarning = new StringBuffer();
		StringBuffer sbConfirm = new StringBuffer();
		for (SourceEntryTableConfig setc : config.getExpEntryTableCfg()) {
			VerifyResultMessages result = checkEntryTableCfg(config, setc);
			if (result.hasWarning() && sbWarning.indexOf(result.getWarningMessage()) < 0) {
				sbWarning.append(result.getWarningMessage()).append(LINE_SEP);
			}
			if (result.hasConfirm() && sbConfirm.indexOf(result.getConfirmMessage()) < 0) {
				sbConfirm.append(result.getConfirmMessage()).append(LINE_SEP);
			}
		}
		return new VerifyResultMessages(null, sbWarning.toString().trim(),
				sbConfirm.toString().trim());
	}

	/**
	 * Check an Entry Table configuration's validation
	 * 
	 * @param config MigrationConfiguration
	 * @param setc MigrationConfiguration
	 * @return VerifyResultMessages
	 */
	protected VerifyResultMessages checkEntryTableCfg(MigrationConfiguration config,
			SourceEntryTableConfig setc) {
		//If don't create and don't migrate data, ignore it
		if (!setc.isCreateNewTable() && !setc.isMigrateData()) {
			return new VerifyResultMessages();
		}
		//Validate condition
		if (!checkEntryTableCondition(config, setc.isMigrateData(), setc.getName(),
				setc.getCondition())) {
			throw new MigrationConfigurationCheckingErrorException(Messages.bind(
					Messages.msgErrInvalidCondition, setc.getName()));
		}
		//Target Name
		if (!verifyTargetDBObjName(setc.getTarget())) {
			throw new MigrationConfigurationCheckingErrorException(NLS.bind(
					Messages.objectMapPageTabFolderErrTableName, setc.getName(), setc.getTarget()));
		}
		Table srcTable = config.getSrcTableSchema(setc.getOwner(), setc.getName());
		if (srcTable == null) {
			throw new MigrationConfigurationCheckingErrorException("Can't find the table ["
					+ setc.getTarget() + "] in source database schema.");
		}
		Table targetTable = config.getTargetTableSchema(setc.getTarget());
		if (targetTable == null) {
			throw new MigrationConfigurationCheckingErrorException("Can't find the table ["
					+ setc.getTarget() + "] in target database schema.");
		}

		StringBuffer sbWarn = new StringBuffer();
		StringBuffer sbConfirm = new StringBuffer();
		//If there is no PK in the source table, output a warning.
		if (!srcTable.hasPK()) {
			String errorMessage = NLS.bind(Messages.objectMapPageErrMsgNoPK, setc.getName());
			sbConfirm.append(errorMessage).append(LINE_SEP);
		}
		checkTableIsInTargetDb(config, setc, targetTable, sbConfirm);
		checkTableColumns(config, setc, srcTable, targetTable, sbConfirm);
		checkEntryTableConstrains(setc, targetTable, sbWarn, sbConfirm);
		return createVerifyResult(sbWarn, sbConfirm);
	}

	/**
	 * @param setc SourceEntryTableConfig
	 * @param targetTable Table
	 * @param sbWarn StringBuffer
	 * @param sbConfirm StringBuffer
	 */
	protected void checkEntryTableConstrains(SourceEntryTableConfig setc, Table targetTable,
			StringBuffer sbWarn, StringBuffer sbConfirm) {
		if (!setc.isCreateNewTable()) {
			return;
		}
		//Validate foreign key configurations
		List<String> names = new ArrayList<String>();
		for (SourceFKConfig fkc : setc.getFKConfigList()) {
			if (!fkc.isCreate()) {
				continue;
			}
			if (!verifyTargetDBObjName(fkc.getTarget())) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrInvalidFKName, setc.getTarget(), fkc.getTarget()));
			}
			if (names.indexOf(fkc.getTarget().toLowerCase(Locale.US)) >= 0) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrDupColumnName, setc.getTarget(), fkc.getTarget()));
			}
			names.add(fkc.getTarget().toLowerCase(Locale.US));

			FK fk = targetTable.getFKByName(fkc.getTarget());
			if (fk == null) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrFKNotFound, setc.getTarget(), fkc.getTarget()));
			}
			if (fk.getUpdateRule() == 0) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.errCascadeOnUpdateNotSupported, setc.getTarget(), fk.getName()));
			}
			List<String> fkcolumns = fk.getColumnNames();
			if (CollectionUtils.isEmpty(fkcolumns)) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrInvalidFKColumns, fk.getTable().getName(), fk.getName()));
			}
			for (String colName : fkcolumns) {
				Column scc = targetTable.getColumnByName(colName); // setc.getColumnConfig(colName);
				if (scc == null) {
					throw new MigrationConfigurationCheckingErrorException(Messages.bind(
							Messages.msgErrFKColumn, setc.getTarget(), fkc.getTarget()));
				}
			}
		}
		//Validate index configurations
		for (SourceIndexConfig sic : setc.getIndexConfigList()) {
			if (!sic.isCreate()) {
				continue;
			}
			if (!verifyTargetDBObjName(sic.getTarget())) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrInvalidIndexName, sic.getParent().getTarget(),
						sic.getTarget()));
			}
			if (names.indexOf(sic.getTarget().toLowerCase(Locale.US)) >= 0) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrDupColumnName, setc.getTarget(), sic.getTarget()));
			}
			names.add(sic.getTarget().toLowerCase(Locale.US));

			Index index = targetTable.getIndexByName(sic.getTarget());
			if (index == null) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrIndexNotFound, sic.getTarget(), setc.getTarget()));
			}
			final List<String> columnNames = index.getColumnNames();
			if (columnNames.isEmpty()) {
				throw new MigrationConfigurationCheckingErrorException(
						Messages.bind(Messages.msgNoIndexColumn, new String[] {setc.getTarget(),
								sic.getTarget()}));
			}
			for (String colName : columnNames) {
				//If it is function based index TODO: need to improve: different version has different behavior. 
				if (colName.matches(".+\\(.*\\).*")) {
					sbWarn.append(
							Messages.bind(Messages.msgExpIndexColumn,
									new String[] {setc.getTarget(), sic.getTarget(), colName})).append(
							LINE_SEP);
					continue;
				}
				SourceColumnConfig scc = setc.getColumnConfigByTarget(colName);
				if (scc == null) {
					sbConfirm.append(Messages.bind(Messages.msgErrIndexColumn,
							new String[] {setc.getTarget(), sic.getTarget(), colName}));
				} else if (!scc.isCreate()) {
					throw new MigrationConfigurationCheckingErrorException(Messages.bind(
							Messages.msgErrIndexColumnNotExp, setc.getTarget(), scc.getTarget()));
				}
			}
		}
	}

	/**
	 * Check serial validation
	 * 
	 * @param config MigrationConfiguration
	 * @return VerifyResultMessages
	 */
	protected VerifyResultMessages checkSerialCfg(MigrationConfiguration config) {
		List<String> serials = new ArrayList<String>();
		for (SourceSequenceConfig sc : config.getExpSerialCfg()) {
			//No create no check
			if (!sc.isCreate()) {
				continue;
			}
			checkSerialCfg(config, sc);
			//Check duplicated
			if (serials.indexOf(sc.getTarget()) >= 0) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.errDuplicateSequenceName, sc.getTarget()));
			}
			serials.add(sc.getTarget());
		}
		return new VerifyResultMessages();
	}

	/**
	 * Check single serial configuration
	 * 
	 * @param config MigrationConfiguration
	 * @param sc SourceConfig
	 */
	protected void checkSerialCfg(MigrationConfiguration config, SourceSequenceConfig sc) {
		//Name
		if (!verifyTargetDBObjName(sc.getTarget())) {
			throw new MigrationConfigurationCheckingErrorException(Messages.bind(
					Messages.errSequenceName, sc.getTarget()));
		}
	}

	/**
	 * Check all SQL configuration
	 * 
	 * @param config MigrationConfiguration
	 * @return VerifyResultMessages
	 */
	protected VerifyResultMessages checkSQLTableCfg(MigrationConfiguration config) {
		if (!config.sourceIsOnline()) {
			return new VerifyResultMessages(null, null, null);
		}
		if (wizardStatus.isSourceOfflineMode()) {
			return new VerifyResultMessages(null, null, null);
		}
		StringBuffer sbWarning = new StringBuffer();
		StringBuffer sbConfirm = new StringBuffer();

		List<String> names = new ArrayList<String>();
		for (SourceSQLTableConfig sstc : config.getExpSQLCfg()) {
			if (names.indexOf(sstc.getName()) >= 0) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrSQLNameDuplicated, sstc.getName()));
			}
			names.add(sstc.getName());
			VerifyResultMessages result = checkSQLTableCfg(config, sstc);
			if (result.hasWarning() && sbWarning.indexOf(result.getWarningMessage()) < 0) {
				sbWarning.append(result.getWarningMessage()).append(LINE_SEP);
			}
			if (result.hasConfirm() && sbConfirm.indexOf(result.getConfirmMessage()) < 0) {
				sbConfirm.append(result.getConfirmMessage()).append(LINE_SEP);
			}
		}
		return new VerifyResultMessages(null, sbWarning.toString().trim(),
				sbConfirm.toString().trim());
	}

	/**
	 * Check single SQL
	 * 
	 * @param config MigrationConfiguration
	 * @param sstc SourceSQLTableConfig
	 * @return VerifyResultMessages
	 */
	protected VerifyResultMessages checkSQLTableCfg(MigrationConfiguration config,
			SourceSQLTableConfig sstc) {
		//If don't create and don't migrate data, ignore it
		if (!sstc.isCreateNewTable() && !sstc.isMigrateData()) {
			return new VerifyResultMessages();
		}
		StringBuffer sbWarn = new StringBuffer();
		StringBuffer sbConfirm = new StringBuffer();
		//Name
		if (!verifyTargetDBObjName(sstc.getTarget())) {
			throw new MigrationConfigurationCheckingErrorException(NLS.bind(
					Messages.objectMapPageTabFolderErrTableName, sstc.getName(), sstc.getTarget()));
		}
		Table srcTable = config.getSrcSQLSchema(sstc.getName());
		if (srcTable == null) {
			throw new MigrationConfigurationCheckingErrorException(
					"Can't find the source SQL schema [" + sstc.getTarget() + "] .");
		}
		Table targetTable = config.getTargetTableSchema(sstc.getTarget());
		if (targetTable == null) {
			throw new MigrationConfigurationCheckingErrorException("Can't find the table ["
					+ sstc.getTarget() + "] in target database schema.");
		}
		checkTableIsInTargetDb(config, sstc, targetTable, sbConfirm);
		//Validate column configurations
		final DBTransformHelper transHelper = config.getDBTransformHelper();
		for (SourceColumnConfig scc : sstc.getColumnConfigList()) {
			if (!verifyTargetDBObjName(scc.getTarget())) {
				throw new MigrationConfigurationCheckingErrorException(NLS.bind(
						Messages.msgErrInvalidColumnName, sstc.getTarget(), scc.getTarget()));
			}
			Column scol = srcTable.getColumnByName(scc.getName());
			if (scol == null) {
				throw new MigrationConfigurationCheckingErrorException("Can't find table column ["
						+ scc.getName() + "] in source SQL schema " + sstc.getName());
			}
			Column tcol = targetTable.getColumnByName(scc.getTarget());
			if (tcol == null) {
				throw new MigrationConfigurationCheckingErrorException(NLS.bind(
						Messages.objectMapPageNoMappingColumn, scc.getName(), sstc.getName()));
			}

			if (!cubridDataTypeHelper.isValidDatatype(tcol.getShownDataType())) {
				throw new MigrationConfigurationCheckingErrorException("Invalid column data type ["
						+ tcol.getShownDataType());
			}
			//Validate data type mapping
			VerifyInfo verifyInfo = transHelper.verifyColumnDataType(scol, tcol, config);
			if (verifyInfo.getResult() == VerifyInfo.TYPE_NO_MATCH) {
				String[] bindings = new String[] {scol.getName(), scol.getShownDataType(),
						tcol.getName(), tcol.getShownDataType()};
				String errorMessage = NLS.bind(Messages.objectMapPageErrMsgColumnNotMatched,
						bindings);
				sbConfirm.append(errorMessage).append(LINE_SEP);
			} else if (verifyInfo.getResult() == VerifyInfo.TYPE_NOENOUGH_LENGTH) {
				String[] bindings = new String[] {scol.getName(), scol.getShownDataType(),
						tcol.getName(), tcol.getShownDataType(), srcTable.getName()};
				String errorMessage = NLS.bind(Messages.msgErrColumnNotEnoughLength, bindings);
				sbConfirm.append(errorMessage).append(LINE_SEP);
			}
		}
		return createVerifyResult(sbWarn, sbConfirm);
	}

	/**
	 * @param config MigrationConfiguration
	 * @param setc SourceEntryTableConfig
	 * @param srcTable Table
	 * @param targetTable Table
	 * @param sbConfirm StringBuffer
	 */
	protected void checkTableColumns(MigrationConfiguration config, SourceEntryTableConfig setc,
			Table srcTable, Table targetTable, StringBuffer sbConfirm) {
		//Validate column configurations
		final DBTransformHelper transHelper = config.getDBTransformHelper();
		boolean noColumn = true;
		List<String> names = new ArrayList<String>();
		for (SourceColumnConfig scc : setc.getColumnConfigList()) {
			//If column does not create or migrate data, ignore it
			if (!scc.isCreate()) {
				continue;
			}
			noColumn = false;
			if (!verifyTargetDBObjName(scc.getTarget())) {
				throw new MigrationConfigurationCheckingErrorException(NLS.bind(
						Messages.msgErrInvalidColumnName, setc.getTarget(), scc.getTarget()));
			}
			if (names.indexOf(scc.getTarget().toLowerCase(Locale.US)) >= 0) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrDupColumnName, setc.getTarget(), scc.getTarget()));
			}
			names.add(scc.getTarget().toLowerCase(Locale.US));
			Column scol = srcTable.getColumnByName(scc.getName());
			if (scol == null) {
				throw new MigrationConfigurationCheckingErrorException("Can't find table column ["
						+ scc.getName() + "] in source table " + setc.getName());
			}
			Column tcol = targetTable.getColumnByName(scc.getTarget());
			if (tcol == null) {
				throw new MigrationConfigurationCheckingErrorException(NLS.bind(
						Messages.objectMapPageNoMappingColumn, scc.getName(), setc.getName()));
			}

			if (!cubridDataTypeHelper.isValidDatatype(tcol.getShownDataType())) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.msgErrInvalidDataType, targetTable.getName(), tcol.getName()));
			}
			//Validate data type mapping
			VerifyInfo verifyInfo = transHelper.verifyColumnDataType(scol, tcol, config);
			if (verifyInfo.getResult() == VerifyInfo.TYPE_NO_MATCH) {
				String[] bindings = new String[] {scol.getName(), scol.getShownDataType(),
						tcol.getName(), tcol.getShownDataType()};
				String errorMessage = NLS.bind(Messages.objectMapPageErrMsgColumnNotMatched,
						bindings);
				sbConfirm.append(errorMessage).append(LINE_SEP);
			} else if (verifyInfo.getResult() == VerifyInfo.TYPE_NOENOUGH_LENGTH) {
				String[] bindings = new String[] {scol.getName(), scol.getShownDataType(),
						tcol.getName(), tcol.getShownDataType(), srcTable.getName()};
				String errorMessage = NLS.bind(Messages.msgErrColumnNotEnoughLength, bindings);
				sbConfirm.append(errorMessage).append(LINE_SEP);
			}
		}
		//If no column to be migrated
		if (noColumn) {
			throw new MigrationConfigurationCheckingErrorException("Table " + setc.getName()
					+ " has no column to be migrated.");
		}
	}

	/**
	 * @param config MigrationConfiguration
	 * @param setc SourceEntryTableConfig
	 * @param targetTable Table
	 * @param sbConfirm StringBuffer
	 */
	protected void checkTableIsInTargetDb(MigrationConfiguration config, SourceTableConfig setc,
			Table targetTable, StringBuffer sbConfirm) {
		if (config.targetIsOnline() && !wizardStatus.isTargetOfflineMode()) {
			final boolean contained = existsTableNameList.contains(targetTable.getName().toUpperCase(
					Locale.US));
			if (setc.isCreateNewTable() && setc.isReplace() && contained) {
				sbConfirm.append(Messages.bind(Messages.msgWarnTableRecreated, setc.getTarget())).append(
						LINE_SEP);
			} else if (setc.isCreateNewTable() && !setc.isReplace() && contained) {
				throw new MigrationConfigurationCheckingErrorException(NLS.bind(
						Messages.objectMapPageErrMsgDuplicatedTable1, targetTable.getName()));
			} else if (!setc.isCreateNewTable() && !contained) {
				throw new MigrationConfigurationCheckingErrorException(NLS.bind(
						Messages.objectMapPageErrMsgDuplicatedTable5, targetTable.getName()));
			}
		}
	}

	/**
	 * Check all views
	 * 
	 * @param config MigrationConfiguration
	 * @return VerifyResultMessages
	 */
	protected VerifyResultMessages checkViewCfg(MigrationConfiguration config) {
		List<String> views = new ArrayList<String>();
		for (SourceConfig sc : config.getExpViewCfg()) {
			if (!sc.isCreate()) {
				continue;
			}
			checkViewCfg(config, sc);
			//Check duplicated
			List<SourceTableConfig> tts = config.getSourceTableConfigByTarget(sc.getTarget());
			boolean flag = false;
			for (SourceTableConfig stc : tts) {
				if (stc.isCreateNewTable() || stc.isMigrateData()) {
					flag = true;
					break;
				}
			}
			if (flag || views.indexOf(sc.getTarget()) >= 0) {
				throw new MigrationConfigurationCheckingErrorException(NLS.bind(
						Messages.objectMapPageErrMsgDuplicatedTable3, sc.getTarget()));
			}
			views.add(sc.getTarget());
		}
		StringBuffer sbWarn = new StringBuffer();
		for (String target : views) {
			final boolean isExist = existsViewNameList.indexOf(target.toUpperCase(Locale.US)) >= 0;
			if (isExist) {
				sbWarn.append(Messages.bind(Messages.objectMapPageErrMsgDuplicatedTable1, target)).append(
						LINE_SEP);
			}
		}
		if (sbWarn.length() > 0) {
			return new VerifyResultMessages(null, sbWarn.toString(), null);
		}
		return new VerifyResultMessages();
	}

	/**
	 * Check single view
	 * 
	 * @param config MigrationConfiguration
	 * @param sc SourceConfig
	 */
	protected void checkViewCfg(MigrationConfiguration config, SourceConfig sc) {
		//Name
		if (!verifyTargetDBObjName(sc.getTarget())) {
			throw new MigrationConfigurationCheckingErrorException(NLS.bind(
					Messages.objectMapPageTabFolderErrViewName, sc.getName(), sc.getTarget()));
		}
		if (!sc.isReplace()) {
			final boolean isExist = existsViewNameList.indexOf(sc.getTarget().toUpperCase(Locale.US)) >= 0;
			if (isExist) {
				throw new MigrationConfigurationCheckingErrorException(Messages.bind(
						Messages.objectMapPageErrMsgDuplicatedTable1, sc.getTarget()));
			}
		}
	}

	/**
	 * @param sbWarn warning messages
	 * @param sbConfirm confirm messages
	 * @return VerifyResultMessages
	 */
	protected VerifyResultMessages createVerifyResult(StringBuffer sbWarn, StringBuffer sbConfirm) {
		VerifyResultMessages result = new VerifyResultMessages();
		if (sbConfirm.length() > 0) {
			result.setConfirmMessage(sbConfirm.toString().trim());
		}
		if (sbWarn.length() > 0) {
			result.setWarningMessage(sbWarn.toString().trim());
		}
		return result;
	}

	/**
	 * 
	 * @return true if user should change the target character type's size to
	 *         avoid data lost.
	 */
	public boolean doesNeedToChangeCharacterTypeSize() {
		return DatabaseType.MYSQL.equals(config.getSourceDBType())
				|| DatabaseType.ORACLE.equals(config.getSourceDBType());
	}

	public MigrationConfiguration getMigrationConfiguration() {
		return config;
	}

	/**
	 * Get the table names which does not have PK
	 * 
	 * @param catalog to be searched No PK tables
	 * 
	 * @return table names with schema name
	 */
	public List<String> getNoPKTables(Catalog catalog) {
		List<Schema> schemas = catalog.getSchemas();
		boolean ifAddSchemaPrefx = schemas.size() > 1;
		List<String> result = new ArrayList<String>();
		for (Schema schema : schemas) {
			String prefix;
			if (ifAddSchemaPrefx) {
				prefix = schema.getName() + ".";
			} else {
				prefix = "";
			}
			List<Table> tables = schema.getTables();
			for (Table table : tables) {
				if (table.hasPK()) {
					continue;
				}
				String noPKTableName = prefix + table.getName();
				result.add(noPKTableName);
			}
		}
		return result;
	}

	/**
	 * @param precision to be adjusted.
	 * @return a valid CUBRID character type precision
	 */
	protected int getRightCharacerTypePrecision(long precision) {
		precision = (precision < 0 || precision > DataTypeConstant.CUBRID_MAXSIZE) ? DataTypeConstant.CUBRID_MAXSIZE
				: precision;
		return (int) precision;
	}

	/**
	 * @param sbWarning StringBuffer
	 * @param sbConfirm StringBuffer
	 * @param result VerifyResultMessages
	 */
	protected void mergeVerifyResults(StringBuffer sbWarning, StringBuffer sbConfirm,
			VerifyResultMessages result) {
		if (result.hasWarning()) {
			sbWarning.append(result.getWarningMessage()).append(LINE_SEP);
		}
		if (result.hasConfirm()) {
			sbConfirm.append(result.getConfirmMessage()).append(LINE_SEP);
		}
	}

	/**
	 * multiply the length of char type column
	 * 
	 * @param scol Column
	 * @param tcol Column
	 * @param factor will effect the result precision
	 */
	public void multiplyCharColumn(Column scol, Column tcol, int factor) {
		long precision = (long) scol.getPrecision() * (long) factor;
		tcol.setPrecision(getRightCharacerTypePrecision(precision));
		tcol.setShownDataType(cubridDataTypeHelper.getShownDataType(tcol).toLowerCase(Locale.US));
	}

	/**
	 * Set default to the length of char type column
	 * 
	 * @param scol Column
	 * @param tcol Column
	 */
	public void setCharTypeColumnToDefaultMapping(Column scol, Column tcol) {
		final DBDataTypeHelper dtHelper = config.getSourceDBType().getDataTypeHelper(null);
		if (dtHelper.isGeneralizedNumeric(scol.getDataType())) {
			return;
		}
		if (!cubridDataTypeHelper.isGenericString(tcol.getDataType())) {
			return;
		}
		Column tempCol = config.getDBTransformHelper().getCUBRIDColumn(scol, config);
		tempCol.setPrecision(getRightCharacerTypePrecision(scol.getPrecision()));
		tempCol.setScale(null);
		cubridDataTypeHelper.setColumnDataType(cubridDataTypeHelper.getShownDataType(tempCol), tcol);
	}

	public void setMigrationConfiguration(MigrationConfiguration migrationConfiguration) {
		this.config = migrationConfiguration;
	}

	/**
	 * set target catalog
	 * 
	 * @param targetCatalog Catalog
	 * @param wizardStatus IMigrationWizardStatus
	 */
	public void setTargetCatalog(Catalog targetCatalog, IMigrationWizardStatus wizardStatus) {
		existsTableNameList.clear();
		existsViewNameList.clear();
		existsSerialNameList.clear();
		this.wizardStatus = wizardStatus;
		//convert all name to upper case .when check convert to upper too
		if (targetCatalog != null && !targetCatalog.getSchemas().isEmpty()) {
			Schema schema = targetCatalog.getSchemas().get(0);
			List<Table> tables = schema.getTables();
			for (Table table : tables) {
				existsTableNameList.add(table.getName().toUpperCase(Locale.US));
				existsViewNameList.add(table.getName().toUpperCase(Locale.US));
			}

			List<View> views = schema.getViews();
			for (View view : views) {
				existsViewNameList.add(view.getName().toUpperCase(Locale.US));
				existsTableNameList.add(view.getName().toUpperCase(Locale.US));
			}

			List<Sequence> serials = schema.getSequenceList();
			for (Sequence serial : serials) {
				existsSerialNameList.add(serial.getName().toUpperCase(Locale.US));
			}
		}
	}

	/**
	 * 
	 * @return table names which don't have PK.
	 */
	public String getNoPKSourceTablesCheckingResult() {
		StringBuffer sb = new StringBuffer();
		for (SourceEntryTableConfig setc : config.getExpEntryTableCfg()) {
			if (!setc.isCreateNewTable() && !setc.isMigrateData()) {
				continue;
			}
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(setc.getName());
		}
		if (sb.length() == 0) {
			return "";
		}
		return NLS.bind(Messages.objectMapPageErrMsgNoPK, sb.toString());
	}

	public boolean checkMultipleSchema(Catalog catalog, MigrationConfiguration cfg) {
		return cfg.sourceIsOnline()
				&& cfg.getSourceDBType().isSupportMultiSchema()
				&& (catalog.getSchemas().size() > 1)
				&& !cfg.hasObjects2Export();
	}

	public boolean createAllObjectsMap(Catalog catalog) {
		List<Schema> schemas = catalog.getSchemas();
		if (schemas.size() <= 1) {
			return false;
		}

		Map<String, Integer> allTablesCountMap = catalog.getAllTablesCountMap();
		Map<String, Integer> allViewsCountMap = catalog.getAllViewsCountMap();
		Map<String, Integer> allSequencesCountMap = catalog.getAllSequencesCountMap();

		allTablesCountMap.clear();
		allViewsCountMap.clear();
		allSequencesCountMap.clear();

		for (Schema schema : schemas) {
			createMap(allTablesCountMap, schema.getTables());
			createMap(allViewsCountMap, schema.getViews());
			createMap(allSequencesCountMap, schema.getSequenceList());
		}

		return true;
	}

	private void createMap(Map<String, Integer> map, List<?> list) {
		for (Object obj : list) {
			DBObject dbObject = (DBObject) obj;
			String name = dbObject.getName();
			map.put(name, (map.get(name) != null ? map.get(name) : 0) + 1);
		}
	}

	public boolean hasDuplicatedObjects(Catalog catalog) {
		return hasDuplicatedObjects(catalog.getAllTablesCountMap())
		        || hasDuplicatedObjects(catalog.getAllViewsCountMap())
		        || hasDuplicatedObjects(catalog.getAllSequencesCountMap());
	}

	private boolean hasDuplicatedObjects(Map<String, Integer> map) {
		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			if (map.get(key) > 1) {
				return true;
			}
		}
		return false;
	}

	public void createDetailMessage(StringBuffer sb, Catalog catalog, String objType) {
		List<Schema> schemas = catalog.getSchemas();
		sb.append("[ " + objType.toUpperCase()  + "(s) ]\n");
		for (Schema schema : schemas) {
			if (objType.equals(DBObject.OBJ_TYPE_TABLE)) {
				createObjectInformation(sb, catalog.getAllTablesCountMap(), schema.getTables());
			} else if (objType.equals(DBObject.OBJ_TYPE_VIEW)) {
				createObjectInformation(sb, catalog.getAllViewsCountMap(), schema.getViews());
			} else if (objType.equals(DBObject.OBJ_TYPE_SEQUENCE)) {
				createObjectInformation(sb, catalog.getAllSequencesCountMap(), schema.getSequenceList());
			}
		}
		sb.append("\n");
	}

	private void createObjectInformation(StringBuffer sb, Map<String, Integer> map, List<?> objectList) {
		for (Object object : objectList) {
			String objectName = ((DBObject) object).getName();
			if (isDuplicatedObject(map, objectName)) {
				String owner = "";
				if (object instanceof TableOrView) {
					owner = ((TableOrView) object).getOwner();
				} else if (object instanceof Sequence) {
					owner = ((Sequence) object).getOwner();
				}
				appendDuplicatedObjectInformation(sb, owner, objectName);
			}
		}
	}

	private boolean isDuplicatedObject(Map<String, Integer> map, String objectName) {
		return map.get(objectName) != null && (map.get(objectName) > 1);
	}

	private void appendDuplicatedObjectInformation(StringBuffer sb, String owner, String objectName) {
		sb.append("- ").append(owner).append(".").append(objectName)
		.append(" -> ")
		.append(owner.toLowerCase()).append("_").append(objectName.toLowerCase())
		.append("\n");
	}
}
