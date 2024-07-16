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
package com.cubrid.cubridmigration.core.engine.task.imp;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.connection.ConnParameters;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.task.ImportTask;

/**
 * UpdateAutoIncColCurrentValueTask Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-2-28 created by Kevin Cao
 */
public class UpdateAutoIncColCurrentValueTask extends
		ImportTask {

	private static final Logger LOG = LogUtil.getLogger(UpdateAutoIncColCurrentValueTask.class);

	private final MigrationConfiguration config;

	public UpdateAutoIncColCurrentValueTask(MigrationConfiguration config) {
		this.config = config;
	}

	/**
	 * Update auto_increment columns current values
	 */
	protected void executeImport() {
		if (!config.targetIsOnline()) {
			return;
		}
		
		if (config.getDestType() == DatabaseType.TIBERO.getID()) {
			return;
		}
		
		ConnParameters tcp = config.getTargetConParams();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			con = tcp.createConnection();
			con.setAutoCommit(true);
			stmt = con.createStatement();
			//Fetch which serial should be updated after migration.
			rs = stmt.executeQuery("select name,current_val,increment_val,class_name,att_name from db_serial where class_name is not null order by class_name,att_name,name");
			List<String[]> tobeUpdated = new ArrayList<String[]>();
			while (rs.next()) {
				String tableName = rs.getString(4);
				Table tt = config.getTargetTableSchema(tableName);
				if (tt == null) {
					continue;
				}
				String colName = rs.getString(5);
				Column col = tt.getColumnByName(colName);
				if (col == null) {
					continue;
				}
				String serialName = rs.getString(1);
				long iv = rs.getLong(3);
				long cv = rs.getLong(2);
				tobeUpdated.add(new String[]{serialName, tableName, colName,
						String.valueOf(iv), String.valueOf(cv) });
			}
			Closer.close(rs);
			//Start update serial's start value
			for (String[] ss : tobeUpdated) {
				try {
					rs = stmt.executeQuery("select max(\"" + ss[2]
							+ "\") from \"" + ss[1] + "\"");
					rs.next();
					Long maxValue = rs.getLong(1);
					if (maxValue == null) {
						continue;
					}
					Closer.close(rs);
					maxValue = maxValue + Long.parseLong(ss[3]);
					//If the current max value of column is less than serial's current value, it doesn't need to be updated.
					if (maxValue < Long.parseLong(ss[4])) {
						continue;
					}
					stmt.execute("alter serial \"" + ss[0] + "\" start with "
							+ String.valueOf(maxValue));
				} catch (Exception e) {
					LOG.error("Serial:" + ss[0], e);
				}
			}
		} catch (SQLException e) {
			LOG.error("", e);
		} finally {
			Closer.close(rs);
			Closer.close(stmt);
			Closer.close(con);
		}
	}
}
