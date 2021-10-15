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

import java.util.HashMap;
import java.util.Map;

/**
 * MigrationStatusManager: thread safe.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-6-27 created by Kevin Cao
 */
public class MigrationStatusManager {

	private long maxMemory;
	private long alertFreeMemory;
	private long warningFreeMemory;
	private int warningCommitCount;
	private int alertCommitCount;

	private final Map<String, DataMigrationStatus> dataMigrationStatus = new HashMap<String, DataMigrationStatus>();

	public static final int STATUS_CONTINUE = 0;
	public static final int STATUS_COMMIT = 1;
	public static final int STATUS_WAITING = 2;

	private long finishedImportTaskCount;
	private boolean hasOOMRisk;

	private final Object lockObj = new Object();
	private final Object lockObj2 = new Object();

	private long totalImportTaskCount;

	/**
	 * add a source's exported record count
	 * 
	 * @param owner of the object
	 * @param source name
	 * @param count of records
	 */
	public void addExpCount(String owner, String source, long count) {
		synchronized (lockObj) {
			String src = (owner == null ? "" : owner) + "." + source;
			DataMigrationStatus dms = dataMigrationStatus.get(src);
			if (dms == null) {
				dms = new DataMigrationStatus();
				dms.setSource(src);
				dataMigrationStatus.put(src, dms);
			}
			dms.addTotalExpCount(count);
		}
	}

	/**
	 * The importing threads processed record count, it it not the record count
	 * inserted successfully. In the end, the imported count should be equal
	 * with the exported count
	 * 
	 * @param owner of the object
	 * @param source String
	 * @param count processed count, including failed count
	 */
	public void addImpCount(String owner, String source, long count) {
		synchronized (lockObj) {
			String src = (owner == null ? "" : owner) + "." + source;
			DataMigrationStatus dms = dataMigrationStatus.get(src);
			if (dms == null) {
				dms = new DataMigrationStatus();
				dms.setSource(src);
				dataMigrationStatus.put(src, dms);
			}
			dms.addTotalImpCount(count);
		}
	}

	/**
	 * Get source exporting status
	 * 
	 * @param owner of the object
	 * @param source name
	 * @return records count
	 */
	public long getExpCount(String owner, String source) {
		synchronized (lockObj) {
			String src = (owner == null ? "" : owner) + "." + source;
			DataMigrationStatus dms = dataMigrationStatus.get(src);
			return dms == null ? 0 : dms.getTotalExpCount();
		}
	}

	/**
	 * Get source's exporting finished status
	 * 
	 * @param owner of the object
	 * @param source name
	 * @return true if the source is finished to export
	 */
	public boolean getExpFlag(String owner, String source) {
		synchronized (lockObj) {
			String src = (owner == null ? "" : owner) + "." + source;
			DataMigrationStatus dms = dataMigrationStatus.get(src);
			return dms != null && dms.isExpDoneFlag();
		}
	}

	/**
	 * Retrieves the finished importing task count
	 * 
	 * @return task count
	 */
	public long getFinishedImportTaskCount() {
		synchronized (lockObj2) {
			return finishedImportTaskCount;
		}
	}

	/**
	 * Get source importing status, including the records which are failed to be
	 * imported
	 * 
	 * @param owner of the object
	 * @param source name
	 * @return record count
	 */
	public long getImpCount(String owner, String source) {
		synchronized (lockObj) {
			String src = (owner == null ? "" : owner) + "." + source;
			DataMigrationStatus dms = dataMigrationStatus.get(src);
			return dms == null ? 0 : dms.getTotalImpCount();
		}

	}

	/**
	 * Retrieves the total importing task count
	 * 
	 * @return total count
	 */
	public long getTotalImportTaskCount() {
		synchronized (lockObj2) {
			return totalImportTaskCount;
		}
	}

	/**
	 * Increase finished importing task count
	 * 
	 */
	public void increaseFinishedImportTaskCount() {
		synchronized (lockObj2) {
			finishedImportTaskCount++;
		}
	}

	/**
	 * Increase total importing task count.
	 * 
	 */
	public void increaseTotalImportTaskCount() {
		synchronized (lockObj2) {
			totalImportTaskCount++;
		}
	}

	/**
	 * Tell the migration process whether to commit exported records now.
	 * 
	 * @param expName String
	 * @param currentCount int
	 * @param commitCount int
	 * @return 0:continue; 1:commit; 2:waiting.
	 */
	public int isCommitNow(String expName, int currentCount, int commitCount) {
		//According to commit count settings
		if (!hasOOMRisk) {
			return currentCount >= commitCount ? STATUS_COMMIT : STATUS_CONTINUE;
		}
		Runtime rt = Runtime.getRuntime();
		if (rt.totalMemory() >= maxMemory) {
			if (rt.freeMemory() <= warningFreeMemory) {
				return currentCount >= warningCommitCount ? STATUS_COMMIT : STATUS_CONTINUE;
			} else if (rt.freeMemory() <= alertFreeMemory) {
				return currentCount >= alertCommitCount ? STATUS_COMMIT : STATUS_WAITING;
			}
		}
		return currentCount >= commitCount ? STATUS_COMMIT : STATUS_CONTINUE;
	}

	/**
	 * Set all of the records from source is exported
	 * 
	 * @param owner of the object
	 * @param source name
	 */
	public void setExpFinished(String owner, String source) {
		synchronized (lockObj) {
			String src = (owner == null ? "" : owner) + "." + source;
			DataMigrationStatus dms = dataMigrationStatus.get(src);
			if (dms == null) {
				dms = new DataMigrationStatus();
				dms.setSource(src);
				dataMigrationStatus.put(src, dms);
			}
			dms.setExpDoneFlag(true);
		}
	}

	public void setHasOOMRisk(boolean hasOOMRisk) {
		this.hasOOMRisk = hasOOMRisk;
	}

	public void setMaxMemory(long maxMemory) {
		this.maxMemory = maxMemory;
	}

	public void setAlertFreeMemory(long alertFreeMemory) {
		this.alertFreeMemory = alertFreeMemory;
	}

	public void setWarningFreeMemory(long warningFreeMemory) {
		this.warningFreeMemory = warningFreeMemory;
	}

	public void setWarningCommitCount(int warningCommitCount) {
		this.warningCommitCount = warningCommitCount;
	}

	public void setAlertCommitCount(int alertCommitCount) {
		this.alertCommitCount = alertCommitCount;
	}
}