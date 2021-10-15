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
package com.cubrid.cubridmigration.ui.database;

import java.io.File;
import java.text.MessageFormat;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.swt.widgets.Display;

import com.cubrid.cubridmigration.core.dbmetadata.IDBSchemaInfoFetcher;
import com.cubrid.cubridmigration.core.dbmetadata.IDBSource;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.io.IReaderEvent;
import com.cubrid.cubridmigration.mysql.MysqlXmlDumpSource;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * Fetch MYSQL XML dump file's schema in a progress dialog. Users can check the
 * parsing progress and cancel the progress.
 * 
 * @author Kevin Cao
 */
public class MysqlXmlDumpSchemaProgressFetcher extends
		SchemaFetcherWithProgress {
	private final boolean hisFirst;

	public MysqlXmlDumpSchemaProgressFetcher(boolean hisFirst) {
		this.hisFirst = hisFirst;
	}

	/**
	 * Create schema information fetcher, if property <hisFirst> is true, the
	 * fetcher will be <MysqlXmlDumpSchemaWithHistoryFetcher>
	 * 
	 * @return IDBSchemaInfoFetcher
	 */
	protected IDBSchemaInfoFetcher createFetcher() {
		if (hisFirst) {
			return new MysqlXmlDumpSchemaWithHistoryFetcher();
		}
		return super.createFetcher();
	}

	/**
	 * Initialize the <IProgressMonitor> and create thread to run
	 * <IDBSchemaInfoFetcher>
	 * 
	 * @param fetcher to be executed.
	 * @param pm IProgressMonitor
	 * 
	 * @return The fetching thread
	 */
	protected Thread startFetchingThread(final IDBSchemaInfoFetcher fetcher,
			final IProgressMonitor pm) {
		final MysqlXmlDumpSource ds = (MysqlXmlDumpSource) dbSource;
		String xmlFile = ds.getFileName();
		long length = new File(xmlFile).length();
		final long factor = getFactor(length);
		final int pmLength = (int) (length / factor);
		final String name = Messages.progressMetadata;
		pm.beginTask(name, pmLength);

		final IReaderEvent readerEvent = new IReaderEvent() {
			private long workCounter = 0;
			private long progress = 0;
			private long lastShowTime = 0;
			private long startTime = 0;

			public void readChars(final int count) {
				final Runnable runnable = new Runnable() {
					public void run() {
						workCounter = workCounter + count;
						long worked = workCounter / factor;
						if (worked == 0) {
							return;
						}
						workCounter = workCounter % factor;
						pm.worked((int) worked);

						progress = progress + worked;
						//Refresh time remaining per 2 seconds.
						if (startTime == 0) {
							startTime = System.currentTimeMillis();
							lastShowTime = System.currentTimeMillis();
							return;
						}
						if ((System.currentTimeMillis() - lastShowTime) < 2000) {
							return;
						}
						lastShowTime = System.currentTimeMillis();
						final long timeUsed = lastShowTime - startTime;
						long msRemain = ((pmLength - progress) * timeUsed / progress) / 1000;
						pm.setTaskName(name
								+ MessageFormat.format(Messages.msgTimeRemaining, msRemain));
					}
				};
				Display.getDefault().syncExec(runnable);
			}
		};
		Thread thread = new Thread(Messages.msgFetchingXMLSchema) {
			public void run() {
				try {
					MysqlXmlDumpSource tempDs = ((MysqlXmlDumpSource) dbSource).clone();
					tempDs.setEvent(readerEvent);
					catalog = fetcher.fetchSchema(tempDs, null);
				} catch (Exception ex) {
					exception = ex;
				} finally {
					isFinished = true;
				}
			}
		};
		thread.start();
		return thread;
	}

	/**
	 * Retrieves the factor used by file length shown.
	 * 
	 * @param len file length
	 * @return 1:Byte 1024:KB 1024*1024=MB ...
	 */
	protected long getFactor(long len) {
		long factor = 1;
		while ((len / factor) > Integer.MAX_VALUE) {
			factor = factor * 1024;
		}
		return factor;
	}

	/**
	 * return Catalog object, the new catalog object will be added into
	 * <CubridNodeManager> automatically
	 * 
	 * @param ds ConnParameters
	 * @param hisFirst boolean
	 * @return Catalog
	 */
	public static Catalog fetch(IDBSource ds, boolean hisFirst) {
		MysqlXmlDumpSchemaProgressFetcher runnable = new MysqlXmlDumpSchemaProgressFetcher(hisFirst);
		runnable.setDBSource(ds);
		Catalog catalog = runnable.fetch();
		return catalog;
	}
}
