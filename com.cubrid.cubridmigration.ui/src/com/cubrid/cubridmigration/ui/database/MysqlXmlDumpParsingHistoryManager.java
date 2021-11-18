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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.CUBRIDIOUtils;
import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.common.xml.IXMLMemento;
import com.cubrid.cubridmigration.core.common.xml.XMLMemento;

/**
 * Record XML dump file's parsing history into local machine.
 * 
 * @author Kevin Cao
 */
public final class MysqlXmlDumpParsingHistoryManager { //NOPMD
	private static final String XML_CATALOG_HISTORY_FILE_NAME = "XmlCatalogHistory.xml";
	private static final Logger LOG = LogUtil.getLogger(MysqlXmlDumpParsingHistoryManager.class);

	private static final List<MysqlXmlDumpParsingHistory> LIST = new ArrayList<MysqlXmlDumpParsingHistory>();

	static {
		File file = new File(PathUtils.getMonitorHistoryDir(), XML_CATALOG_HISTORY_FILE_NAME);
		if (file.exists()) {
			try {
				IXMLMemento xmlMemento = XMLMemento.loadMemento(file.getPath());
				if (null != xmlMemento) {
					IXMLMemento[] history = xmlMemento.getChildren("History");

					for (IXMLMemento xml : history) {
						String xmlFile = xml.getChild("xmlFile").getTextData();
						String xmlFileCharset = xml.getChild("xmlFileCharset").getTextData();
						String xmlFileLength = xml.getChild("xmlFileLength").getTextData();
						String jsonFileName = xml.getChild("jsonFileName").getTextData();

						MysqlXmlDumpParsingHistory hist = new MysqlXmlDumpParsingHistory(xmlFile, xmlFileCharset,
								Long.parseLong(xmlFileLength), jsonFileName);
						LIST.add(hist);
					}
				}
			} catch (IOException ex) {
				LOG.error("", ex);
			}
		}
	}

	/**
	 * 
	 * return saved XMLCatalogHistory list
	 * 
	 * @return List<XMLCatalogHistory>
	 */
	public static List<MysqlXmlDumpParsingHistory> getXMLCatalogHistoryList() {
		return new ArrayList<MysqlXmlDumpParsingHistory>(LIST);
	}

	/**
	 * save migration history
	 * 
	 * @param list List<MigrationHistory>
	 */
	private static void save(List<MysqlXmlDumpParsingHistory> list) {
		try {
			synchronized (MysqlXmlDumpParsingHistoryManager.class) {
				XMLMemento memento = XMLMemento.createWriteRoot("MigrationHistory");

				for (MysqlXmlDumpParsingHistory hist : list) {
					IXMLMemento history = memento.createChild("History");
					IXMLMemento xmlFile = history.createChild("xmlFile");
					xmlFile.putTextData(hist.getXmlFile());

					IXMLMemento xmlFileCharset = history.createChild("xmlFileCharset");
					xmlFileCharset.putTextData(hist.getXmlFileCharset());

					IXMLMemento xmlFileLength = history.createChild("xmlFileLength");
					xmlFileLength.putTextData(String.valueOf(hist.getXmlFileLength()));

					IXMLMemento jsonFileName = history.createChild("jsonFileName");
					jsonFileName.putTextData(hist.getJsonFileName());
				}

				String xmlString = memento.saveToString();

				File file = new File(PathUtils.getMonitorHistoryDir(),
						XML_CATALOG_HISTORY_FILE_NAME);

				if (!file.exists()) {
					boolean flag = file.createNewFile();
					LOG.info("create xml catalog history xml file:" + flag);
				}

				CUBRIDIOUtils.writeLines(file, new String[] {xmlString}, "UTF-8");
			}
		} catch (ParserConfigurationException ex) {
			LOG.error(ex);
		} catch (IOException ex) {
			LOG.error(ex);
		}
	}

	/**
	 * appendMigrationHistory
	 * 
	 * @param history MigrationHistory
	 */
	public static void appendMigrationHistory(MysqlXmlDumpParsingHistory history) {
		synchronized (MysqlXmlDumpParsingHistoryManager.class) {
			try {
				for (MysqlXmlDumpParsingHistory his : LIST) {
					if (his.getXmlFile().equals(history.getXmlFile())) {
						LIST.remove(his);
						break;
					}
				}
				LIST.add(history);
				save(LIST);
			} catch (Exception ex) {
				LOG.error(LogUtil.getExceptionString(ex));
			}
		}
	}

	/**
	 * removeMigrationHistory
	 * 
	 * @param history MigrationHistory
	 */
	public static void removeMigrationHistory(MysqlXmlDumpParsingHistory history) {
		synchronized (MysqlXmlDumpParsingHistoryManager.class) {
			try {
				if (LIST.contains(history)) {
					LIST.remove(history);
					save(LIST);
				}
			} catch (Exception ex) {
				LOG.error(LogUtil.getExceptionString(ex));
			}
		}
	}

	/**
	 * return XMLCatalogHistory by file
	 * 
	 * @param xmlFile String
	 * @param xmlFileCharset String
	 * @return XMLCatalogHistory
	 */
	public static MysqlXmlDumpParsingHistory getXMLCatalogHistory(String xmlFile, String xmlFileCharset) {
		for (MysqlXmlDumpParsingHistory history : LIST) {
			if (xmlFile.endsWith(history.getXmlFile())
					&& xmlFileCharset.equals(history.getXmlFileCharset())) {
				return history;
			}
		}
		return null;
	}
}
