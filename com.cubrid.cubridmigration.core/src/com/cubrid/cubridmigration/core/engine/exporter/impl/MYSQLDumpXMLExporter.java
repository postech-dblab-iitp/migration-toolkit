///*
// * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search Solution. 
// *
// * Redistribution and use in source and binary forms, with or without modification, 
// * are permitted provided that the following conditions are met: 
// *
// * - Redistributions of source code must retain the above copyright notice, 
// *   this list of conditions and the following disclaimer. 
// *
// * - Redistributions in binary form must reproduce the above copyright notice, 
// *   this list of conditions and the following disclaimer in the documentation 
// *   and/or other materials provided with the distribution. 
// *
// * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
// *   may be used to endorse or promote products derived from this software without 
// *   specific prior written permission. 
// *
// * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
// * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
// * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
// * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
// * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
// * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
// * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
// * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
// * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
// * OF SUCH DAMAGE. 
// *
// */
//package com.cubrid.cubridmigration.core.engine.exporter.impl;
//
//import java.io.FileInputStream;
//import java.io.Reader;
//import java.util.ArrayList;
//import java.util.List;
//
//import javax.xml.stream.XMLInputFactory;
//import javax.xml.stream.XMLStreamConstants;
//import javax.xml.stream.XMLStreamReader;
//
//import org.xml.sax.Attributes;
//
//import com.ctc.wstx.api.WstxInputProperties;
//import com.ctc.wstx.stax.WstxInputFactory;
//
//import com.cubrid.cubridmigration.core.engine.RecordExportedListener;
//import com.cubrid.cubridmigration.core.engine.config.SourceTableConfig;
//import com.cubrid.cubridmigration.core.engine.event.MigrationErrorEvent;
//import com.cubrid.cubridmigration.core.engine.exception.BreakMigrationException;
//import com.cubrid.cubridmigration.core.engine.exporter.MigrationExporter;
//import com.cubrid.cubridmigration.core.io.RmInvalidXMLCharReader;
//import com.cubrid.cubridmigration.graph.dbobj.Edge;
//import com.cubrid.cubridmigration.graph.dbobj.Vertex;
//
///**
// * 
// * MYSQLDumpXMLMigrationExporter Description
// * 
// * @author Kevin Cao
// * @version 1.0 - 2011-8-9 created by Kevin Cao
// */
//@SuppressWarnings("restriction")
//public class MYSQLDumpXMLExporter extends
//		MigrationExporter {
//
//	private PerformMYSQLXMLDataReader handler;
//
//	/**
//	 * export all tables' record
//	 * 
//	 * @param oneNewRecord RecordExportedListener
//	 */
//	public void exportAllRecords(RecordExportedListener oneNewRecord) {
//		if (config == null || eventHandler == null || handler == null) {
//			throw new BreakMigrationException(
//					"MYSQLDumpXMLExporter was not constructed.");
//		}
//		List<Character> invalidateChars = new ArrayList<Character>();
//		try {
//			Reader is = new RmInvalidXMLCharReader(new FileInputStream(
//					config.getSourceFileName()),
//					config.getSourceFileEncoding(), invalidateChars);
//			try {
//				XMLInputFactory factory = WstxInputFactory.newInstance();
//				factory.setProperty(WstxInputProperties.P_NORMALIZE_LFS, false);
//				XMLStreamReader reader = factory.createXMLStreamReader(is);
//				handler.setOneNewRecord(oneNewRecord);
//				handler.setInvalidateChars(invalidateChars);
//				STAX2AttributesAdapter attributes = new STAX2AttributesAdapter(
//						reader);
//				while (reader.hasNext()) {
//					if (interrupted) {
//						return;
//					}
//					int event = reader.next();
//					switch (event) {
//					case XMLStreamConstants.START_ELEMENT: {
//						handler.startElement("", reader.getLocalName(),
//								reader.getName().getLocalPart(), attributes);
//						break;
//					}
//					case XMLStreamConstants.END_ELEMENT: {
//						handler.endElement("", reader.getLocalName(),
//								reader.getName().getLocalPart());
//						if (handler.isParsingCompleted()) {
//							return;
//						}
//						break;
//					}
//					case XMLStreamConstants.CHARACTERS: {
//						handler.characters(reader.getText().toCharArray(), 0,
//								reader.getTextLength());
//						break;
//					}
//					case XMLStreamConstants.CDATA: {
//						handler.characters(reader.getText().toCharArray(), 0,
//								reader.getTextLength());
//						break;
//					}
//					default:
//						break;
//					}
//				}
//			} finally {
//				is.close();
//			}
//		} catch (OutOfMemoryError error) {
//			eventHandler.handleEvent(new MigrationErrorEvent(error));
//		} catch (Exception e) {
//			eventHandler.handleEvent(new MigrationErrorEvent(e));
//		}
//	}
//
//	/**
//	 * Export table records
//	 * 
//	 * @param st SourceTableConfig
//	 * @param oneNewRecord RecordExportedListener
//	 */
//	public void exportTableRecords(SourceTableConfig st,
//			RecordExportedListener oneNewRecord) {
//		throw new BreakMigrationException("Method is not supported.");
//	}
//
//	/**
//	 * 
//	 * STAX2AttributesAdapter implements org.xml.sax.Attributes to reuse the XML
//	 * content handler
//	 * 
//	 * @author Kevin Cao
//	 * @version 1.0 - 2012-2-1 created by Kevin Cao
//	 */
//	private static class STAX2AttributesAdapter implements
//			Attributes {
//		private final XMLStreamReader reader;
//
//		public STAX2AttributesAdapter(XMLStreamReader reader) {
//			this.reader = reader;
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @return not supported
//		 */
//		public int getLength() {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param index Integer
//		 * @return not supported
//		 */
//		public String getURI(int index) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param index Integer
//		 * @return not supported
//		 */
//		public String getLocalName(int index) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param index Integer
//		 * @return not supported
//		 */
//		public String getQName(int index) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param index Integer
//		 * @return not supported
//		 */
//		public String getType(int index) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param index Integer
//		 * @return not supported
//		 */
//		public String getValue(int index) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param uri String
//		 * @param localName String
//		 * @return not supported
//		 */
//		public int getIndex(String uri, String localName) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param qName String
//		 * @return not supported
//		 */
//		public int getIndex(String qName) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param uri String
//		 * @param localName String
//		 * @return not supported
//		 */
//		public String getType(String uri, String localName) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param qName String
//		 * @return not supported
//		 */
//		public String getType(String qName) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Not supported
//		 * 
//		 * @param uri String
//		 * @param localName String
//		 * @return not supported
//		 */
//		public String getValue(String uri, String localName) {
//			throw new RuntimeException("Method is not supported.");
//		}
//
//		/**
//		 * Retrieves the attribute value by attribute name
//		 * 
//		 * @param qName the attribute name with name-space prefix like "xsi:nil"
//		 * @return the attribute value
//		 */
//		public String getValue(String qName) {
//			String[] names = qName.split(":");
//			String nameSp = names.length > 1 ? names[0] : null;
//			String attrName = names.length > 1 ? names[1] : qName;
//			for (int i = 0; i < reader.getAttributeCount(); i++) {
//				boolean flag = attrName.equals(reader.getAttributeLocalName(i));
//				if ((nameSp == null && flag)
//						|| (nameSp != null
//								&& nameSp.equals(reader.getAttributePrefix(i)) && flag)) {
//					return reader.getAttributeValue(i);
//				}
//			}
//			return null;
//		}
//	}
//
//	public void setHandler(PerformMYSQLXMLDataReader handler) {
//		this.handler = handler;
//	}
//
//	public void exportGraphVertexRecords(Vertex v,
//			RecordExportedListener oneNewRecord) {
//		
//	}
//
//	public void exportGraphEdgeRecords(Edge e,
//			RecordExportedListener oneNewRecord) {
//		
//	}
//
//	public void exportCDCObject(Vertex vertex, Edge edge,
//			RecordExportedListener mrManager) {
//		// TODO Auto-generated method stub
//		
//	}
//}
