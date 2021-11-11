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
package com.cubrid.cubridmigration.ui.common.navigator.event;

import java.util.List;

import com.cubrid.common.ui.navigator.DefaultCUBRIDNode;
import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Function;
import com.cubrid.cubridmigration.core.dbobject.Index;
import com.cubrid.cubridmigration.core.dbobject.PK;
import com.cubrid.cubridmigration.core.dbobject.PartitionInfo;
import com.cubrid.cubridmigration.core.dbobject.Procedure;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Sequence;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.core.dbobject.Trigger;
import com.cubrid.cubridmigration.core.dbobject.View;
import com.cubrid.cubridmigration.ui.common.navigator.node.ColumnNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ColumnsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.DatabaseNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FKNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FKsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FunctionNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.FunctionsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.IndexNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.IndexesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.PKNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.PartitionsNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ProcedureNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ProceduresNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SchemaNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SequenceNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.SequencesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.StoredProceduresNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TableNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TablesNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TriggerNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.TriggersNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ViewNode;
import com.cubrid.cubridmigration.ui.common.navigator.node.ViewsNode;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * This class is for managing all CUBRID Node in navigator tree
 * 
 * @author pangqiren
 */
public final class CubridNodeManager {

	private static final String PATH_PARTITIONS = "/partitions";
	private static final String PATH_INDEXES = "/indexes";
	private static final String PATH_FKS = "/fks";
	private static final String PATH_COLUMNS = "/columns";
	private static final String PATH_TRIGGERS = "/triggers";
	private static final String PATH_STORED_PROCEDURE = "/StoredProcedure";
	private static final String PATH_VIEWS = "/views";
	private static final String PATH_TABLES = "/tables";
	private static final String XML_HOST_NODE_ID = "MySQL dump file";

	private static volatile CubridNodeManager instance = null;
	private final static Object LOCKOBJ = new Object();

	/**
	 * Return the only CUBRID Node manager
	 * 
	 * @return CubridNodeManager
	 */
	public static CubridNodeManager getInstance() { // NOPMD
		synchronized (LOCKOBJ) {
			if (instance == null) {
				instance = new CubridNodeManager();
			}
			return instance;
		}

	};

	private CubridNodeManager() {
		//do nothing.
	}

	/**
	 * add sequence nodes
	 * 
	 * @param parentNode parentNode
	 * @param schema Schema
	 */
	private void addSerialNodes(DefaultCUBRIDNode parentNode, Schema schema) {
		String parentID = parentNode.getId();

		List<Sequence> sequenceList = schema.getSequenceList();
		String sequencesID = parentID + "/sequences";
		String sequencesLabels = Messages.labelTreeObjSerial + "(" + sequenceList.size() + ")";
		SequencesNode sequencesNode = new SequencesNode(sequencesID, sequencesLabels);
		parentNode.addChild(sequencesNode);
		if (sequenceList.isEmpty()) {
			sequencesNode.setContainer(false);
		}
		for (Sequence sequence : sequenceList) {
			// add a sequence
			String sequenceID = sequencesID + "/" + sequence.getName();
			String sequenceLabel = sequence.getName();
			SequenceNode sequenceNode = new SequenceNode(sequenceID, sequenceLabel);
			sequenceNode.setSequence(sequence);
			sequencesNode.addChild(sequenceNode);
		}
	}

	/**
	 * add store procedure and function nodes
	 * 
	 * @param parentNode parentNode
	 * @param schema Schema
	 */
	private void addSPFuncNodes(DefaultCUBRIDNode parentNode, Schema schema) {

		String parentID = parentNode.getId();

		String spID = parentID + PATH_STORED_PROCEDURE;
		String spLabels = "Stored Procedure";
		StoredProceduresNode spNode = new StoredProceduresNode(spID, spLabels);
		parentNode.addChild(spNode);

		// add Procedures
		List<Procedure> procedures = schema.getProcedures();
		String proceduresID = parentID + "/procedures";
		String proceduresLabels = Messages.labelTreeObjProcedure + "(" + procedures.size() + ")";
		ProceduresNode proceduresNode = new ProceduresNode(proceduresID, proceduresLabels);
		spNode.addChild(proceduresNode);

		for (Procedure procedure : procedures) {
			// add a procedure
			String procedureID = proceduresID + "/" + procedure.getName();
			String procedureLabels = procedure.getName();
			ProcedureNode procedureNode = new ProcedureNode(procedureID, procedureLabels);
			procedureNode.setProcedure(procedure);
			proceduresNode.addChild(procedureNode);
		}

		// add Functions
		List<Function> functions = schema.getFunctions();
		String functionsID = parentID + "/functions";
		String functionsLabels = Messages.labelTreeObjFunction + "(" + functions.size() + ")";
		FunctionsNode functionsNode = new FunctionsNode(functionsID, functionsLabels);
		spNode.addChild(functionsNode);

		for (Function function : functions) {
			// add a function
			String functionID = functionsID + "/" + function.getName();
			String functionLabels = function.getName();
			FunctionNode functionNode = new FunctionNode(functionID, functionLabels);
			functionNode.setFunction(function);
			functionsNode.addChild(functionNode);
		}
	}

	/**
	 * add table nodes
	 * 
	 * @param parentNode parentNode
	 * @param schema Schema
	 */
	private void addTableNodes(DefaultCUBRIDNode parentNode, Schema schema) {
		String parentID = parentNode.getId();

		List<Table> tables = schema.getTables();

		String tablesID = parentID + PATH_TABLES;
		String tablesLabels = Messages.labelTreeObjTable + "(" + tables.size() + ")";
		TablesNode tablesNode = new TablesNode(tablesID, tablesLabels);
		parentNode.addChild(tablesNode);

		if (tables.isEmpty()) {
			tablesNode.setContainer(false);
		}
		for (Table table : tables) {
			// add a table
			String tableID = tablesID + "/" + table.getName();
			String tableLabels = table.getName();
			TableNode tableNode = new TableNode(tableID, tableLabels);
			tableNode.setTable(table);
			tablesNode.addChild(tableNode);
			List<Column> columns = table.getColumns();

			String columnsID = tablesID + PATH_COLUMNS;
			String columnsLabels = Messages.labelTreeObjColumn + "(" + columns.size() + ")";
			ColumnsNode columnsNode = new ColumnsNode(columnsID, columnsLabels);
			tableNode.addChild(columnsNode);

			for (Column column : columns) {
				// add a column
				String columnID = columnsID + "/" + column.getName();
				String columnLabels = column.getName();
				ColumnNode columnNode = new ColumnNode(columnID, columnLabels);
				columnNode.setColumn(column);
				columnsNode.addChild(columnNode);
			}
			// add PK node
			PK pk = table.getPk();

			if (pk != null) {
				String pkID = tablesID + "/" + pk.getName();
				String pkLabels = pk.getName();
				PKNode pkNode = new PKNode(pkID, pkLabels);
				pkNode.setPk(pk);
				tableNode.addChild(pkNode);
			}

			// add FK nodes
			List<FK> fks = table.getFks();

			if (!fks.isEmpty()) {
				String fksID = tablesID + PATH_FKS;
				String fksLabels = Messages.labelTreeObjFk + "(" + fks.size() + ")";
				FKsNode fksNode = new FKsNode(fksID, fksLabels);
				tableNode.addChild(fksNode);

				for (FK fk : fks) {
					// add a fk
					String fkID = fksID + "/" + fk.getName();
					String fkLabels = fk.getName();
					FKNode fkNode = new FKNode(fkID, fkLabels);
					fkNode.setFk(fk);
					fksNode.addChild(fkNode);
				}
			}

			// add index nodes
			List<Index> indexes = table.getIndexes();

			if (!indexes.isEmpty()) {
				String indexesID = tablesID + PATH_INDEXES;
				String indexesLabels = Messages.labelTreeObjIndex + "(" + indexes.size() + ")";
				IndexesNode indexesNode = new IndexesNode(indexesID, indexesLabels);
				tableNode.addChild(indexesNode);

				for (Index index : indexes) {
					// add a fk
					String indexID = indexesID + "/" + index.getName();
					String indexLabels = index.getName();
					IndexNode indexNode = new IndexNode(indexID, indexLabels);
					indexNode.setIndex(index);
					indexesNode.addChild(indexNode);
				}
			}

			// add partition nodes
			PartitionInfo partitionInfo = table.getPartitionInfo();

			if (partitionInfo != null) {
				String partitionsID = tablesID + PATH_PARTITIONS;
				String partitionsLabels = Messages.labelTreeObjPartition;
				PartitionsNode partitionsNode = new PartitionsNode(partitionsID, partitionsLabels);
				partitionsNode.setPartitionInfo(partitionInfo);
				tableNode.addChild(partitionsNode);
				partitionsNode.setContainer(false);
			}
		}
	}

	/**
	 * add trigger nodes
	 * 
	 * @param parentNode parentNode
	 * @param schema Schema
	 */
	private void addTriggerNodes(DefaultCUBRIDNode parentNode, Schema schema) {
		String parentID = parentNode.getId();

		List<Trigger> triggers = schema.getTriggers();
		String triggersID = parentID + PATH_TRIGGERS;
		String triggersLabels = Messages.labelTreeObjTrigger + "(" + triggers.size() + ")";
		TriggersNode triggersNode = new TriggersNode(triggersID, triggersLabels);
		parentNode.addChild(triggersNode);

		for (Trigger trigger : triggers) {
			// add a trigger
			String triggerID = triggersID + "/" + trigger.getName();
			String triggerLabels = trigger.getName();
			TriggerNode triggerNode = new TriggerNode(triggerID, triggerLabels);
			triggerNode.setTrigger(trigger);
			triggersNode.addChild(triggerNode);
		}
	}

	/**
	 * add view nodes
	 * 
	 * @param parentNode parentNode
	 * @param schema Schema
	 */
	private void addViewNodes(DefaultCUBRIDNode parentNode, Schema schema) {
		String parentID = parentNode.getId();

		List<View> views = schema.getViews();
		String viewsID = parentID + PATH_VIEWS;
		String viewsLabels = Messages.labelTreeObjView + "(" + views.size() + ")";
		ViewsNode viewsNode = new ViewsNode(viewsID, viewsLabels);
		parentNode.addChild(viewsNode);
		if (views.isEmpty()) {
			viewsNode.setContainer(false);
		}
		for (View view : views) {
			// add a view
			String viewID = viewsID + "/" + view.getName();
			String viewLabels = view.getName();
			ViewNode viewNode = new ViewNode(viewID, viewLabels);
			viewNode.setView(view);
			viewsNode.addChild(viewNode);
		}
	}

	/**
	 * createDbNode
	 * 
	 * @param catalog Catalog
	 * @param hostNodeID String
	 * @return DatabaseNode
	 */
	public DatabaseNode createDbNode(Catalog catalog, String hostNodeID) {
		String dbName = catalog.getName();
		String dbNodeID;
		if (catalog.getConnectionParameters() == null) {
			dbNodeID = getDatabaseNodeID(hostNodeID, dbName, "xml");
		} else {
			dbNodeID = getDatabaseNodeID(hostNodeID, dbName,
					catalog.getConnectionParameters().getConUser());
		}
		DatabaseNode databaseNode = new DatabaseNode(dbNodeID, dbName);

		if (XML_HOST_NODE_ID.endsWith(hostNodeID)) {
			databaseNode.setXMLDatabase(true);
		} else {
			databaseNode.setXMLDatabase(false);
		}

		for (Schema schema : catalog.getSchemas()) {
			DefaultCUBRIDNode parentNode;
			String schemaNodeLabel = schema.getName();
			String schemaNodeID = String.format("%s/%s", dbNodeID, schemaNodeLabel);

			SchemaNode schemNode = new SchemaNode(schemaNodeID, schemaNodeLabel);
			schemNode.setSchema(schema);

			databaseNode.addChild(schemNode);
			parentNode = schemNode;

			addTableNodes(parentNode, schema);

			addViewNodes(parentNode, schema);

			addSPFuncNodes(parentNode, schema);

			addTriggerNodes(parentNode, schema);

			addSerialNodes(parentNode, schema);
		}

		return databaseNode;
	}

	/**
	 * get database node ID
	 * 
	 * @param hostNodeID String
	 * @param dbName String
	 * @param userName String
	 * @return String
	 */
	private String getDatabaseNodeID(String hostNodeID, String dbName, String userName) {
		return String.format("%s/%s/%s", hostNodeID, dbName, userName);
	}

}
