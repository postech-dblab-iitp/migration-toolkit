package com.cubrid.cubridmigration.ui.wizard.page;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;
import org.eclipse.jface.viewers.ColumnWeightData;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TableLayout;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.TableColumn;

import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.FK;
import com.cubrid.cubridmigration.core.dbobject.Schema;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Node;
import com.cubrid.cubridmigration.ui.database.GraphContentProvider;
import com.cubrid.cubridmigration.ui.database.GraphLabelProvider;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;

//GDB select table page.
public class GraphTableSelectPage extends MigrationWizardPage {

	private String[] columnNames = new String[] {"check box", "table name"};
	private TableViewer tableViewer;
	private TableViewer columnViewer;
	private Map<String, List<Column>> columnData = new HashMap<String, List<Column>>();
	private List<Table> tableList = new ArrayList<Table>();
	private List<Table> selectedTableList = new ArrayList<Table>();
	private boolean firstVisible = true;
	
	public GraphTableSelectPage(String pageName) {
		super(pageName);
	}
	
	@Override
	public void createControl(Composite parent) {
		//GD table select page createControl method
		Composite container = new Composite(parent, SWT.NONE);
		container.setLayout(new GridLayout());
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		SashForm sash = new SashForm(container, SWT.HORIZONTAL);
		sash.setLayout(new FillLayout());
		sash.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		createTableView(sash);
		createColumnView(sash);
		
		setControl(container);
	}
	
	//GDB left side. table list. bottom select all, deselect all button
	public void createTableView(SashForm sash) {
		Group groupContainer1 = new Group(sash, SWT.NONE);
		groupContainer1.setText("group container1");
		groupContainer1.setLayout(new FillLayout());
		
		tableViewer = new TableViewer(groupContainer1, SWT.FULL_SELECTION);
		tableViewer.setContentProvider(new GraphContentProvider());
		tableViewer.setLabelProvider(new GraphLabelProvider());
		
		TableLayout tableLayout = new TableLayout();
		tableLayout.addColumnData(new ColumnWeightData(7, true));
		tableLayout.addColumnData(new ColumnWeightData(93, true));
		
		tableViewer.getTable().setLayout(tableLayout);
		tableViewer.getTable().setLinesVisible(true);
		tableViewer.getTable().setHeaderVisible(true);
		
		TableColumn column1 = new TableColumn(tableViewer.getTable(), SWT.LEFT);
		TableColumn column2 = new TableColumn(tableViewer.getTable(), SWT.LEFT);
		column2.setText("table Name");
		
		tableViewer.addSelectionChangedListener(new ISelectionChangedListener() {
			@Override
			public void selectionChanged(SelectionChangedEvent event) {
				IStructuredSelection selection = (IStructuredSelection) event.getSelection();
				changeTableData(selection.getFirstElement());
				changeColumnData(selection.getFirstElement());
			}
		});
		
	}
	
	//GDB right side column info list. (column name, Data type) bottom Incremental Migration checkbox
	public void createColumnView(SashForm sash) {
		Group groupContainer2 = new Group(sash, SWT.NONE);
		groupContainer2.setText("group container2");
		groupContainer2.setLayout(new FillLayout());
		
		columnViewer = new TableViewer(groupContainer2, SWT.FULL_SELECTION);
		columnViewer.setContentProvider(new IStructuredContentProvider() {
			@Override
			@SuppressWarnings("unchecked")
			public Object[] getElements(Object inputElement) {
				if (inputElement instanceof ArrayList) {
					List<Column> columnList = (ArrayList<Column>) inputElement;
					
					return columnList.toArray();
				} else {
					return new Object[0];
				}
			}
			
			@Override
			public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {}
			
			@Override
			public void dispose() {}
			
		});
		
		columnViewer.setLabelProvider(new ITableLabelProvider() {
			@Override
			public String getColumnText(Object element, int columnIndex) {
				Column column = (Column) element;
				
				switch (columnIndex) {
				case 0:
					return column.getName();
				case 1:
					return column.getDataType();
				default :
					return null;
				}
			}
			
			@Override
			public Image getColumnImage(Object element, int columnIndex) {return null;}

			@Override
			public void addListener(ILabelProviderListener listener) {}

			@Override
			public void dispose() {}

			@Override
			public boolean isLabelProperty(Object element, String property) {return false;}

			@Override
			public void removeListener(ILabelProviderListener listener) {}
		});
		
		TableLayout columnLayout = new TableLayout();
		columnLayout.addColumnData(new ColumnWeightData(50, true));
		columnLayout.addColumnData(new ColumnWeightData(50, true));
		
		TableColumn column1 = new TableColumn(columnViewer.getTable(), SWT.LEFT);
		TableColumn column2 = new TableColumn(columnViewer.getTable(), SWT.LEFT);
		
		column1.setText("column Name");
		column2.setText("data Type");
		
		columnViewer.getTable().setLayout(columnLayout);
		columnViewer.getTable().setLinesVisible(true);
		columnViewer.getTable().setHeaderVisible(true);
	}
	
	public void showTableViewerData(List<Schema> schemaList) {
		for (Schema schema : schemaList) {
			List<Table> schemaTableList = schema.getTables();
			for (Table table : schemaTableList) {
				tableList.add(table);
			}
		}
		tableViewer.setInput(tableList);
	}
	
	public void makeColumnViewerData(List<Schema> schemaList) {
		
		for (Schema schema : schemaList) {
			List<Table> schemaTableList = schema.getTables();
			for (Table table : schemaTableList) {
				List<Column> columnList = table.getColumns();
				
				columnData.put(table.getName(), columnList);

				
				
//				columnViewer.setInput(columnList);
			}
		}
	}
	
	public void changeTableData(Object data) {
		if (data == null) {
			return;
		}
		Table table = (Table) data;
		if (table.isSelected()) {
			table.setSelected(false);
		} else {
			table.setSelected(true);
		}
		
		tableViewer.refresh();
	}
	
	public void changeColumnData(Object data) {
		if (data == null) {
			return;
		}
		Table table = (Table) data;
		List<Column> columnList = columnData.get(table.getName());
		
		columnViewer.setInput(columnList);
		columnViewer.refresh();
	}
	
	@Override
	protected void afterShowCurrentPage(PageChangedEvent event) {
		if (firstVisible) {
			final MigrationWizard mw = getMigrationWizard();
			setTitle(mw.getStepNoMsg(GraphTableSelectPage.this) + Messages.objectMapPageTitle);
			setDescription(Messages.objectMapPageDescription);
			
			setErrorMessage(null);
			
			Catalog sourceCatalog = mw.getSourceCatalog();
			
			List<Schema> schemaList = sourceCatalog.getSchemas();
			
	//		showTableInformationForGdbms(schemaList);
			
			showTableViewerData(schemaList);
			
			makeColumnViewerData(schemaList);
			
			firstVisible = false;
		}
	}
	
	protected void handlePageLeaving(PageChangingEvent event) {
		// If page is not complete, it should be go to previous page.
		
		if (!isPageComplete()) {
			return;
		}
		if (isGotoNextPage(event)) {
//			event.doit = updateMigrationConfig();
			event.doit = saveSelectedTable();
		}
	}
	
	//GDB error dialog
	public boolean saveSelectedTable() {
		selectedTableList.clear();
		for (Table table : tableList) {
			if (table.isSelected()) {
				selectedTableList.add(table);
			}
		}
		
		if (selectedTableList.isEmpty()) {
			MessageDialog.openError(getShell(), "No table selected", "no table selected. please select more than one table");
			
			return false;
		} else {
			showTableInformationForGdbms(selectedTableList);
		}
		
		return true;
	}
	
	private void showTableInformationForGdbms(List<Table> tables) {
		MigrationWizard mw = getMigrationWizard();
		
		GraphDictionary gdbDict = mw.getGraphDictionary();
		gdbDict.clean();
		
		List<Table> joinTablesEdgesList = new ArrayList<Table>();
		List<Table> intermediateNodesList = new ArrayList<Table>();
		List<Table> firstNodesList = new ArrayList<Table>();
		List<Table> secondNodesList = new ArrayList<Table>();
		List<Table> recursiveEdgesList = new ArrayList<Table>();

		for (Table table : tables) {

			int importedKeysCount = table.getImportedKeysCount();
			int exportedKeysCount = table.getExportedKeysCount();

			if (importedKeysCount == 2 && exportedKeysCount == 0) {
				joinTablesEdgesList.add(table);
			} else if (importedKeysCount >= 3) {
				intermediateNodesList.add(table);
			} else if (importedKeysCount == 0) {
				firstNodesList.add(table);
			} else if (!isRecursive(table) && (importedKeysCount > 0 || exportedKeysCount > 0)) {
				secondNodesList.add(table);
			} else if (isRecursive(table)) {
				recursiveEdgesList.add(table);
			}
		}

		printTableElement("JoinTables Edges", joinTablesEdgesList);
		printTableElement("Intermediate Nodes", intermediateNodesList);
		printTableElement("First Nodes", firstNodesList);
		printTableElement("Second Nodes", secondNodesList);
		printTableElement("Recursive Edges", recursiveEdgesList);
		
		migrateFirstNode(firstNodesList, gdbDict);
		migrateSecondNodes(secondNodesList, gdbDict);
		migrateRecursiveRelationship(recursiveEdgesList, gdbDict);
		migrateIntermediateNodes(intermediateNodesList, gdbDict);
		migrateJoinTablesEdges(joinTablesEdgesList, gdbDict);
		
		gdbDict.setNodeAndEdge();
		
//		gdbDict.printNodeAndEdge();
	}
	
	private boolean isRecursive(Table table){
		List<FK> fks = table.getFks();
		for (FK fk : fks) {
			String referencedTableName = fk.getReferencedTableName();
			if (referencedTableName.equalsIgnoreCase(table.getName())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * printTableElement
	 *
	 * @param elementType
	 * @param tableList
	 */
	private void printTableElement(String elementType, List<Table> tableList) {
		System.out.println("==== " + elementType + " ==== ");

		for (Table table : tableList) {
			System.out.println("- " + table.getName());
		}

		System.out.println();
	}
	
	//GDB first node
	private void migrateFirstNode(List<Table> firstNodeList, GraphDictionary gdbDict) {
		for (Table table : firstNodeList) {
			Node node = new Node();
			node.setNodeLabel(table.getName());
			node.setColumnList(table.getColumns());
			
			gdbDict.setMigratedNodeList(node);
		}
	}
	
	//GDB second node
	private void migrateSecondNodes(List<Table> secondNodeList, GraphDictionary gdbDict) {
		for (Table table : secondNodeList) {
			Node startNode = new Node();
			startNode.setNodeLabel(table.getName());
			startNode.setColumnList(table.getColumns());
			Edge edge = new Edge();

			gdbDict.setMigratedNodeList(startNode);
			
			for (FK fk : table.getFks()) {
				String endNodeName = null;
				Node endNode = gdbDict.getMigratedNodeByName(fk.getReferencedTableName());
				
				if (endNode != null) {
					endNodeName = endNode.getNodeLabel();
				}
				
				if (endNodeName == null) {
					for (Table selectedTable : selectedTableList) {
						if (selectedTable.getName().equals(fk.getReferencedTableName())) {
							Node migratedNode = new Node();
							migratedNode.setNodeLabel(fk.getReferencedTableName());
							migratedNode.setColumnList(selectedTable.getColumns());
							
							gdbDict.setMigratedNodeList(migratedNode);
						}
					}
					
				} else {
					edge.setEndNodeName(endNodeName);
				}
			}
			
			if (!edge.getEndNodeName().isEmpty()) {
				edge.setStartNodeName(startNode.getNodeLabel());
				gdbDict.setMigratedEdgeList(edge);
			}
		}
	}
	
	//GDB intermediate node
	private void migrateIntermediateNodes(List<Table> intermediateNodeList, GraphDictionary gdbDict){
		for (Table table : intermediateNodeList) {
			Node startNode = new Node();
			startNode.setNodeLabel(table.getName());
			startNode.setColumnList(table.getColumns());
			Edge edge = new Edge();

			gdbDict.setMigratedNodeList(startNode);
			
			for (FK fk : table.getFks()) {
				String endNodeName = null;
				Node endNode = gdbDict.getMigratedNodeByName(fk.getReferencedTableName());
				
				if (endNode != null) {
					endNodeName = endNode.getNodeLabel();
				}
				
				if (endNodeName == null) {
					for (Table selectedTable : selectedTableList) {
						if (selectedTable.getName().equals(fk.getReferencedTableName())) {
							Node migratedNode = new Node();
							migratedNode.setNodeLabel(fk.getReferencedTableName());
							migratedNode.setColumnList(selectedTable.getColumns());
							
							gdbDict.setMigratedNodeList(migratedNode);
						}
					}
					
				} else {
					edge.setEndNodeName(endNodeName);
				}
			}
			
			if (!edge.getEndNodeName().isEmpty()) {
				edge.setStartNodeName(startNode.getNodeLabel());
				gdbDict.setMigratedEdgeList(edge);
			}
			
//			Node startNode = new Node();
//			Edge edge = new Edge();
//			startNode.setNodeLabel(table.getName());
//			
//			gdbDict.setMigratedNodeList(startNode);
//			edge.setStartNodeName(startNode.getNodeLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endNodeName;
//				Node endNode = gdbDict.getMigratedNodeByName(fk.getReferencedTableName());
//				if (endNode == null) {
//					endNodeName = null;
//				} else {
//					endNodeName = endNode.getNodeLabel();
//				}
//				
//				if (endNodeName != null) {
//					edge.setEndNodeName(endNodeName);
//					
//				} else {
//					Node migratedNode = new Node();
//					migratedNode.setNodeLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedNodeList(migratedNode);
//				}
//			}
//			gdbDict.setMigratedEdgeList(edge);
		}
	}
	
	//GDB join table edges
	//GDB if start node or end node is null?
	private void migrateJoinTablesEdges(List<Table> joinTablesEdges, GraphDictionary gdbDict) {
		for (Table table : joinTablesEdges) {
			List<FK> fkList = table.getFks();
			
			Edge edge = new Edge();
			
			FK fk1 = fkList.get(0);
			FK fk2 = fkList.get(1);
			
			edge.setStartNodeName(fk1.getReferencedTableName());
			edge.setEndNodeName(fk2.getReferencedTableName());
			edge.setColumnList(table.getColumns());
			
			gdbDict.setMigratedEdgeList(edge);
		}
	}
	
	//GDB recursive relationship
	private void migrateRecursiveRelationship(List<Table> recursiveEdges, GraphDictionary gdbDict){
		for (Table table : recursiveEdges) {
			Node startNode = new Node();
			Edge edge = new Edge();
			
			startNode.setNodeLabel(table.getName());
			startNode.setColumnList(table.getColumns());
			
			gdbDict.setMigratedNodeList(startNode);
			
			edge.setStartNodeName(table.getName());
			edge.setEndNodeName(table.getName());
			
			Node node = gdbDict.getMigratedNodeByName(table.getName());
			
			if (node == null) {
				gdbDict.setMigratedNodeList(startNode);
			}
			
			gdbDict.setMigratedEdgeList(edge);
			
//			Node startNode = new Node();
//			Edge edge = new Edge();
//			startNode.setNodeLabel(table.getName());
//			
//			gdbDict.setMigratedNodeList(startNode);
//			edge.setStartNodeName(startNode.getNodeLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endNodeName;
//				Node endNode = gdbDict.getMigratedNodeByName(fk.getReferencedTableName());
//				if (endNode == null) {
//					endNodeName = null;
//				} else {
//					endNodeName = endNode.getNodeLabel();
//				}
//				
//				if (endNodeName != null) {
//					edge.setEndNodeName(endNodeName);
//					
//				} else {
//					Node migratedNode = new Node();
//					migratedNode.setNodeLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedNodeList(migratedNode);
//				}
//			}
//			gdbDict.setMigratedEdgeList(edge);
		}
	}
}
