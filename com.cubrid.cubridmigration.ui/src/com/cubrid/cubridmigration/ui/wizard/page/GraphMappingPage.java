package com.cubrid.cubridmigration.ui.wizard.page;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.viewers.ColumnWeightData;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.LabelProvider;
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
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.zest.core.viewers.EntityConnectionData;
import org.eclipse.zest.core.viewers.GraphViewer;
import org.eclipse.zest.core.viewers.IGraphEntityContentProvider;
import org.eclipse.zest.layouts.algorithms.GridLayoutAlgorithm;

import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Node;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;

//GDB override ObjectMappingPage. GraphMappingPage seems to have a similar structure to ObjectMappingPage
public class GraphMappingPage extends MigrationWizardPage {

	private GraphViewer graphViewer;
	private TableViewer columnViewer;
	
	public GraphMappingPage(String pageName) {
		super(pageName);
		//GDB mapping page constructor
	}

	@Override
	public void createControl(Composite parent) {
		//GDB mapping page create control
		
		Composite container = new Composite(parent, SWT.NONE);
		container.setLayout(new FillLayout());
		container.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		SashForm sash = new SashForm(container, SWT.HORIZONTAL);
		sash.setLayout(new FillLayout());
		sash.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		createGraphView(sash);
		createTableView(sash);
		
		sash.setWeights(new int[] {1, 1});
		sash.setSashWidth(10);
		
		setControl(container);
	}
	
	//GDB this will show right side widget. show node list and edge list
	public void createGraphView(SashForm parent) {
		
		Group groupContainer1 = new Group(parent, SWT.NONE);
		groupContainer1.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		groupContainer1.setLayout(new GridLayout());
		groupContainer1.setText("sash 1");
		
		TabFolder tabFolder = new TabFolder(groupContainer1, SWT.NONE);
		tabFolder.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		Group groupContainer3 = new Group(tabFolder, SWT.NONE);
		groupContainer3.setText("group 3");
		
		createGraph(tabFolder);
		
		Group groupContainer4 = new Group(tabFolder, SWT.NONE);
		groupContainer4.setText("group 4");
		
		TabItem folder1 = new TabItem(tabFolder, SWT.NONE);
		folder1.setText("Node");
		folder1.setControl(graphViewer.getControl());
		
		TabItem folder2 = new TabItem(tabFolder, SWT.NONE);
		folder2.setText("Edge");
		folder2.setControl(groupContainer4);
		
	}
	
	public void createGraph(Composite parent) {
		graphViewer = new GraphViewer(parent, SWT.BORDER);
		graphViewer.setContentProvider(new IGraphEntityContentProvider() {

			@Override
			@SuppressWarnings("unchecked")
			public Object[] getElements(Object inputElement) {
				if (inputElement instanceof List) {
					List<Node> nodeList = (ArrayList<Node>) inputElement;
					
					return nodeList.toArray();
				} else {
					return new Object[0];
				}
			}
			
			@Override
			public Object[] getConnectedTo(Object entity) {
				if (entity instanceof Node) {
					Node nodeList = (Node) entity;
					
					List<Node> list = nodeList.getEndNodes();
					
					if (list == null) {
						return new Object[0];
					} else {
						return nodeList.getEndNodes().toArray();
					}
				} else {
					return new Object[0];
				}
			}
			
			@Override
			public void dispose() {}

			@Override
			public void inputChanged(Viewer viewer, Object oldInput,
					Object newInput) {}

		});
		graphViewer.setLabelProvider(new LabelProvider() {
			@Override
			public String getText(Object element) {
				if (element instanceof Node) {
					Node node = (Node) element;
					return node.getNodeLabel();
					
				} if (element instanceof EntityConnectionData) {
					EntityConnectionData test = (EntityConnectionData) element;
					Node startNode = (Node) test.source;
					Node endNode = (Node) test.dest;
					
					return "" + startNode.getNodeLabel() + "_" + endNode.getNodeLabel();
				}
				
				return null;
			}
		
		});
		
		graphViewer.setLayoutAlgorithm(new GridLayoutAlgorithm());
		graphViewer.addSelectionChangedListener(new ISelectionChangedListener() {
			@Override
			public void selectionChanged(SelectionChangedEvent event) {
				IStructuredSelection selection = (IStructuredSelection) event.getSelection();
				changeColumnData(selection.getFirstElement());
				
			}
		});
		graphViewer.applyLayout();
	}
	
	public void changeColumnData(Object data) {
		if (data == null) {
			return;
		}
		Node node = (Node) data;
		List<Column> columnList = node.getColumnList();
		
		columnViewer.setInput(columnList);
		columnViewer.refresh();
		
	}
	
	public void createTableView(Composite parent) {
		Group groupContainer = new Group(parent, SWT.NONE);
		groupContainer.setLayout(new FillLayout());
		groupContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		groupContainer.setText("sash 2");
		
//		TabFolder tabFolder = new TabFolder(groupContainer, SWT.NONE);
//		tabFolder.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
//		
//		Group groupContainer3 = new Group(tabFolder, SWT.NONE);
//		groupContainer3.setText("group 3");
//		
//		Group groupContainer4 = new Group(tabFolder, SWT.NONE);
//		groupContainer3.setText("group 4");
//		
//		TabItem folder1 = new TabItem(tabFolder, SWT.NONE);
//		folder1.setText("Node");
//		folder1.setControl(groupContainer3);
//		
//		TabItem folder2 = new TabItem(tabFolder, SWT.NONE);
//		folder2.setText("Edge");
//		folder2.setControl(groupContainer4);
		
		columnViewer = new TableViewer(groupContainer, SWT.FULL_SELECTION);
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
					return null;
				case 1:
					return column.getName();
				case 2:
					return column.getDataType();
					
					//GDB column 3, 4 is migrated column.
					//GDB todo: make this column editable
				case 3:
					return column.getName();
				case 4:
					return null;
				}
				
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Image getColumnImage(Object element, int columnIndex) {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public void removeListener(ILabelProviderListener listener) {}
			
			@Override
			public boolean isLabelProperty(Object element, String property) {return false;}
			
			@Override
			public void dispose() {}
			
			@Override
			public void addListener(ILabelProviderListener listener) {}
			
		});
		
		TableLayout tableLayout = new TableLayout();
		tableLayout.addColumnData(new ColumnWeightData(4, true));
		tableLayout.addColumnData(new ColumnWeightData(24, true));
		tableLayout.addColumnData(new ColumnWeightData(24, true));
		tableLayout.addColumnData(new ColumnWeightData(24, true));
		tableLayout.addColumnData(new ColumnWeightData(24, true));
		
		columnViewer.getTable().setLayout(tableLayout);
		columnViewer.getTable().setLinesVisible(true);
		columnViewer.getTable().setHeaderVisible(true);
		
		TableColumn col1 = new TableColumn(columnViewer.getTable(), SWT.LEFT);
		TableColumn col2 = new TableColumn(columnViewer.getTable(), SWT.LEFT);
		TableColumn col3 = new TableColumn(columnViewer.getTable(), SWT.LEFT);
		TableColumn col4 = new TableColumn(columnViewer.getTable(), SWT.LEFT);
		TableColumn col5 = new TableColumn(columnViewer.getTable(), SWT.LEFT);
		
		col2.setText("Column name");
		col3.setText("Data Type");
		col4.setText("Property Name");
		col5.setText("Graph Type");
	}
	
	public void showGraphData(List<Node> nodeList) {
		graphViewer.setInput(nodeList);
	}
	
	//GDB GraphMappingPage -> afterShowCurrentPage
	protected void afterShowCurrentPage(PageChangedEvent event) {
		final MigrationWizard mw = getMigrationWizard();
		setTitle(mw.getStepNoMsg(GraphMappingPage.this) + Messages.objectMapPageTitle);
		setDescription(Messages.objectMapPageDescription);
		
		setErrorMessage(null);
		
		Catalog sourceCatalog = mw.getSourceCatalog();
	
		GraphDictionary gdbDict = mw.getGraphDictionary();
		
		gdbDict.printNodeAndEdge();
		
		
//		showTableInformationForGdbms(sourceCatalog);
		
		showGraphData(gdbDict.getMigratedNodeList());
	}
	
//	private void showTableInformationForGdbms(Catalog sourceCatalog) {
//		MigrationWizard mw = getMigrationWizard();
//		
//		GraphDictionary gdbDict = mw.getGraphDictionary();
//		
//		List<Schema> schemas = sourceCatalog.getSchemas();
//
//		List<Table> joinTablesEdgesList = new ArrayList<Table>();
//		List<Table> intermediateNodesList = new ArrayList<Table>();
//		List<Table> firstNodesList = new ArrayList<Table>();
//		List<Table> secondNodesList = new ArrayList<Table>();
//		List<Table> recursiveEdgesList = new ArrayList<Table>();
//
//		for (Schema schema : schemas) {
//			List<Table> tables = schema.getTables();
//			for (Table table : tables) {
//
//				int importedKeysCount = table.getImportedKeysCount();
//				int exportedKeysCount = table.getExportedKeysCount();
//
//				if (importedKeysCount == 2 && exportedKeysCount == 0) {
//					joinTablesEdgesList.add(table);
//				} else if (importedKeysCount >= 3) {
//					intermediateNodesList.add(table);
//				} else if (importedKeysCount == 0) {
//					firstNodesList.add(table);
//				} else if (importedKeysCount > 0 || exportedKeysCount > 0) {
//					secondNodesList.add(table);
//				}
//
//				List<FK> fks = table.getFks();
//				for (FK fk : fks) {
//					String referencedTableName = fk.getReferencedTableName();
//					if (referencedTableName.equalsIgnoreCase(table.getName())) {
//						recursiveEdgesList.add(table);
//					}
//				}
//			}
//		}
//
//		printTableElement("JoinTables Edges", joinTablesEdgesList);
//		printTableElement("Intermediate Nodes", intermediateNodesList);
//		printTableElement("First Nodes", firstNodesList);
//		printTableElement("Second Nodes", secondNodesList);
//		printTableElement("Recursive Edges", recursiveEdgesList);
//		
//		migrateFirstNode(firstNodesList, gdbDict);
//		migrateSecondNodes(secondNodesList, gdbDict);
//		migrateIntermediateNodes(intermediateNodesList, gdbDict);
//		migrateJoinTablesEdges(joinTablesEdgesList, gdbDict);
//		migrateRecursiveRelationship(recursiveEdgesList, gdbDict);
//		
//		gdbDict.printNodeAndEdge();
//	}
//
//	/**
//	 * printTableElement
//	 *
//	 * @param elementType
//	 * @param tableList
//	 */
//	private void printTableElement(String elementType, List<Table> tableList) {
//		System.out.println("==== " + elementType + " ==== ");
//
//		for (Table table : tableList) {
//			System.out.println("- " + table.getName());
//		}
//
//		System.out.println();
//	}
//	
//	//GDB first node
//	private void migrateFirstNode(List<Table> firstNodeList, GraphDictionary gdbDict) {
//		for (Table table : firstNodeList) {
//			Node node = new Node();
//			node.setNodeLabel(table.getName());
//			
//			gdbDict.setMigratedNodeList(node);
//		}
//	}
//	
//	//GDB second node
//	private void migrateSecondNodes(List<Table> secondNodeList, GraphDictionary gdbDict) {
//		for (Table table : secondNodeList) {
//			Node startNode = new Node();
//			Edge edge = new Edge();
//			startNode.setNodeLabel(table.getName());
//			
//			gdbDict.setMigratedNodeList(startNode);
//			edge.setStartNodeName(startNode.getNodeLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endNodeName = gdbDict.getMigratedNodeByName(fk.getReferencedTableName()).getNodeLabel();
//				
//				if (endNodeName != null) {
//					edge.setEndNodeName(endNodeName);
//					gdbDict.setMigratedEdgeList(edge);
//					
//				} else {
//					Node endNode = new Node();
//					endNode.setNodeLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedNodeList(endNode);
//				}
//			}
//		}
//	}
//	
//	//GDB intermediate node
//	private void migrateIntermediateNodes(List<Table> intermediateNodeList, GraphDictionary gdbDict){
//		for (Table table : intermediateNodeList) {
//			Node startNode = new Node();
//			Edge edge = new Edge();
//			startNode.setNodeLabel(table.getName());
//			
//			gdbDict.setMigratedNodeList(startNode);
//			edge.setStartNodeName(startNode.getNodeLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endNodeName = gdbDict.getMigratedNodeByName(fk.getReferencedTableName()).getNodeLabel();
//				
//				if (endNodeName != null) {
//					edge.setEndNodeName(endNodeName);
//					gdbDict.setMigratedEdgeList(edge);
//					
//				} else {
//					Node endNode = new Node();
//					endNode.setNodeLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedNodeList(endNode);
//				}
//			}
//		}
//	}
//	
//	//GDB join table edges
//	private void migrateJoinTablesEdges(List<Table> joinTablesEdges, GraphDictionary gdbDict) {
//		for (Table table : joinTablesEdges) {
//			Node startNode = new Node();
//			Edge edge = new Edge();
//			startNode.setNodeLabel(table.getName());
//			
//			gdbDict.setMigratedNodeList(startNode);
//			edge.setStartNodeName(startNode.getNodeLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endNodeName = gdbDict.getMigratedNodeByName(fk.getReferencedTableName()).getNodeLabel();
//				
//				if (endNodeName != null) {
//					edge.setEndNodeName(endNodeName);
//					gdbDict.setMigratedEdgeList(edge);
//					
//				} else {
//					Node endNode = new Node();
//					endNode.setNodeLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedNodeList(endNode);
//				}
//			}
//		}
//	}
//	
//	//GDB recursive relationship
//	private void migrateRecursiveRelationship(List<Table> recursiveEdges, GraphDictionary gdbDict){
//		for (Table table : recursiveEdges) {
//			Node startNode = new Node();
//			Edge edge = new Edge();
//			startNode.setNodeLabel(table.getName());
//			
//			gdbDict.setMigratedNodeList(startNode);
//			edge.setStartNodeName(startNode.getNodeLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endNodeName = gdbDict.getMigratedNodeByName(fk.getReferencedTableName()).getNodeLabel();
//				
//				if (endNodeName != null) {
//					edge.setEndNodeName(endNodeName);
//					gdbDict.setMigratedEdgeList(edge);
//					
//				} else {
//					Node endNode = new Node();
//					endNode.setNodeLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedNodeList(endNode);
//				}
//			}
//		}
//	}
}
