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
import org.eclipse.zest.core.widgets.ZestStyles;
import org.eclipse.zest.layouts.LayoutStyles;
import org.eclipse.zest.layouts.algorithms.GridLayoutAlgorithm;

import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;

//GDB override ObjectMappingPage. GraphMappingPage seems to have a similar structure to ObjectMappingPage
public class GraphMappingPage extends MigrationWizardPage {

	private GraphViewer graphViewer;
	private TableViewer columnViewer;
	
	private TableViewer gdbTable;
	private TableViewer rdbTable;
	
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
	
	//GDB this will show right side widget. show vertex list and edge list
	public void createGraphView(SashForm parent) {
		
		Group groupContainer1 = new Group(parent, SWT.NONE);
		groupContainer1.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		groupContainer1.setLayout(new GridLayout());
		groupContainer1.setText(Messages.msgGraph);
		
		TabFolder tabFolder = new TabFolder(groupContainer1, SWT.NONE);
		tabFolder.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		createGraph(tabFolder);
		
		TabItem folder1 = new TabItem(tabFolder, SWT.NONE);
		folder1.setText("Vertex");
		folder1.setControl(graphViewer.getControl());
		
		TabItem folder2 = new TabItem(tabFolder, SWT.NONE);
		folder2.setText("Edge");
		
	}
	
	public void createGraph(Composite parent) {
		graphViewer = new GraphViewer(parent, SWT.BORDER);
		graphViewer.setConnectionStyle(ZestStyles.CONNECTIONS_DIRECTED);
		graphViewer.setLayoutAlgorithm(new GridLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING));;
		graphViewer.setContentProvider(new IGraphEntityContentProvider() {

			@Override
			@SuppressWarnings("unchecked")
			public Object[] getElements(Object inputElement) {
				if (inputElement instanceof List) {
					List<Vertex> vertexList = (ArrayList<Vertex>) inputElement;
					
					return vertexList.toArray();
				} else {
					return new Object[0];
				}
			}
			
			@Override
			public Object[] getConnectedTo(Object entity) {
				if (entity instanceof Vertex) {
					Vertex vertexList = (Vertex) entity;
					
					List<Vertex> list = vertexList.getEndVertexes();
					
					if (list == null) {
						return new Object[0];
					} else {
						return vertexList.getEndVertexes().toArray();
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
				if (element instanceof Vertex) {
					Vertex vertex = (Vertex) element;
					return vertex.getVertexLabel();
					
				} if (element instanceof EntityConnectionData) {
					EntityConnectionData test = (EntityConnectionData) element;
					Vertex startVertex = (Vertex) test.source;
					Vertex endVertex = (Vertex) test.dest;
					
					return "" + startVertex.getVertexLabel() + "_" + endVertex.getVertexLabel();
				}
				
				return null;
			}
		
		});
		
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
		Vertex vertex = (Vertex) data;
		List<Column> columnList = vertex.getColumnList();
		
		gdbTable.setInput(columnList);
		rdbTable.setInput(columnList);
		
		gdbTable.refresh();
		rdbTable.refresh();
		
	}
	
	public void createTableView(Composite parent) {
		SashForm sashContainer = new SashForm(parent, SWT.HORIZONTAL);
		sashContainer.setLayout(new FillLayout());
		sashContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		Group leftSash = new Group(sashContainer, SWT.NONE);
		leftSash.setLayout(new FillLayout());
		leftSash.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		leftSash.setText("RDB column");
		
		Group rightSash = new Group(sashContainer, SWT.NONE);
		rightSash.setLayout(new FillLayout());
		rightSash.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		rightSash.setText("GDB column");
		
		rdbTable = new TableViewer(leftSash, SWT.NONE);
		rdbTable.setContentProvider(new IStructuredContentProvider() {
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
		
		rdbTable.setLabelProvider(new ITableLabelProvider() {
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
					
				default :
					return null;
				}
			}
			
			@Override
			public Image getColumnImage(Object element, int columnIndex) {
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
		
		TableLayout tableLayout1 = new TableLayout();
		
		tableLayout1.addColumnData(new ColumnWeightData(10, true));
		tableLayout1.addColumnData(new ColumnWeightData(45, true));
		tableLayout1.addColumnData(new ColumnWeightData(45, true));
		
		rdbTable.getTable().setLayout(tableLayout1);
		rdbTable.getTable().setLinesVisible(true);
		rdbTable.getTable().setHeaderVisible(true);
		
		TableColumn rdbColumn1 = new TableColumn(rdbTable.getTable(), SWT.LEFT);
		TableColumn rdbColumn2 = new TableColumn(rdbTable.getTable(), SWT.LEFT);
		TableColumn rdbColumn3 = new TableColumn(rdbTable.getTable(), SWT.LEFT);
		
		rdbColumn2.setText("Column Name");
		rdbColumn3.setText("Data Type");
		
		
		gdbTable = new TableViewer(rightSash, SWT.NONE);
		gdbTable.setContentProvider(new IStructuredContentProvider() {
			
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
		
		gdbTable.setLabelProvider(new ITableLabelProvider() {
			
			@Override
			public String getColumnText(Object element, int columnIndex) {
				// TODO Auto-generated method stub
				
				Column column = (Column) element;
				
				switch (columnIndex) {
				case 0:
					return column.getName();
				case 1:
				    return column.getGraphDataType();
				default:
					return null;
				}
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
		
		TableLayout tableLayout2 = new TableLayout();
		
		tableLayout2.addColumnData(new ColumnWeightData(50, true));
		tableLayout2.addColumnData(new ColumnWeightData(50, true));
		
		gdbTable.getTable().setLayout(tableLayout2);
		gdbTable.getTable().setLinesVisible(true);
		gdbTable.getTable().setHeaderVisible(true);
		
		TableColumn gdbColumn1 = new TableColumn(gdbTable.getTable(), SWT.LEFT);
		TableColumn gdbColumn2 = new TableColumn(gdbTable.getTable(), SWT.LEFT);
		
		gdbColumn1.setText("Property Name");
		gdbColumn2.setText("GDB Types");
		
		
		/*
		Group groupContainer = new Group(parent, SWT.NONE);
		groupContainer.setLayout(new FillLayout());
		groupContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		groupContainer.setText("sash 2");
		
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
		*/
	}
	
	public void showGraphData(List<Vertex> vertexList) {
		graphViewer.setInput(vertexList);
	}
	
	//GDB GraphMappingPage -> afterShowCurrentPage
	protected void afterShowCurrentPage(PageChangedEvent event) {
		final MigrationWizard mw = getMigrationWizard();
		MigrationConfiguration cfg = mw.getMigrationConfig();
		setTitle(mw.getStepNoMsg(GraphMappingPage.this) + Messages.objectMapPageTitle);
		setDescription(Messages.objectMapPageDescription);
		
		setErrorMessage(null);
		
		Catalog sourceCatalog = mw.getSourceCatalog();
	
		GraphDictionary gdbDict = cfg.getGraphDictionary();
		
		gdbDict.printVertexAndEdge();
		
		
//		showTableInformationForGdbms(sourceCatalog);
		
		showGraphData(gdbDict.getMigratedVertexList());
	}
	
//	private void showTableInformationForGdbms(Catalog sourceCatalog) {
//		MigrationWizard mw = getMigrationWizard();
//		
//		GraphDictionary gdbDict = mw.getGraphDictionary();
//		
//		List<Schema> schemas = sourceCatalog.getSchemas();
//
//		List<Table> joinTablesEdgesList = new ArrayList<Table>();
//		List<Table> intermediateVertexesList = new ArrayList<Table>();
//		List<Table> firstVertexesList = new ArrayList<Table>();
//		List<Table> secondVertexesList = new ArrayList<Table>();
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
//					intermediateVertexesList.add(table);
//				} else if (importedKeysCount == 0) {
//					firstVertexesList.add(table);
//				} else if (importedKeysCount > 0 || exportedKeysCount > 0) {
//					secondVertexesList.add(table);
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
//		printTableElement("Intermediate Vertexes", intermediateVertexesList);
//		printTableElement("First Vertexes", firstVertexesList);
//		printTableElement("Second Vertexes", secondVertexesList);
//		printTableElement("Recursive Edges", recursiveEdgesList);
//		
//		migrateFirstVertex(firstVertexesList, gdbDict);
//		migrateSecondVertexes(secondVertexesList, gdbDict);
//		migrateIntermediateVertexes(intermediateVertexesList, gdbDict);
//		migrateJoinTablesEdges(joinTablesEdgesList, gdbDict);
//		migrateRecursiveRelationship(recursiveEdgesList, gdbDict);
//		
//		gdbDict.printVertexAndEdge();
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
//	//GDB first vertex
//	private void migrateFirstVertex(List<Table> firstVertexList, GraphDictionary gdbDict) {
//		for (Table table : firstVertexList) {
//			Vertex vertex = new Vertex();
//			vertex.setVertexLabel(table.getName());
//			
//			gdbDict.setMigratedVertexList(vertex);
//		}
//	}
//	
//	//GDB second vertex
//	private void migrateSecondVertexes(List<Table> secondVertexList, GraphDictionary gdbDict) {
//		for (Table table : secondVertexList) {
//			Vertex startVertex = new Vertex();
//			Edge edge = new Edge();
//			startVertex.setVertexLabel(table.getName());
//			
//			gdbDict.setMigratedVertexList(startVertex);
//			edge.setStartVertexName(startVertex.getVertexLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endVertexName = gdbDict.getMigratedVertexByName(fk.getReferencedTableName()).getVertexLabel();
//				
//				if (endVertexName != null) {
//					edge.setEndVertexName(endVertexName);
//					gdbDict.setMigratedEdgeList(edge);
//					
//				} else {
//					Vertex endVertex = new Vertex();
//					endVertex.setVertexLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedVertexList(endVertex);
//				}
//			}
//		}
//	}
//	
//	//GDB intermediate vertex
//	private void migrateIntermediateVertexes(List<Table> intermediateVertexList, GraphDictionary gdbDict){
//		for (Table table : intermediateVertexList) {
//			Vertex startVertex = new Vertex();
//			Edge edge = new Edge();
//			startVertex.setVertexLabel(table.getName());
//			
//			gdbDict.setMigratedVertexList(startVertex);
//			edge.setStartVertexName(startVertex.getVertexLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endVertexName = gdbDict.getMigratedVertexByName(fk.getReferencedTableName()).getVertexLabel();
//				
//				if (endVertexName != null) {
//					edge.setEndVertexName(endVertexName);
//					gdbDict.setMigratedEdgeList(edge);
//					
//				} else {
//					Vertex endVertex = new Vertex();
//					endVertex.setVertexLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedVertexList(endVertex);
//				}
//			}
//		}
//	}
//	
//	//GDB join table edges
//	private void migrateJoinTablesEdges(List<Table> joinTablesEdges, GraphDictionary gdbDict) {
//		for (Table table : joinTablesEdges) {
//			Vertex startVertex = new Vertex();
//			Edge edge = new Edge();
//			startVertex.setVertexLabel(table.getName());
//			
//			gdbDict.setMigratedVertexList(startVertex);
//			edge.setStartVertexName(startVertex.getVertexLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endVertexName = gdbDict.getMigratedVertexByName(fk.getReferencedTableName()).getVertexLabel();
//				
//				if (endVertexName != null) {
//					edge.setEndVertexName(endVertexName);
//					gdbDict.setMigratedEdgeList(edge);
//					
//				} else {
//					Vertex endVertex = new Vertex();
//					endVertex.setVertexLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedVertexList(endVertex);
//				}
//			}
//		}
//	}
//	
//	//GDB recursive relationship
//	private void migrateRecursiveRelationship(List<Table> recursiveEdges, GraphDictionary gdbDict){
//		for (Table table : recursiveEdges) {
//			Vertex startVertex = new Vertex();
//			Edge edge = new Edge();
//			startVertex.setVertexLabel(table.getName());
//			
//			gdbDict.setMigratedVertexList(startVertex);
//			edge.setStartVertexName(startVertex.getVertexLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endVertexName = gdbDict.getMigratedVertexByName(fk.getReferencedTableName()).getVertexLabel();
//				
//				if (endVertexName != null) {
//					edge.setEndVertexName(endVertexName);
//					gdbDict.setMigratedEdgeList(edge);
//					
//				} else {
//					Vertex endVertex = new Vertex();
//					endVertex.setVertexLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedVertexList(endVertex);
//				}
//			}
//		}
//	}
}
