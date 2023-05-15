package com.cubrid.cubridmigration.ui.wizard.page;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.viewers.ColumnWeightData;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
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
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.zest.core.viewers.EntityConnectionData;
import org.eclipse.zest.core.viewers.GraphViewer;
import org.eclipse.zest.core.viewers.IGraphEntityRelationshipContentProvider;
import org.eclipse.zest.core.widgets.Graph;
import org.eclipse.zest.core.widgets.GraphNode;
import org.eclipse.zest.core.widgets.ZestStyles;
import org.eclipse.zest.core.widgets.internal.ZestRootLayer;
import org.eclipse.zest.layouts.LayoutStyles;
import org.eclipse.zest.layouts.algorithms.GridLayoutAlgorithm;

import com.cubrid.cubridmigration.core.dbobject.Catalog;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;
import com.cubrid.cubridmigration.ui.wizard.dialog.GraphEdgeSettingDialog;
import com.cubrid.cubridmigration.ui.wizard.dialog.GraphRenamingDialog;

//GDB override ObjectMappingPage. GraphMappingPage seems to have a similar structure to ObjectMappingPage
public class GraphMappingPage extends MigrationWizardPage {
	public static final Image CHECK_IMAGE = MigrationUIPlugin.getImage("icon/checked.gif");
	public static final Image UNCHECK_IMAGE = MigrationUIPlugin.getImage("icon/unchecked.gif");
	
	private GraphDictionary gdbDict;
	
	private GraphViewer graphViewer;
	private Graph graph;
	
	private String highlightNodeName = "";
	
	private Vertex selectedVertex;
	private Vertex startVertex;
	private Vertex endVertex;
	
	private Object selectedObject;
	
	private TableViewer columnViewer;
	private TableViewer gdbTable;
	private TableViewer rdbTable;
	
	private Menu popupMenu;
	
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
		setPopupMenu(parent);
	
		graphViewer = new GraphViewer(parent, SWT.BORDER);	
		graphViewer.setConnectionStyle(ZestStyles.CONNECTIONS_DIRECTED);
		graphViewer.setLayoutAlgorithm(new GridLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING));
		
		graphViewer.getGraphControl().setMenu(popupMenu);
		
		graphViewer.setContentProvider(new IGraphEntityRelationshipContentProvider() {
			
			@Override
			public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {}
			
			@Override
			public void dispose() {}
			
			@Override
			public Object[] getElements(Object inputElement) {
				if (inputElement instanceof List) {
					@SuppressWarnings("unchecked")
					List<Vertex> vertexList = (ArrayList<Vertex>) inputElement;
				
					return vertexList.toArray();
				} else {
					return new Object[0];
				}
			}
			
			@Override
			public Object[] getRelationships(Object source, Object dest) {
				Vertex startVertex = (Vertex) source;
				Vertex endVertex = (Vertex) dest;
				
				ArrayList<Edge> allEdgeList = (ArrayList<Edge>) gdbDict.getMigratedEdgeList();
				ArrayList<Edge> currentEdgeList = new ArrayList<Edge>();
				
				
				for (Edge edge : allEdgeList) {
					if (edge.getStartVertexName().equals(startVertex.getVertexLabel())
							&& edge.getEndVertexName().equals(endVertex.getVertexLabel())) {
						currentEdgeList.add(edge);
					}
				}
				
				return currentEdgeList.toArray();
			}
		});
				
		graphViewer.setLabelProvider(new LabelProvider() {
			@Override
			public String getText(Object element) {
				if (element instanceof Vertex) {
					Vertex vertex = (Vertex) element;
					return vertex.getVertexLabel();
					
				} if (element instanceof Edge) {
					Edge edge = (Edge) element;
					
					return edge.getEdgeLabel();
				}
				
				return null;
			}
		});
		
		graphViewer.addSelectionChangedListener(new ISelectionChangedListener() {
			@SuppressWarnings("unchecked")
			@Override
			public void selectionChanged(SelectionChangedEvent event) {
				IStructuredSelection selection = (IStructuredSelection) event.getSelection();
				changeColumnData(selection.getFirstElement());
				
				if(selection.getFirstElement() instanceof Vertex) {
					selectedObject = (Vertex) selection.getFirstElement();
					selectedVertex = (Vertex) selection.getFirstElement();
					
//					for (GraphNode gNode : (ArrayList<GraphNode>) graphViewer.getGraphControl().getNodes()) {
//						if (gNode.getText().equalsIgnoreCase(selectedVertex.getVertexLabel())) {
//							highlightNodeName = gNode.getText();
//						}
//					}
					
					menuHandler();
					System.out.println("select object: " + ((Vertex) selectedObject).getVertexLabel());
				}
				
				if (selection.getFirstElement() instanceof Edge) {
					selectedObject = (Edge) selection.getFirstElement();
					
					menuHandler();
					System.out.println("selected object: " + ((Edge) selectedObject).getEdgeLabel());
				}
				
				graphViewer.refresh();
			}
		});
		
		graph = graphViewer.getGraphControl();
		
		graph.addSelectionListener(new SelectionListener() {
			
			@Override
			public void widgetSelected(SelectionEvent e) {
				ArrayList<GraphNode> selectList = (ArrayList<GraphNode>) graph.getSelection();
				for (GraphNode selection : selectList) {
					GraphNode selectedNode = (GraphNode) selection;
					
//					selectedNode.unhighlight();
					
					for (GraphNode gNode : (ArrayList<GraphNode>) graph.getNodes()) {
						if (gNode.getText().equals(highlightNodeName)) {
							setHighlight(gNode);
						}
					}
				}
			}
			
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		graphViewer.applyLayout();
	}
	
	public void setPopupMenu(Composite parent) {
		popupMenu = new Menu(parent);
		
		//TODO setting message
		MenuItem item1 = new MenuItem(popupMenu, SWT.POP_UP);
		item1.setText("select as start vertex");
		
		item1.addSelectionListener(new SelectionListener() {
			
			@Override
			public void widgetSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				if (startVertex != null) {
					startVertex = null;
				}
				
				startVertex = selectedVertex;
				
				for (GraphNode gNode : (ArrayList<GraphNode>) graphViewer.getGraphControl().getNodes()) {
					if (gNode.getText().equalsIgnoreCase(selectedVertex.getVertexLabel())) {
						highlightNodeName = gNode.getText();
						
						setHighlight(gNode);
						
						break;
					}
				}
			}
			
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		MenuItem item2 = new MenuItem(popupMenu, SWT.POP_UP);
		item2.setText("select as end vertex");
		
		item2.addSelectionListener(new SelectionListener() {
			
			@Override
			public void widgetSelected(SelectionEvent e) {
				
				if (endVertex != null) {
					endVertex = null;
				}
				
				endVertex = selectedVertex;
				
				GraphEdgeSettingDialog edgeSettingDialog = new GraphEdgeSettingDialog(getShell(), gdbDict, startVertex, endVertex);
				edgeSettingDialog.open();
				
				graphViewer.refresh();
				graphViewer.applyLayout();
			}
			
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		MenuItem item3 = new MenuItem(popupMenu, SWT.POP_UP);
		item3.setText("cancel");
		
		item3.addSelectionListener(new SelectionListener() {
			
			@Override
			public void widgetSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				if (startVertex != null) {
					startVertex = null;
				}
				
				if (endVertex != null) {
					endVertex = null;
				}
			}
			
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		MenuItem separator = new MenuItem(popupMenu, SWT.SEPARATOR);
		
		MenuItem changeName = new MenuItem(popupMenu, SWT.POP_UP);
		changeName.setText("change name");
		
		changeName.addSelectionListener(new SelectionListener() {
			
			@Override
			public void widgetSelected(SelectionEvent e) {
				
				GraphRenamingDialog renameDialog = new GraphRenamingDialog(getShell(), gdbDict, selectedObject);
				renameDialog.open();
				
				graphViewer.refresh();
				graphViewer.applyLayout();
			}
			
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		item1.setEnabled(true);
		item2.setEnabled(false);
		item3.setEnabled(false);
		changeName.setEnabled(true);
	}
	
	public void setHighlight(GraphNode node) {
		Display dis = graphViewer.getGraphControl().getDisplay();
		
		node.unhighlight();
		node.setHighlightColor(new Color(dis, 153, 204, 102));
		node.setNodeStyle(GraphNode.HIGHLIGHT_ON);
		node.highlight();
	}
	
	public void menuHandler() {
		MenuItem[] items = popupMenu.getItems();
		
		if (selectedVertex != null) {
			items[0].setEnabled(true);
			items[1].setEnabled(false);
			items[2].setEnabled(false);
			
		} else {
			items[0].setEnabled(false);
			items[1].setEnabled(false);
			items[2].setEnabled(false);
		}
		
		if (startVertex != null) {
			popupMenu.getItem(0).setEnabled(true);
			popupMenu.getItem(1).setEnabled(true);
			popupMenu.getItem(2).setEnabled(true);
		}
		
		if (endVertex != null) {
			//do nothing?
		}
	}
	
	public void changeColumnData(Object data) {
		if (data == null) {
			return;
		}
		
		List<Column> columnList = null;;
		
		if (data instanceof Vertex) {
			Vertex vertex = (Vertex) data;
			columnList = vertex.getColumnList();
			
		} else if (data instanceof EntityConnectionData) {
			EntityConnectionData connData = (EntityConnectionData) data;
			
		} else if (data instanceof Edge) {
			Edge edge = (Edge) data;
			columnList = edge.getColumnList();
		}
		
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
		
		rdbTable = new TableViewer(leftSash, SWT.FULL_SELECTION);
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
				Column column = (Column) element;
				
				if (columnIndex == 0) {
					if (column.isSelected()) {
						return CHECK_IMAGE;
					} else {
						return UNCHECK_IMAGE;
					}
				}
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
		
		rdbTable.addSelectionChangedListener(new ISelectionChangedListener() {
			@Override
			public void selectionChanged(SelectionChangedEvent event) {
				IStructuredSelection selection = (IStructuredSelection) event.getSelection();
				if (selection.isEmpty()) {
					return;
				}
				changeColumnSelect(selection.getFirstElement());
			}
		});
		
		gdbTable = new TableViewer(rightSash, SWT.FULL_SELECTION);
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
	
	public void changeColumnSelect(Object selectedColumn) {
		Column column = (Column) selectedColumn;
		if (column.isSelected()) {
			column.setSelected(false);
		} else {
			column.setSelected(true);
		}
		
		rdbTable.refresh();
	}
	
	public void showGraphData(List<Vertex> vertexList) {
		graphViewer.setInput(vertexList);
	}
	
	public void createEdgeView() {
		
	}
	
	//GDB GraphMappingPage -> afterShowCurrentPage
	protected void afterShowCurrentPage(PageChangedEvent event) {
		final MigrationWizard mw = getMigrationWizard();
		MigrationConfiguration cfg = mw.getMigrationConfig();
		setTitle(mw.getStepNoMsg(GraphMappingPage.this) + Messages.objectMapPageTitle);
		setDescription(Messages.objectMapPageDescription);
		
		setErrorMessage(null);
		
		Catalog sourceCatalog = mw.getSourceCatalog();
	
		gdbDict = cfg.getGraphDictionary();
		
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
