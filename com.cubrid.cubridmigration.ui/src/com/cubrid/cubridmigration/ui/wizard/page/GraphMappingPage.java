package com.cubrid.cubridmigration.ui.wizard.page;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.dialogs.PageChangedEvent;
import org.eclipse.jface.dialogs.PageChangingEvent;

import org.eclipse.jface.viewers.CellEditor;
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
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
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
import org.eclipse.zest.core.widgets.GraphConnection;
import org.eclipse.zest.core.widgets.GraphItem;
import org.eclipse.zest.core.widgets.GraphNode;
import org.eclipse.zest.core.widgets.ZestStyles;
import org.eclipse.zest.layouts.LayoutStyles;
import org.eclipse.zest.layouts.algorithms.GridLayoutAlgorithm;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.graph.dbobj.Work;
import com.cubrid.cubridmigration.graph.dbobj.WorkBuffer;
import com.cubrid.cubridmigration.graph.dbobj.WorkController;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizard;
import com.cubrid.cubridmigration.ui.wizard.dialog.GraphEdgeSettingDialog;
import com.cubrid.cubridmigration.ui.wizard.dialog.GraphRenamingDialog;

//GDB override ObjectMappingPage. GraphMappingPage seems to have a similar structure to ObjectMappingPage

enum workTypeEnum {
	WT_DELETE,
	WT_CREATE,
	WT_RENAME
}

public class GraphMappingPage extends MigrationWizardPage {
	public static final Image CHECK_IMAGE = MigrationUIPlugin.getImage("icon/checked.gif");
	public static final Image UNCHECK_IMAGE = MigrationUIPlugin.getImage("icon/unchecked.gif");
	
	private MigrationConfiguration mConfig;
	
	private GraphDictionary gdbDict;
	
	private GraphViewer graphViewer;
	private Graph graph;
	
	private String highlightNodeName = "";
	
	private Vertex selectedVertex;
	private Vertex startVertex;
	private Vertex endVertex;
	
	private Edge selectedEdge;
	
	private Object selectedObject;
	
	private TableViewer gdbTable;
	private TableViewer rdbTable;
	
	private Menu popupMenu;
	Button twoWayBtn;
	
	private String[] columnProperties = {"Property Name", "GDB Types"};
	
	private WorkBuffer workBuffer = new WorkBuffer();
	private WorkController workCtrl = new WorkController();
	
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
					deleteMenuHandler(true);
					redoUndoHandler();
					System.out.println("select object: " + ((Vertex) selectedObject).getVertexLabel());
				}
				
				if (selection.getFirstElement() instanceof Edge) {
					selectedObject = (Edge) selection.getFirstElement();
					selectedEdge = (Edge) selection.getFirstElement();
					
					menuHandler();
					deleteMenuHandler(false);
					redoUndoHandler();
					System.out.println("selected object: " + ((Edge) selectedObject).getEdgeLabel());
				}
				
				graphViewer.refresh();
			}
		});
		
		graph = graphViewer.getGraphControl();
		
		graph.addSelectionListener(new SelectionListener() {
			
			@Override
			@SuppressWarnings("unchecked")
			public void widgetSelected(SelectionEvent e) {
				ArrayList<GraphItem> selectList = (ArrayList<GraphItem>) graph.getSelection();
				for (GraphItem selection : selectList) {
					if (selection instanceof GraphConnection) {
						GraphConnection conntion = (GraphConnection) selection;
						//conntion.unhighlight();
					} else if (selection instanceof GraphNode) {
						GraphNode gNode = (GraphNode) selection;
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
	
	@SuppressWarnings("unused")
	public void setPopupMenu(Composite parent) {
		popupMenu = new Menu(parent);
		
		//TODO setting message
		MenuItem item1 = new MenuItem(popupMenu, SWT.POP_UP);
		item1.setText("Select As Start Vertex");
		
		item1.addSelectionListener(new SelectionListener() {
			
			@SuppressWarnings("unchecked")
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
		item2.setText("Select As End Vertex");
		
		item2.addSelectionListener(new SelectionListener() {
			
			@Override
			public void widgetSelected(SelectionEvent e) {
				
				if (endVertex != null) {
					endVertex = null;
				}
				
				endVertex = selectedVertex;
				
				GraphEdgeSettingDialog edgeSettingDialog = new GraphEdgeSettingDialog(getShell(), 
						mConfig, gdbDict, startVertex, endVertex, workBuffer, workCtrl);
				edgeSettingDialog.open();
				
				redoUndoHandler();
				
				graphViewer.refresh();
			}
			
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		MenuItem item3 = new MenuItem(popupMenu, SWT.POP_UP);
		item3.setText("Cancel");
		
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
		changeName.setText("Change Name");
		
		changeName.addSelectionListener(new SelectionListener() {
			
			@Override
			public void widgetSelected(SelectionEvent e) {
				
				GraphRenamingDialog renameDialog = new GraphRenamingDialog(getShell(), gdbDict, selectedObject);
				renameDialog.open();
				
				graphViewer.refresh();
			}
			
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		MenuItem deleteEdge = new MenuItem(popupMenu, SWT.POP_UP);
		deleteEdge.setText("Delete Edge");
		
		deleteEdge.addSelectionListener(new SelectionListener() {

			@Override
			public void widgetSelected(SelectionEvent e) {
				
				GraphDictionary gdbDict = mConfig.getGraphDictionary();
				List<Edge> migratedEdgeList = gdbDict.getMigratedEdgeList();
				
				for (Edge edge : migratedEdgeList) {
					if (edge.getEdgeLabel().equalsIgnoreCase(selectedEdge.getEdgeLabel())) {
						
						workBuffer.addWork(workCtrl.createWork(workTypeEnum.WT_DELETE.ordinal(), edge));
						
						gdbDict.removeEdge(edge.getEdgeLabel());
						
						break;
					}
				}
				
				graphViewer.refresh();
			}

			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		MenuItem separator2 = new MenuItem(popupMenu, SWT.SEPARATOR);
		
		MenuItem undo = new MenuItem(popupMenu, SWT.POP_UP);
		undo.setText("Undo");
		
		undo.addSelectionListener(new SelectionListener() {
			@Override
			public void widgetSelected(SelectionEvent e) {
				executeUndo(workBuffer.undo());
				redoUndoHandler();
				
				graphViewer.refresh();
			}
			
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		MenuItem redo = new MenuItem(popupMenu, SWT.POP_UP);
		redo.setText("Redo");
		
		redo.addSelectionListener(new SelectionListener() {
			@Override
			public void widgetSelected(SelectionEvent e) {
				executeRedo(workBuffer.redo());
				redoUndoHandler();
				
				graphViewer.refresh();
			}
			
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {}
		});
		
		item1.setEnabled(true);
		item2.setEnabled(false);
		item3.setEnabled(false);

		changeName.setEnabled(true);
		deleteEdge.setEnabled(true);
		redo.setEnabled(false);
		undo.setEnabled(false);
	}
	
	public void setHighlight(GraphNode node) {
		Display dis = graphViewer.getGraphControl().getDisplay();
		
		node.unhighlight();
		node.setHighlightColor(new Color(dis, 153, 204, 102));
		node.setNodeStyle(GraphNode.HIGHLIGHT_ON);
		node.highlight();
	}
	
	public void deleteMenuHandler(boolean isVertex) {
		MenuItem[] items = popupMenu.getItems();
		
		if (isVertex) {
			items[5].setEnabled(false);
		} else {
			items[5].setEnabled(true);
		}
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
	
	public void redoUndoHandler() {
		if (workBuffer.isUndoListEmpty()) {
			popupMenu.getItem(7).setEnabled(false);
		} else {
			popupMenu.getItem(7).setEnabled(true);
		}
		
		if (workBuffer.isRedoListEmpty()) {
			popupMenu.getItem(8).setEnabled(false);
		} else {
			popupMenu.getItem(8).setEnabled(true);
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
		
		SashForm verticalSash = new SashForm(parent, SWT.VERTICAL);
		verticalSash.setLayout(new GridLayout(4, false));
		verticalSash.setLayoutData(new GridData(SWT.FILL, SWT.FILL, false, false));
		
		SashForm sashContainer = new SashForm(verticalSash, SWT.HORIZONTAL);
		sashContainer.setLayout(new FillLayout());
		sashContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		SashForm btnContiainer = new SashForm(verticalSash, SWT.VERTICAL);
		sashContainer.setLayout(new FillLayout());
		sashContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, false, false));
		
		twoWayBtn = new Button(btnContiainer, SWT.CHECK);
		twoWayBtn.setText("set two-way edge");
		
		verticalSash.setWeights(new int[] {15, 1});
		verticalSash.setSashWidth(1);
		
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
		
		CellEditor[] editors = new CellEditor[] {
				null,
				null,
		};
		
		gdbTable.setCellEditors(editors);
		
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
		
		gdbTable.setColumnProperties(columnProperties);
		
		gdbColumn1.setText(columnProperties[0]);
		gdbColumn2.setText(columnProperties[1]);
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
	
	//GDB GraphMappingPage -> afterShowCurrentPage
	protected void afterShowCurrentPage(PageChangedEvent event) {
		final MigrationWizard mw = getMigrationWizard();
		mConfig = mw.getMigrationConfig();
		setTitle(mw.getStepNoMsg(GraphMappingPage.this) + Messages.objectMapPageTitle);
		setDescription(Messages.objectMapPageDescription);
		
		setErrorMessage(null);
		
		gdbDict = mConfig.getGraphDictionary();
		
		gdbDict.printVertexAndEdge();
		
		showGraphData(gdbDict.getMigratedVertexList());
	}
	
	protected void handlePageLeaving(PageChangingEvent event) {
		if (!isPageComplete()) {
			return;
		}
		
		if (twoWayBtn.getSelection()) {
			event.doit = setTwoWayEdge();
		}
		
		gdbDict.setVertexAndEdge();
	}
	
	private boolean setTwoWayEdge() {
		List<Edge> edgeList = gdbDict.getMigratedEdgeList();
		List<Edge> twoWayEdgeList = new ArrayList<Edge>();
		
		for (Edge edge : edgeList) {
			int edgeType = Edge.TWO_WAY_TYPE;
			
			if (edge.getEdgeType() == Edge.RECURSIVE_TYPE)
				continue;
			
			if (edge.getEdgeType() == Edge.JOINTABLE_TYPE)
				edgeType = Edge.JOIN_TWO_WAY_TYPE;
			
			Edge copiedEdge = new Edge(edge);
			copiedEdge.setEdgeType(edgeType);
			copiedEdge.removeIDCol();
			
			if (mConfig.targetIsCSV()) {
				Column twoWayStartCol = new Column(":END_ID(" + copiedEdge.getEndVertexName() + ")");
				twoWayStartCol.setDataType("ID");
				
				Column twoWayEndCol = new Column(":START_ID(" + copiedEdge.getStartVertexName() + ")");
				twoWayEndCol.setDataType("ID");
				
				copiedEdge.addColumnAtFirst(twoWayEndCol);
				copiedEdge.addColumnAtFirst(twoWayStartCol);
			}
			
			twoWayEdgeList.add(copiedEdge);
		}
		
		gdbDict.addMigratedEdgeList(twoWayEdgeList);

		return true;
	}

	private void executeUndo(Work work) {
		workCtrl.setWork(work);
		
		if (work.getWorkType() == workTypeEnum.WT_DELETE.ordinal()) {
			gdbDict.addMigratedEdgeList(work.getEdge());
		} else if (work.getWorkType() == workTypeEnum.WT_CREATE.ordinal()) {
			gdbDict.removeEdge(workCtrl.getEdge().getEdgeLabel());
		}
		graphViewer.refresh();
	}
	
	private void executeRedo(Work work) {
		workCtrl.setWork(work);
		
		if (work.getWorkType() == workTypeEnum.WT_DELETE.ordinal()) {
			gdbDict.removeEdge(workCtrl.getEdge().getEdgeLabel());
		} else if (work.getWorkType() == workTypeEnum.WT_CREATE.ordinal()) {
			gdbDict.addMigratedEdgeList(work.getEdge());
		}
		graphViewer.refresh();
	}
}
