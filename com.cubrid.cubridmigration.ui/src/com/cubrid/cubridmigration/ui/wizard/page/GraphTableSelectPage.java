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
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
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
		groupContainer1.setText(Messages.msgTableList);
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
		groupContainer2.setText(Messages.msgColumnList);
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
		if (isFirstVisible) {
			final MigrationWizard mw = getMigrationWizard();
			setTitle(mw.getStepNoMsg(GraphTableSelectPage.this) + Messages.objectMapPageTitle);
			setDescription(Messages.objectMapPageDescription);
			
			setErrorMessage(null);
            mw.refreshWizardStatus();
			
			Catalog sourceCatalog = mw.getSourceCatalog();
			
			List<Schema> schemaList = sourceCatalog.getSchemas();
			
			clearData();
	//		showTableInformationForGdbms(schemaList);
			showTableViewerData(schemaList);
			
			makeColumnViewerData(schemaList);
			
			isFirstVisible = false;
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
			MessageDialog.openError(getShell(), "No table selected", Messages.errNoTableSelected);
			return false;
		} else {
			showTableInformationForGdbms(selectedTableList);
		}
		
		return true;
	}
	
	private void showTableInformationForGdbms(List<Table> tables) {
		MigrationWizard mw = getMigrationWizard();
		MigrationConfiguration cfg = mw.getMigrationConfig();
		
		GraphDictionary gdbDict = cfg.getGraphDictionary();
		gdbDict.clean();
		
		List<Table> joinTablesEdgesList = new ArrayList<Table>();
		List<Table> intermediateVertexesList = new ArrayList<Table>();
		List<Table> firstVertexesList = new ArrayList<Table>();
		List<Table> secondVertexesList = new ArrayList<Table>();
		List<Table> recursiveEdgesList = new ArrayList<Table>();

		for (Table table : tables) {

			int importedKeysCount = table.getImportedKeysCount();
			int exportedKeysCount = table.getExportedKeysCount();

			if (importedKeysCount == 2 && exportedKeysCount == 0) {
				joinTablesEdgesList.add(table);
			} else if (importedKeysCount >= 3) {
				intermediateVertexesList.add(table);
			} else if (importedKeysCount == 0) {
				firstVertexesList.add(table);
			} else if (!isRecursive(table) && (importedKeysCount > 0 || exportedKeysCount > 0)) {
				secondVertexesList.add(table);
			} else if (isRecursive(table)) {
				recursiveEdgesList.add(table);
			}
		}

		printTableElement("JoinTables Edges", joinTablesEdgesList);
		printTableElement("Intermediate Vertexes", intermediateVertexesList);
		printTableElement("First Vertexes", firstVertexesList);
		printTableElement("Second Vertexes", secondVertexesList);
		printTableElement("Recursive Edges", recursiveEdgesList);
		
		migrateFirstVertex(firstVertexesList, gdbDict);
		migrateSecondVertexes(secondVertexesList, gdbDict);
		migrateRecursiveRelationship(recursiveEdgesList, gdbDict);
		migrateIntermediateVertexes(intermediateVertexesList, gdbDict);
		migrateJoinTablesEdges(joinTablesEdgesList, gdbDict);
		
		gdbDict.setVertexAndEdge();
		
//		gdbDict.printVertexAndEdge();
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
	
	//GDB first vertex
	private void migrateFirstVertex(List<Table> firstVertexList, GraphDictionary gdbDict) {
		for (Table table : firstVertexList) {
			Vertex vertex = new Vertex();
			vertex.setOwner(table.getOwner());
			vertex.setVertexLabel(table.getName());
			vertex.setColumnList(table.getColumns());
			vertex.setVertexType(Vertex.FIRST_TYPE);
			vertex.setHasPK(table.hasPK());
			
			gdbDict.addMigratedVertexList(vertex);
		}
	}
	
	//GDB second vertex
	private void migrateSecondVertexes(List<Table> secondVertexList, GraphDictionary gdbDict) {
		for (Table table : secondVertexList) {
			Vertex startVertex = new Vertex();
			startVertex.setOwner(table.getOwner());
			startVertex.setVertexLabel(table.getName());
			startVertex.setColumnList(table.getColumns());
			startVertex.setVertexType(Vertex.SECOND_TYPE);
			startVertex.setHasPK(table.hasPK());
			Edge edge = new Edge();
			edge.setEdgeType(Edge.SECOND_FK_TYPE);
			gdbDict.addMigratedVertexList(startVertex);
			
			for (FK fk : table.getFks()) {
				edge = new Edge();
				edge.setEdgeLabel(fk.getName());
				edge.setFKSring(fk.getFKString());

                for (String columName : fk.getColumnNames()) {
					edge.addFKColumnList(columName);
				}
                
				String endVertexName = null;
				Vertex endVertex = gdbDict.getMigratedVertexByName(fk.getReferencedTableName());
				
				if (endVertex != null) {
					endVertexName = endVertex.getVertexLabel();
				}
				
				if (endVertexName == null) {
					for (Table selectedTable : selectedTableList) {
						if (selectedTable.getName().equals(fk.getReferencedTableName())) {
							Vertex migratedVertex = new Vertex();
							migratedVertex.setVertexLabel(fk.getReferencedTableName());
							migratedVertex.setColumnList(selectedTable.getColumns());
							
							gdbDict.addMigratedVertexList(migratedVertex);
						}
					}
					
				} else {
					edge.setEndVertexName(endVertexName);
				}
				
				if (!edge.getEndVertexName().isEmpty()) {
				    edge.setOwner(startVertex.getOwner());
					edge.setStartVertexName(startVertex.getVertexLabel());
					gdbDict.addMigratedEdgeList(edge);
				}
			}
			
		}
	}
	
	//GDB intermediate vertex
	private void migrateIntermediateVertexes(List<Table> intermediateVertexList, GraphDictionary gdbDict){
		for (Table table : intermediateVertexList) {
			Vertex startVertex = new Vertex();
			startVertex.setOwner(table.getOwner());
			startVertex.setVertexLabel(table.getName());
			startVertex.setColumnList(table.getColumns());
			startVertex.setVertexType(Vertex.INTERMEDIATE_TYPE);
			startVertex.setHasPK(table.hasPK());

			gdbDict.addMigratedVertexList(startVertex);
			
			Edge edge;
			for (FK fk : table.getFks()) {
				edge = new Edge();
				edge.setEdgeType(Edge.INTERMEDIATE_FK_TYPE);
				edge.setEdgeLabel(fk.getName());
				edge.setFKSring(fk.getFKString());
				String endVertexName = null;
				Vertex endVertex = gdbDict.getMigratedVertexByName(fk.getReferencedTableName());
				
				if (endVertex != null) {
					endVertexName = endVertex.getVertexLabel();
				}
				
				if (endVertexName == null) {
					for (Table selectedTable : selectedTableList) {
						if (selectedTable.getName().equals(fk.getReferencedTableName())) {
							Vertex migratedVertex = new Vertex();
							migratedVertex.setOwner(table.getOwner());
							migratedVertex.setVertexLabel(fk.getReferencedTableName());
							migratedVertex.setColumnList(selectedTable.getColumns());
							
							gdbDict.addMigratedVertexList(migratedVertex);
						}
					}
					
				} else {
					edge.setEndVertexName(endVertexName);
				}
				
				if (!edge.getEndVertexName().isEmpty()) {
                    edge.setOwner(startVertex.getOwner());
					edge.setStartVertexName(startVertex.getVertexLabel());
					gdbDict.addMigratedEdgeList(edge);
				}
			}
			
//			Vertex startVertex = new Vertex();
//			Edge edge = new Edge();
//			startVertex.setVertexLabel(table.getName());
//			
//			gdbDict.setMigratedVertexList(startVertex);
//			edge.setStartVertexName(startVertex.getVertexLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endVertexName;
//				Vertex endVertex = gdbDict.getMigratedVertexByName(fk.getReferencedTableName());
//				if (endVertex == null) {
//					endVertexName = null;
//				} else {
//					endVertexName = endVertex.getVertexLabel();
//				}
//				
//				if (endVertexName != null) {
//					edge.setEndVertexName(endVertexName);
//					
//				} else {
//					Vertex migratedVertex = new Vertex();
//					migratedVertex.setVertexLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedVertexList(migratedVertex);
//				}
//			}
//			gdbDict.setMigratedEdgeList(edge);
		}
	}
	
	//GDB join table edges
	//GDB if start vertex or end vertex is null?
	private void migrateJoinTablesEdges(List<Table> joinTablesEdges, GraphDictionary gdbDict) {
		for (Table table : joinTablesEdges) {
			List<FK> fkList = table.getFks();
			
			Edge edge = new Edge();
			
			FK fk1 = fkList.get(0);
			FK fk2 = fkList.get(1);
			
			edge.setOwner(table.getOwner());
			edge.setStartVertexName(fk1.getReferencedTableName());
			edge.setEndVertexName(fk2.getReferencedTableName());
			edge.setColumnList(table.getColumns());
			edge.setEdgeLabel(fk1.getReferencedTableName() + "_" + fk2.getReferencedTableName());
            edge.setEdgeType(Edge.JOINTABLE_TYPE);
			
			gdbDict.addMigratedEdgeList(edge);
		}
	}
	
	//GDB recursive relationship
	private void migrateRecursiveRelationship(List<Table> recursiveEdges, GraphDictionary gdbDict){
		for (Table table : recursiveEdges) {
			Vertex startVertex = new Vertex();
			Edge edge = new Edge();
			
			startVertex.setOwner(table.getOwner());
			startVertex.setVertexLabel(table.getName());
			startVertex.setColumnList(table.getColumns());
			startVertex.setVertexType(Vertex.RECURSIVE_TYPE);
			startVertex.setHasPK(table.hasPK());
			
			gdbDict.addMigratedVertexList(startVertex);
			
			edge.setOwner(table.getOwner());
			edge.setStartVertexName(table.getName());
			edge.setEndVertexName(table.getName());
			edge.setEdgeType(Edge.RECURSIVE_TYPE);
			
			Vertex vertex = gdbDict.getMigratedVertexByName(table.getName());
			
			if (vertex == null) {
				gdbDict.addMigratedVertexList(startVertex);
			}
			
			gdbDict.addMigratedEdgeList(edge);
			
//			Vertex startVertex = new Vertex();
//			Edge edge = new Edge();
//			startVertex.setVertexLabel(table.getName());
//			
//			gdbDict.setMigratedVertexList(startVertex);
//			edge.setStartVertexName(startVertex.getVertexLabel());
//			
//			for (FK fk : table.getFks()) {
//				String endVertexName;
//				Vertex endVertex = gdbDict.getMigratedVertexByName(fk.getReferencedTableName());
//				if (endVertex == null) {
//					endVertexName = null;
//				} else {
//					endVertexName = endVertex.getVertexLabel();
//				}
//				
//				if (endVertexName != null) {
//					edge.setEndVertexName(endVertexName);
//					
//				} else {
//					Vertex migratedVertex = new Vertex();
//					migratedVertex.setVertexLabel(fk.getReferencedTableName());
//					gdbDict.setMigratedVertexList(migratedVertex);
//				}
//			}
//			gdbDict.setMigratedEdgeList(edge);
		}
	}
	
	public void setFirstVisible(boolean isFirstVisible) {
		this.isFirstVisible = isFirstVisible;
	}
	
	private void clearData() {
		tableList.clear();
		columnData.clear();
	}
}
