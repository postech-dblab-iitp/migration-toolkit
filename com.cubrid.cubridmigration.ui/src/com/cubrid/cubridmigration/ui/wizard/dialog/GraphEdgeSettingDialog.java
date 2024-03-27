package com.cubrid.cubridmigration.ui.wizard.dialog;


import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ColumnWeightData;
import org.eclipse.jface.viewers.ComboBoxCellEditor;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.TableLayout;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.graph.dbobj.WorkBuffer;
import com.cubrid.cubridmigration.graph.dbobj.WorkController;

public class GraphEdgeSettingDialog extends Dialog {
	
	private MigrationConfiguration mConfig;
	private GraphDictionary gdbDict;
	
	private Vertex startVertex;
	private Vertex endVertex;
	
	private	TableViewer edgeTable;
	
	private Text txtEdgeName;
	
	private ArrayList<ColumnData> columnDataList = new ArrayList<ColumnData>();
	
	private WorkController workCtrl;
	private WorkBuffer workBuffer;

	private class ColumnData {
		private String startColumnName;
		private String startColumnType;
		private String endColumnName;
		private String endColumnType;
		
		public ColumnData() {
			this.startColumnName = "";
			this.startColumnType = "";
			this.endColumnName = "";
			this.endColumnType = "";
		}
		
		public String getStartColumnName() {
			return startColumnName;
		}
		public void setStartColumnName(String startColumnName) {
			this.startColumnName = startColumnName;
		}
		public String getStartColumnType() {
			return startColumnType;
		}
		public void setStartColumnType(String startColumnType) {
			this.startColumnType = startColumnType;
		}
		public String getEndColumnName() {
			return endColumnName;
		}
		public void setEndColumnName(String endColumnName) {
			this.endColumnName = endColumnName;
		}
		public String getEndColumnType() {
			return endColumnType;
		}
		public void setEndColumnType(String endColumnType) {
			this.endColumnType = endColumnType;
		}
		
	}
	
	private String[] startVertexColumnList = null;
	
	private String[] endVertexColumnList = null;
	
	private String[] propertyList = {
			"Start Vertex Column",
			"Column Type",
			"End Vertex Column",
			"Column Type"
	};

	public GraphEdgeSettingDialog(Shell parentShell, 
			MigrationConfiguration config, 
			GraphDictionary gdbDict, 
			Vertex startVertex, 
			Vertex endVertex,
			WorkBuffer workBuffer,
			WorkController workController) {
		super(parentShell);
		this.mConfig = config;
		this.gdbDict = gdbDict;
		this.startVertex = startVertex;
		this.endVertex = endVertex;
		this.workBuffer = workBuffer;
		this.workCtrl = workController;
	}
	
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setMinimumSize(700, 500);
		getShell().setText("Create New Edge");
	}
	
	@Override
	protected Control createDialogArea(Composite parent) {
		// TODO Auto-generated method stub
		Composite composite = new Composite(parent, SWT.NONE);
		composite.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		composite.setLayout(new GridLayout(1, true));
//		
		Group labelContainer = new Group(composite, SWT.NONE);
		labelContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, false, false));
		labelContainer.setLayout(new GridLayout(1, true));
//
		Composite startVertexContainer = new Composite(labelContainer, SWT.NONE);
		startVertexContainer.setLayout(new GridLayout(2, true));
		startVertexContainer.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, false, false));
		
		Label lblStartVertex = new Label(startVertexContainer, SWT.NONE);
		lblStartVertex.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, false, false));
		lblStartVertex.setText("start vertex: ");
//		
		Text lblStartVertexName = new Text(startVertexContainer, SWT.SINGLE | SWT.READ_ONLY);
		lblStartVertex.setLayoutData(new GridData(SWT.FILL, SWT.FILL, false, false));
		lblStartVertexName.setText(startVertex.getVertexLabel());
//
		Composite endVertexContainer = new Composite(labelContainer, SWT.NONE);
		endVertexContainer.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, false, false));
		endVertexContainer.setLayout(new GridLayout(2, true));
//
		Label lblEndVertex = new Label(endVertexContainer, SWT.NONE);
		lblEndVertex.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, false, false));
		lblEndVertex.setText("end vertex: ");
		
		Text lblEndVertexName = new Text(endVertexContainer, SWT.SINGLE | SWT.READ_ONLY);
		lblEndVertexName.setLayoutData(new GridData(SWT.FILL, SWT.FILL, false, false));
		lblEndVertexName.setText(endVertex.getVertexLabel());
		
		Composite edgeNameContainer = new Composite(labelContainer, SWT.NONE);
		edgeNameContainer.setLayout(new GridLayout(2, true));
		edgeNameContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		
		Label lblEdgeName = new Label(edgeNameContainer, SWT.NONE);
		lblEdgeName.setLayoutData(new GridData(SWT.RIGHT, SWT.FILL, true, false));
		lblEdgeName.setText("set edge name: ");
		
		txtEdgeName = new Text(edgeNameContainer, SWT.BORDER);
		txtEdgeName.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		txtEdgeName.setText("");
		
		Group tableContainer = new Group(composite, SWT.NONE);
		tableContainer.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		tableContainer.setLayout(new FillLayout());
		
		setEdgeTable(tableContainer);
		
		return parent;
	}
	
	public void setEdgeTable(Group tableContainer) {
		edgeTable = new TableViewer(tableContainer, SWT.FULL_SELECTION);
		
		edgeTable.setContentProvider(new IStructuredContentProvider() {
			
			@Override
			public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {}
			
			@Override
			public void dispose() {}
			
			@Override
			public Object[] getElements(Object inputElement) {
				// TODO Auto-generated method stub
				
				if (inputElement instanceof List) {
					
					@SuppressWarnings("unchecked")
					ArrayList<ColumnData> obj = (ArrayList<ColumnData>) inputElement;
					
					return obj.toArray();
				} else {
					return new Object[0];
				}
			}
		});
		
		edgeTable.setLabelProvider(new ITableLabelProvider() {
			
			@Override
			public String getColumnText(Object element, int columnIndex) {
				ColumnData column = (ColumnData) element;
				
				switch (columnIndex) {
				case 0:
					return column.getStartColumnName();
				case 1:
					return column.getStartColumnType();
				case 2:
					return column.getEndColumnName();
				case 3:
					return column.getEndColumnType();
				default:
					return null;
				}
			}
			
			@Override
			public void removeListener(ILabelProviderListener listener) {}
			
			@Override
			public boolean isLabelProperty(Object element, String property) {return false;}
			
			@Override
			public void dispose() {}
			
			@Override
			public void addListener(ILabelProviderListener listener) {}
			
			@Override
			public Image getColumnImage(Object element, int columnIndex) {return null;}
		});

		edgeTable.setColumnProperties(propertyList);
		
		TableLayout tableLayout1 = new TableLayout();
		
		tableLayout1.addColumnData(new ColumnWeightData(1000));
		tableLayout1.addColumnData(new ColumnWeightData(1000));
		tableLayout1.addColumnData(new ColumnWeightData(1000));
		tableLayout1.addColumnData(new ColumnWeightData(1000));
		
		edgeTable.getTable().setLayout(tableLayout1);
		edgeTable.getTable().setLinesVisible(true);
		edgeTable.getTable().setHeaderVisible(true);
  		
		TableColumn edgeColumn1 = new TableColumn(edgeTable.getTable(), SWT.LEFT);
		TableColumn edgeColumn2 = new TableColumn(edgeTable.getTable(), SWT.LEFT);
		TableColumn edgeColumn3 = new TableColumn(edgeTable.getTable(), SWT.LEFT);
		TableColumn edgeColumn4 = new TableColumn(edgeTable.getTable(), SWT.LEFT);
		
		edgeColumn1.setWidth(700);
		
		edgeColumn1.setText(propertyList[0]);
		edgeColumn2.setText(propertyList[1]);
		edgeColumn3.setText(propertyList[2]);
		edgeColumn4.setText(propertyList[3]);
		
		makeData();
		
		ComboBoxCellEditor startVertexComboBox = new ComboBoxCellEditor(edgeTable.getTable(), startVertexColumnList);
		ComboBoxCellEditor endVertexComboBox = new ComboBoxCellEditor(edgeTable.getTable(), endVertexColumnList);
		
		CellEditor[] editors = new CellEditor[] {
				startVertexComboBox,
				null,
				endVertexComboBox,
				null
		};
		
		edgeTable.setCellEditors(editors);
		edgeTable.setCellModifier(new ICellModifier() {
			
			@Override
			public void modify(Object element, String property, Object value) {
				// TODO Auto-generated method stub
				TableItem tabItem = (TableItem) element;
				ColumnData colData = (ColumnData) tabItem.getData();
				
				if (property.equals(propertyList[0])) {
					String columnName = getStartVertexColumn((Integer) value);
					
					colData.setStartColumnName(columnName);
					colData.setStartColumnType(getStartVertexType(columnName));
				}
				
				if (property.equals(propertyList[2])) {
					String ColumnName = getEndVertexColumn((Integer) value);
					
					colData.setEndColumnName(getEndVertexColumn((Integer) value));
					colData.setEndColumnType(getEndVertexType(ColumnName));
				}
				
				
				if (!(colData.getStartColumnType().equals(colData.getEndColumnType()))
						&& !(colData.getEndColumnType().equals("") || colData.getStartColumnType().equals(""))) {
					MessageDialog.openError(getShell(), "warning", "both of column type must be same");
				}
				
				edgeTable.refresh();
			}
			
			@Override
			public Object getValue(Object element, String property) {
				// TODO Auto-generated method stub
				if (property.equals(propertyList[0])) {
					
					ColumnData colData = (ColumnData) element;
					
					return getStartVertexColumn(colData);
				}
				
				if (property.equals(propertyList[2])) {
					
					ColumnData colData = (ColumnData) element;
					
					return getEndVertexColumn(colData);
				}
				
				return null;
			}
			
			@Override
			public boolean canModify(Object element, String property) {
				// TODO Auto-generated method stub
				if (property.equals(propertyList[0]) || property.equals(propertyList[2])) {
					return true;
				}
				
				else {
					return false;					
				}
			}
			
			public int getStartVertexColumn(ColumnData element) {
				for (int i = 0; i < startVertexColumnList.length; i++) {
					if (startVertexColumnList[i].equals(element.getStartColumnName())) {
						return i;
					}
				}
				
				return 0;
			}
			
			public int getEndVertexColumn(ColumnData element) {
				for (int i = 0; i < endVertexColumnList.length; i++) {
					if (endVertexColumnList[i].equals(element.getEndColumnName())) {
						return i;
					}
				}
				
				return 0;
			}
			
			public String getStartVertexColumn(int index) {
				if (index != -1) {
					return startVertexColumnList[index];
				}					
				return null;
			}
			
			public String getEndVertexColumn(int index) {
				if (index != -1) {
					return endVertexColumnList[index];
				}
				return null;
			}
			
			public String getStartVertexType(String columnName) {
				for (Column col : startVertex.getColumnList()) {
					if (col.getName().equals(columnName)) {
						return col.getDataType();
					}
				}
				
				return "";
			}
			
			public String getEndVertexType(String columnName) {
				for (Column col : endVertex.getColumnList()) {
					if (col.getName().equals(columnName)) {
						return col.getDataType();
					}
				}
				
				return "";
			}
		});
	}
	
	
	private void makeData() {
		int columnLength = startVertex.getColumnList().size();
		
		for (int i = 0; i < columnLength; i++ ) {
			columnDataList.add(new ColumnData());
		}
		
		edgeTable.setInput(columnDataList);
		
		
		ArrayList<String> startVertexColumn = new ArrayList<String>();
		ArrayList<String> endVertexColumn = new ArrayList<String>();
		
		startVertexColumn.add("");
		endVertexColumn.add("");
		
		for (Column col : startVertex.getColumnList()) {
			startVertexColumn.add(col.getName());
		}
		
		for (Column col : endVertex.getColumnList()) {
			endVertexColumn.add(col.getName());
		}
		
		startVertexColumnList = (String[]) startVertexColumn.toArray(new String[0]);
		endVertexColumnList = (String[]) endVertexColumn.toArray(new String[0]);
	}
	
	private boolean checkStatus() {
		
		return true;
	}
	
	private void saveData() {
		for (ColumnData col : columnDataList) {
			if (col.getEndColumnName().equals("")) {
				continue;
			}
			
			Edge newEdge = new Edge();
			
			newEdge.setEdgeLabel(txtEdgeName.getText());
			
			newEdge.setStartVertex(startVertex);
			newEdge.setStartVertexName(startVertex.getVertexLabel());
			
			newEdge.setEndVertex(endVertex);
			newEdge.setEndVertexName(endVertex.getVertexLabel());
			
			newEdge.addFKCol2Ref(col.getStartColumnName(), col.getEndColumnName());
			
			newEdge.setEdgeType(Edge.CUSTOM_TYPE);
			
			if (mConfig.targetIsCSV()) {
				Column startCol = new Column(":START_ID(" + startVertex.getVertexLabel() + ")");
				Column endCol = new Column(":END_ID(" + endVertex.getVertexLabel() + ")");
				
				startCol.setDataType("ID");
				endCol.setDataType("ID");
				
				newEdge.addColumn(startCol);
				newEdge.addColumn(endCol);
			}
			
			startVertex.getEndVertexes().add(endVertex);
			
			workBuffer.addWork(workCtrl.createWork(1, newEdge));
			
			gdbDict.addMigratedEdgeList(newEdge);
		}
	}
	
	@Override
	protected void okPressed() {
		// TODO Auto-generated method stub
		// return edge to graph view
		
		boolean flag = checkStatus();
		saveData();
		if (flag) {
			super.okPressed();
		}
	}
}
