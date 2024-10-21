package com.cubrid.cubridmigration.ui.wizard.dialog;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.MessageDialog;
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
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.DateTime;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.dbobject.DBObject;
import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;
import com.cubrid.cubridmigration.ui.message.Messages;


public class GraphDateTimeFilterDialog extends Dialog {
	
	public static final Image CHECK_IMAGE = MigrationUIPlugin.getImage("icon/checked.gif");
	public static final Image UNCHECK_IMAGE = MigrationUIPlugin.getImage("icon/unchecked.gif");
	
	private List<Column> dbObject = new ArrayList<Column>();	
	
	private String objectName;
	
	private TableViewer columnTable;
	
	private Column selectedColumn;
	
	private Object selectedObject;
	
	DateTime fromDateCalendar;
	DateTime fromTimeCalendar;
	DateTime toDateCalendar;
	DateTime toTimeCalendar;
	
	private String[] propertyList = {
		Messages.lblColumnName,
		Messages.lblDataType
	};
	
	public GraphDateTimeFilterDialog(Shell parentShell, Object selectedObject) {
		super(parentShell);
		
		this.selectedObject = selectedObject;
		
		this.objectName = ((DBObject) selectedObject).getName();
		
		if (selectedObject instanceof Vertex) {
			for (Column col : ((Vertex) selectedObject).getColumnList()) {
				if ((col.getDataType().equalsIgnoreCase("date") || col.getDataType().equalsIgnoreCase("datetime") || col.getDataType().matches("(?i).*TIMESTAMP.*")) && col.isSelected()) {
					dbObject.add(col);
				}
			}
		} else if (selectedObject instanceof Edge) {
			for (Column col : ((Edge) selectedObject).getColumnList()) {
				if ((col.getDataType().equalsIgnoreCase("date") || col.getDataType().equalsIgnoreCase("datetime") || col.getDataType().matches("(?i).*timestamp.*")) && col.isSelected()) {
					dbObject.add(col);
				}
			}
		}
	}
	
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setMinimumSize(600, 400);
		getShell().setText("filtering table: " + objectName);
	}
	
	@Override
	protected Control createDialogArea(Composite parent) {
		Composite comp = new Composite(parent, SWT.NONE);
		comp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		comp.setLayout(new GridLayout(2, true));
		
		Group grp1 = new Group(comp, SWT.NONE);
		grp1.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		grp1.setLayout(new FillLayout());
		
		Group grp2 = new Group(comp, SWT.NONE);
		grp2.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		grp2.setLayout(new GridLayout());
		
		setColumnTable(grp1);
		setCalendar(grp2);
		
		return parent;
	}
	
	protected void setCalendar(Group group) {
		
		Composite fromDateComp = new Composite(group, SWT.NONE);
		fromDateComp.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, true, true));
		fromDateComp.setLayout(new GridLayout(3, false));
		
		Label lblFromDate = new Label(fromDateComp, SWT.CENTER);
		lblFromDate.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblFromDate.setText("from: ");
		
		fromDateCalendar = new DateTime(fromDateComp, SWT.DATE);
		fromDateCalendar.setEnabled(false);
		
		fromTimeCalendar = new DateTime(fromDateComp, SWT.TIME);
		fromTimeCalendar.setEnabled(false);
		
		Label lblToDate = new Label(fromDateComp, SWT.CENTER);
		lblToDate.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
		lblToDate.setText("to: ");
		
		toDateCalendar = new DateTime(fromDateComp, SWT.DATE);
		toDateCalendar.setEnabled(false);
		
		toTimeCalendar = new DateTime(fromDateComp, SWT.TIME);
		toTimeCalendar.setEnabled(false);
	}
	
	protected void setColumnTable(Group group) {
		columnTable = new TableViewer(group, SWT.FULL_SELECTION);
		
		columnTable.setContentProvider(new IStructuredContentProvider() {
			
			@Override
			public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {}
			
			@Override
			public void dispose() {}
			
			@Override
			public Object[] getElements(Object inputElement) { 
				if (inputElement instanceof ArrayList) {
					List<Table> list = (ArrayList<Table>) inputElement;
					
					return list.toArray();
				} else {
					return new Object[0];
				}
			}
		});
		
		columnTable.setLabelProvider(new ITableLabelProvider() {
			
			@Override
			public void removeListener(ILabelProviderListener listener) {}
			
			@Override
			public boolean isLabelProperty(Object element, String property) { return false; }
			
			@Override
			public void dispose() {}
			
			@Override
			public void addListener(ILabelProviderListener listener) {}
			
			@Override
			public String getColumnText(Object element, int columnIndex) {
				Column col = (Column) element;
				
				switch (columnIndex) {
				case 0:
					return null;
				case 1:
					return col.getName();
				default:
					return col.getDataType();
				}	
			}			
			
			@Override
			public Image getColumnImage(Object element, int columnIndex) { 		
				Column column = (Column) element;
				if (columnIndex == 0) {
					if (column.isConditionColumn()) {
						return CHECK_IMAGE;
					} else {
						return UNCHECK_IMAGE;
					}
				}
				return null;	
			}
		});
		
		columnTable.addSelectionChangedListener(new ISelectionChangedListener() {
			
			@Override
			public void selectionChanged(SelectionChangedEvent event) {
			
				IStructuredSelection selection = (IStructuredSelection) event.getSelection();
				
				selectedColumn = (Column) selection.getFirstElement();
				
				fromDateCalendar.setEnabled(true);
				toDateCalendar.setEnabled(true);
				
				if (selectedColumn.getDataType().equalsIgnoreCase("date")) {
					fromTimeCalendar.setEnabled(false);
					toTimeCalendar.setEnabled(false);
				} else {
					fromTimeCalendar.setEnabled(true);
					toTimeCalendar.setEnabled(true);
				}
				
				if (selection.isEmpty()) {
					return;
				}

				TableItem[] tableItems = ((TableViewer)event.getSource()).getTable().getItems();
				
				for (TableItem item : tableItems) {
					Column col = (Column) item.getData();
					
					if (col.getName().equals(selectedColumn.getName())) {
						col.setConditionColumn(true);
					} else {
						col.setConditionColumn(false);
					}
				}
				
				columnTable.refresh();
			}
		});
		
		columnTable.setColumnProperties(propertyList);
		
		TableLayout tableLayout = new TableLayout();
		
		tableLayout.addColumnData(new ColumnWeightData(20));
		tableLayout.addColumnData(new ColumnWeightData(100));
		tableLayout.addColumnData(new ColumnWeightData(100));
		
		columnTable.getTable().setLayout(tableLayout);
		columnTable.getTable().setLinesVisible(true);
		columnTable.getTable().setHeaderVisible(true);
		
		TableColumn col0 = new TableColumn(columnTable.getTable(), SWT.CENTER);
		TableColumn col1 = new TableColumn(columnTable.getTable(), SWT.LEFT);
		TableColumn col2 = new TableColumn(columnTable.getTable(), SWT.LEFT);
		
		col0.setWidth(100);
		col1.setWidth(100);
		col2.setWidth(100);
		
		col1.setText(propertyList[0]);
		col2.setText(propertyList[1]);
		
		columnTable.setInput(dbObject);
	}
	
	protected boolean checkData() {
		
		if (selectedColumn == null) {
			MessageDialog.openWarning(getShell(), Messages.errColumnNotSelected, Messages.msgErrColumnNotSelected);
			return false;
		}

		int fromYear = fromDateCalendar.getYear();
		int fromMonth = fromDateCalendar.getMonth();
		int fromDate = fromDateCalendar.getDay();
		int toYear = toDateCalendar.getYear();
		int toMonth = toDateCalendar.getMonth();
		int toDate = toDateCalendar.getDay();
		
		int fromHour = 0, fromMin = 0, fromSec = 0;
		int toHour = 0, toMin = 0, toSec = 0;
		
		String fromDateTime;
		String toDateTime;
		
		if (fromTimeCalendar.isEnabled() && toTimeCalendar.isEnabled()) {
			fromHour = fromTimeCalendar.getHours();
			fromMin = fromTimeCalendar.getMinutes();
			fromSec = fromTimeCalendar.getSeconds();
			toHour = toTimeCalendar.getHours();
			toMin = toTimeCalendar.getMinutes();
			toSec = toTimeCalendar.getSeconds();
			
			Calendar fromCal = Calendar.getInstance();
			fromCal.set(fromYear, fromMonth, fromDate, fromHour, fromMin, fromSec);
			
			Calendar toCal = Calendar.getInstance();
			toCal.set(toYear, toMonth, toDate, toHour, toMin, toSec);
			
			int compareResult = fromCal.compareTo(toCal);
			
			if (compareResult > 0) {
				MessageDialog.openWarning(getShell(), Messages.errInvalidData, Messages.msgErrInvalidData);
				return false;
			} else {
			    fromDateTime = String.format("%d-%02d-%02d %02d:%02d:%02d", fromYear, fromMonth + 1, fromDate, fromHour, fromMin, fromSec);
			    toDateTime = String.format("%d-%02d-%02d %02d:%02d:%02d", toYear, toMonth + 1, toDate, toHour, toMin, toSec);
			}
		} else {
			Calendar fromCal = Calendar.getInstance();
			fromCal.set(fromYear, fromMonth, fromDate);
			
			Calendar toCal = Calendar.getInstance();
			toCal.set(toYear, toMonth, toDate);
			
			if (fromCal.compareTo(toCal) > 0) {
				MessageDialog.openWarning(getShell(), Messages.errInvalidData, Messages.msgErrInvalidData);
				return false;
			} else {
				fromDateTime = String.format("%d-%02d-%02d", fromYear, fromMonth + 1, fromDate);
				toDateTime = String.format("%d-%02d-%02d", toYear, toMonth + 1, toDate);
			}
		}
		
		selectedColumn.setFromDate(fromDateTime);
		selectedColumn.setToDate(toDateTime);
		
		return true;
	}
	
	private void setDateTimeFilter() {
		if (selectedObject instanceof Vertex) {
			((Vertex) selectedObject).setHasDateTimeFilter(true);
		} else {
			((Edge) selectedObject).setHasDateTimeFilter(true);
		}
	}
	
	@Override
	protected void okPressed() {
		boolean flag = checkData();
		
		if (flag){
			setDateTimeFilter();
			super.okPressed();
		} else {
			//TODO open dialog
		}
	}
}
