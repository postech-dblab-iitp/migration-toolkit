package com.cubrid.cubridmigration.ui.preference;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.viewers.ColumnViewerEditorActivationEvent;
import org.eclipse.jface.viewers.ComboBoxCellEditor;
import org.eclipse.jface.viewers.ViewerCell;
import org.eclipse.swt.widgets.Table;

import com.cubrid.cubridmigration.core.dbobject.Column;

public class GraphDataTypeComboBoxCellEditor extends ComboBoxCellEditor {
	public GraphDataTypeComboBoxCellEditor(Table table, String[] tableData) {
		super(table, tableData);
	}
	
	@Override
	public void activate(ColumnViewerEditorActivationEvent activationEvent) {
		ViewerCell cell = (ViewerCell) activationEvent.getSource();
		
		Object element = cell.getElement();
		
		if (element instanceof Column){
			Column gdbCol = (Column) element;
			
			List<String> typeList = getTypeList(gdbCol.getGraphDataType());
			
			setItems(typeList.toArray(new String[0]));
		}
	}
	
	public List<String> getTypeList(String type) {
		ArrayList<String> types = new ArrayList<String>();
		
		if (type.equals("not support")) {
			types.add(type);
			
			return types;
		} else if (type.equals("integer")) {
			types.add("integer");
			types.add("string");
			
			return types;
			
		} else if (type.equals("date")) {
			types.add("date");
			types.add("string");
			
			return types;
			
		} else if (type.equals("datetime")) {
			types.add("datetime");
			types.add("string");
			
			return types;
			
		} else if (type.equals("string")) {
			types.add("string");
			return types;
			
		} else {
			types.add(type);
			types.add("string");
			
			return types;
		}
	}
}
