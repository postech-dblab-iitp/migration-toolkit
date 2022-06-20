package com.cubrid.cubridmigration.ui.database;

import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.swt.graphics.Image;

import com.cubrid.cubridmigration.core.dbobject.Table;
import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

//GDB
public class GraphLabelProvider implements
		ITableLabelProvider {
	public static final Image CHECK_IMAGE = MigrationUIPlugin.getImage("icon/checked.gif");
	public static final Image UNCHECK_IMAGE = MigrationUIPlugin.getImage("icon/unchecked.gif");
	
	@Override
	public Image getColumnImage(Object element, int columnIndex) {
		Table table = (Table) element;
		
		if (columnIndex == 0) {
			if (table.isSelected()) {
				return CHECK_IMAGE;
			} else {
				return UNCHECK_IMAGE;
			}
		}
		return null;
	}
	
	@Override
	public String getColumnText(Object element, int columnIndex) {
		Table table = (Table) element;
		
		switch (columnIndex) {
		case 0:
			return null;
		case 1:
			return table.getName();
		default:
			return null;
		}
	}
	
	@Override
	public void addListener(ILabelProviderListener listener) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dispose() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isLabelProperty(Object element, String property) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void removeListener(ILabelProviderListener listener) {
		// TODO Auto-generated method stub
		
	}
}
