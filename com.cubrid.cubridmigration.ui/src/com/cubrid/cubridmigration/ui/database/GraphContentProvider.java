package com.cubrid.cubridmigration.ui.database;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.Viewer;

import com.cubrid.cubridmigration.core.dbobject.Table;

//GDB content provider
public class GraphContentProvider implements 
		IStructuredContentProvider {
	
	@Override
	@SuppressWarnings("unchecked")
	public Object[] getElements(Object inputElement) {
		if (inputElement instanceof ArrayList) {
			List<Table> list = (ArrayList<Table>) inputElement;
			
			return list.toArray();
			
		} else {
			return new Object[0];
		}
	}

	@Override
	public void dispose() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
		// TODO Auto-generated method stub
		
	}
}
