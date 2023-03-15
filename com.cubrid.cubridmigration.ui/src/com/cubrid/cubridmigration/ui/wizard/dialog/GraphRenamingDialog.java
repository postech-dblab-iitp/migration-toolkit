package com.cubrid.cubridmigration.ui.wizard.dialog;

import java.util.ArrayList;

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.cubrid.cubridmigration.graph.dbobj.Edge;
import com.cubrid.cubridmigration.graph.dbobj.GraphDictionary;
import com.cubrid.cubridmigration.graph.dbobj.Vertex;

public class GraphRenamingDialog extends Dialog {
	
	private Object selectedObject;
	private Text txtNewName;
	private GraphDictionary gdbDict;

	public GraphRenamingDialog(Shell parentShell, GraphDictionary gdbDict, Object selectedObject) {
		super(parentShell);
		this.gdbDict = gdbDict;
		this.selectedObject = selectedObject;
	}
	
	protected void constrainShellSize() {
		super.constrainShellSize();
		getShell().setMinimumSize(500, 150);
		getShell().setText("Create New Edge");
	}

	@Override
	protected Control createDialogArea(Composite parent) {
		// TODO Auto-generated method stub
		
		Composite comp = new Composite(parent, SWT.NONE);
		comp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		comp.setLayout(new GridLayout(2, false));
		
		Label lblNewName = new Label(comp, SWT.NONE);
		lblNewName.setText("Enter new name: ");
//		lblNewName.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		
		txtNewName = new Text(comp, SWT.BORDER);
		txtNewName.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
		txtNewName.setText("");
		
		return parent;
	}
	
	private boolean changeName() {
		if (selectedObject instanceof Vertex) {
			Vertex selectedVertex = (Vertex) selectedObject;
			boolean isNameNotDuplicate = true;
			
			for (Vertex vertex : gdbDict.getMigratedVertexList()) {
				if (vertex.getVertexLabel().equalsIgnoreCase(txtNewName.getText())) {
					MessageDialog.openError(getShell(), "Error", "object name duplicate");
					isNameNotDuplicate = false;
				}
			}
			
			if (isNameNotDuplicate) {
				renameVertex(selectedVertex);
				
				return true;
			}
			
			return false;
			
		} else if (selectedObject instanceof Edge) {
			Edge selectedEdge = (Edge) selectedObject;
			boolean isNameNotDuplicate = true;
			
			for (Edge edge : gdbDict.getMigratedEdgeList()) {
				if (edge.getEdgeLabel().equalsIgnoreCase(txtNewName.getText())) {
					MessageDialog.openError(getShell(), "Error", "object name duplicate");
					isNameNotDuplicate = false;
				}
			}
			
			if (isNameNotDuplicate) {
				selectedEdge.setEdgeLabel(txtNewName.getText());
				return true;
			}
			
			return false;
		}
		return false;
	}
	
	private void renameVertex(Vertex selectedVertex) {
		//change edge name first
		
		ArrayList<Edge> edgeList = (ArrayList<Edge>) gdbDict.getMigratedEdgeList();
		
		for (Edge edge : edgeList) {
			if (edge.getStartVertexName().equals(selectedVertex.getVertexLabel())) {
				edge.setStartVertexName(txtNewName.getText());
			}
			
			if (edge.getEndVertexName().equals(selectedVertex.getVertexLabel())) {
				edge.setEndVertexName(txtNewName.getText());
			}
		}
		
		//change vertex name second
		
		ArrayList<Vertex> vertexList = (ArrayList<Vertex>) gdbDict.getMigratedVertexList();
		
		for (Vertex vertex : vertexList) {
			if (vertex.getVertexLabel().equals(selectedVertex.getVertexLabel())) {
				vertex.setVertexLabel(txtNewName.getText());
			}
			
			for (Vertex endVertex : vertex.getEndVertexes()) {
				if (endVertex.getVertexLabel().equals(selectedVertex.getVertexLabel())) {
					endVertex.setVertexLabel(txtNewName.getText());
				}
			}
		}
		
		selectedVertex.setVertexLabel(txtNewName.getText());
		
		
	}
	
	@Override
	protected void okPressed() {
		// TODO Auto-generated method stub
		boolean isNameChanged = changeName();
		
		if (isNameChanged) {
			super.okPressed();
		}
	}
}
