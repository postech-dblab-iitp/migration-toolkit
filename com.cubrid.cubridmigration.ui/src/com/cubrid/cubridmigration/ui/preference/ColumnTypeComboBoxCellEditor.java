/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */
package com.cubrid.cubridmigration.ui.preference;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.viewers.ColumnViewerEditorActivationEvent;
import org.eclipse.jface.viewers.ComboBoxCellEditor;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.ViewerCell;
import org.eclipse.swt.widgets.Table;

import com.cubrid.cubridmigration.core.mapping.model.MapItem;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;

/**
 * ColumnTypeComboBoxCellEditor Description
 * 
 * @author Kevin.Wang
 * @version 1.0 - 2011-11-25 created by Kevin.Wang
 */
public class ColumnTypeComboBoxCellEditor extends
		ComboBoxCellEditor {

	final private TableViewer tableViewer;
	private MapObject selectedTarget = null;
	private MapItem mapItem;
	private MapItem xmlConfigMapItem;

	/**
	 * @param tableViewer TableViewer
	 * @param table Table
	 * @param targetDataTypeArr String[]
	 * @param readOnly INT
	 * @return ColumnTypeComboBoxCellEditor
	 */
	public ColumnTypeComboBoxCellEditor(TableViewer tableViewer, Table table,
			String[] targetDataTypeArr, int readOnly) {
		super(table, targetDataTypeArr, readOnly);
		this.tableViewer = tableViewer;
	}

	/**
	 * activate
	 * @param activationEvent ColumnViewerEditorActivationEvent
	 */
	public void activate(ColumnViewerEditorActivationEvent activationEvent) {
		List<String> itemList = new ArrayList<String>();

		ViewerCell cell = (ViewerCell) activationEvent.getSource();
		Object element = cell.getElement();

		if (element instanceof MapItem) {
			mapItem = (MapItem) element;

			MapObject source = mapItem.getSource();
			//find the MapItem from xml config map
			xmlConfigMapItem = mapItem.getMappingHelper().getXmlConfigMapItem(
					source.getDatatype(), source.getPrecision(),
					source.getScale());

			if (xmlConfigMapItem != null) {
				List<MapObject> availableTargetList = xmlConfigMapItem.getAvailableTargetList();
				for (MapObject mapObject : availableTargetList) {
					itemList.add(mapObject.getDatatype());
				}
			}
		}

		String[] strItems = new String[itemList.size()];
		for (int i = 0; i < itemList.size(); i++) {
			strItems[i] = itemList.get(i);
		}
		setItems(strItems);

		super.activate(activationEvent);
	}

	/**
	 * deactivate
	 */
	public void deactivate() {
		//set the selection
		Integer selected = (Integer) doGetValue();
		if (selected > -1 && tableViewer != null && mapItem != null
				&& xmlConfigMapItem != null
				&& xmlConfigMapItem.getAvailableTargetList().size() > selected) {
			selectedTarget = xmlConfigMapItem.getAvailableTargetList().get(
					selected);
			mapItem.setTarget(selectedTarget.clone());
			tableViewer.refresh();
		}

		super.deactivate();
	}
}
