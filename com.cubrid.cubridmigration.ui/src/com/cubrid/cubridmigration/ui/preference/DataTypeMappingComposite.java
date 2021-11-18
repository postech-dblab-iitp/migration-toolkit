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

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TextCellEditor;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Item;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;

import com.cubrid.common.ui.swt.Resources;
import com.cubrid.cubridmigration.core.datatype.DataTypeSymbol;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.MapItem;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * Type mapping composite
 * 
 * @author pangqiren
 */
public class DataTypeMappingComposite {

	/**
	 * 
	 * MySelectionAdapter
	 * 
	 * @author JessieHuang
	 */
	private class MySelectionAdapter extends
			SelectionAdapter {
		private final String name;

		public MySelectionAdapter(String name) {
			this.name = name;
		}

		/**
		 * Override widgetSelected
		 * 
		 * @param event SelectionEvent
		 */
		public void widgetSelected(SelectionEvent event) {
			TableColumn sortColumn = tableViewer.getTable().getSortColumn();
			TableColumn currentColumn = (TableColumn) event.widget;
			int dir = tableViewer.getTable().getSortDirection();

			if (sortColumn == currentColumn) { //NOPMD
				dir = dir == SWT.UP ? SWT.DOWN : SWT.UP;
			} else {
				tableViewer.getTable().setSortColumn(currentColumn);
				dir = SWT.UP;
			}

			tableViewer.getTable().setSortDirection(dir);
			tableViewer.setSorter(new DataTypeSorter(name, dir));
			refresh();
		}
	}

	/**
	 * 
	 * TableContentProvider Description
	 * 
	 * @author pangqiren
	 */
	private static class TableContentProvider implements
			IStructuredContentProvider {

		/**
		 * dispose
		 */
		public void dispose() {
			//empty
		}

		/**
		 * getElements
		 * 
		 * @param inputElement Object
		 * @return an Object[]
		 */
		public Object[] getElements(Object inputElement) {
			if (inputElement instanceof Collection) {
				return ((Collection<?>) inputElement).toArray();
			}
			return new Object[] {};
		}

		/**
		 * inputChanged
		 * 
		 * @param viewer Viewer
		 * @param oldInput Object
		 * @param newInput Object
		 */
		public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
			//empty
		}
	}

	/**
	 * TableLabelProvider
	 * 
	 * @author pangqiren
	 */
	private static class TableLabelProvider implements
			ITableLabelProvider {

		/**
		 * addListener
		 * 
		 * @param listener ILabelProviderListener
		 */
		public void addListener(ILabelProviderListener listener) {
			//empty
		}

		/**
		 * dispose
		 */
		public void dispose() {
			//empty
		}

		/**
		 * getColumnImage
		 * 
		 * @param element Object
		 * @param columnIndex int
		 * @return Image
		 */
		public Image getColumnImage(Object element, int columnIndex) {
			return null;
		}

		/**
		 * getColumnText
		 * 
		 * @param element Object
		 * @param columnIndex int
		 * @return String
		 */
		public String getColumnText(Object element, int columnIndex) {
			if (!(element instanceof MapItem)) {
				return "";
			}

			MapItem mapItem = (MapItem) element;
			MapObject sourceDataTypeMapping = mapItem.getSource();
			MapObject targetDataTypeMapping = mapItem.getTarget();

			if (columnIndex == 0) {
				return sourceDataTypeMapping == null ? "" : sourceDataTypeMapping.getDatatype();
			} else if (columnIndex == 1) {
				return sourceDataTypeMapping == null
						|| sourceDataTypeMapping.getPrecision() == null ? ""
						: sourceDataTypeMapping.getPrecision();
			} else if (columnIndex == 2) {
				return sourceDataTypeMapping == null || sourceDataTypeMapping.getScale() == null ? ""
						: sourceDataTypeMapping.getScale();
			} else if (columnIndex == 3) {
				return targetDataTypeMapping == null ? "" : targetDataTypeMapping.getDatatype();
			} else if (columnIndex == 4) {
				return targetDataTypeMapping == null
						|| targetDataTypeMapping.getPrecision() == null ? ""
						: targetDataTypeMapping.getPrecision();
			} else if (columnIndex == 5) {
				return targetDataTypeMapping == null || targetDataTypeMapping.getScale() == null ? ""
						: targetDataTypeMapping.getScale();
			}

			return "";
		}

		/**
		 * isLabelProperty
		 * 
		 * @param element Object
		 * @param property String
		 * @return boolean
		 */
		public boolean isLabelProperty(Object element, String property) {
			return false;
		}

		/**
		 * removeListener
		 * 
		 * @param listener ILabelProviderListener
		 */
		public void removeListener(ILabelProviderListener listener) {
			//empty
		}

	}

	private final AbstractDataTypeMappingHelper dataTypeMapping;
	private final String[] targetDataTypeArr;
	private Map<String, MapItem> dataTypeMap = null;
	private Collection<MapItem> mappingItemCollection;

	private TableViewer tableViewer;
	private Table table;
	private final String[] columnNameArr = new String[] {Messages.colSourceDataType,
			Messages.colPrecision, Messages.colScale, Messages.colTargetDataType,
			Messages.colPrecision, Messages.colScale};

	private ICellModifier cellModifier = new ICellModifier() {

		public boolean canModify(Object element, String property) {
			if (property.equals(columnNameArr[3])) {
				return true;
			} else if (property.equals(columnNameArr[4])) {
				return true;
			} else if (property.equals(columnNameArr[5])) {
				return true;
			}

			return false;
		}

		public Object getValue(Object element, String property) {
			MapItem mapItem = (MapItem) element;
			MapObject targetDataTypeMapping = mapItem.getTarget();

			if (property.equals(columnNameArr[3])) {
				String dataType = targetDataTypeMapping == null ? ""
						: targetDataTypeMapping.getDatatype();
				int index = 0;

				for (int i = 0; i < targetDataTypeArr.length; i++) {
					if (dataType != null && dataType.equals(targetDataTypeArr[i])) {
						index = i;
						break;
					}
				}

				return Integer.valueOf(index);
			} else if (property.equals(columnNameArr[4])) {
				return targetDataTypeMapping == null
						|| targetDataTypeMapping.getPrecision() == null ? ""
						: targetDataTypeMapping.getPrecision();
			} else if (property.equals(columnNameArr[5])) {
				return targetDataTypeMapping == null || targetDataTypeMapping.getScale() == null ? ""
						: targetDataTypeMapping.getScale();
			}

			return null;
		}

		public void modify(Object element, String property, Object value) {
			MapItem mapItem = null;

			if (element instanceof Item) {
				mapItem = (MapItem) ((Item) element).getData();
			}

			if (mapItem == null || mapItem.getTarget() == null) {
				return;
			}

			MapObject targetDataTypeMapping = mapItem.getTarget();

			String str = value.toString();

			if (property.equals(columnNameArr[3])) {
				if (str.matches("^\\d+$")) {
					int index = Integer.parseInt(str);

					if (targetDataTypeArr != null) {
						if (index >= targetDataTypeArr.length) {
							index = 0;
						}

						targetDataTypeMapping.setDatatype(targetDataTypeArr[index]);
					}
				}
			} else if (property.equals(columnNameArr[4])) {
				targetDataTypeMapping.setPrecision(str);
			} else if (property.equals(columnNameArr[5])) {
				targetDataTypeMapping.setScale(str);
			}

			tableViewer.refresh();
			refresh();
		}

	};

	public DataTypeMappingComposite(Composite parent, AbstractDataTypeMappingHelper dataTypeMapping) {
		this.dataTypeMapping = dataTypeMapping;
		CUBRIDDataTypeHelper dataTypeHelper = CUBRIDDataTypeHelper.getInstance(null);
		final DataTypeSymbol[] cubridDataType = dataTypeHelper.getCUBRIDDataTypes();
		targetDataTypeArr = new String[cubridDataType.length - 3];
		int index = 0;

		for (int i = 0; i < cubridDataType.length; i++) {
			DataTypeSymbol type = cubridDataType[i];
			String innerType = type.getInnerDataType();

			if (dataTypeHelper.isCollection(innerType)) {
				continue;
			}
			targetDataTypeArr[index] = innerType;
			if ("object".equals(innerType)) {
				targetDataTypeArr[index] = "glo";
			}
			index++;
		}
		if (dataTypeMapping == null || dataTypeMapping.getPreferenceConfigMap() == null) {
			mappingItemCollection = null;
		} else {
			init(dataTypeMapping.getPreferenceConfigMap());
		}
		createTypeMappingTableGroup(parent);
		//sort up
		TableColumn currentColumn = tableViewer.getTable().getColumn(0);
		int dir = SWT.UP;
		table.setSortColumn(currentColumn);
		table.setSortDirection(dir);
		tableViewer.setSorter(new DataTypeSorter(DataTypeSorter.SRC_DATATYPE_SORT, dir));
		refresh();
	}

	/**
	 * createTypeMappingTableGroup
	 * 
	 * @param parent Composite
	 */
	private void createTypeMappingTableGroup(Composite parent) {
		tableViewer = new TableViewer(parent, SWT.BORDER | SWT.FULL_SELECTION);
		table = tableViewer.getTable();
		table.setLayout(new GridLayout());
		table.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

		tableViewer.setContentProvider(new TableContentProvider());
		tableViewer.setLabelProvider(new TableLabelProvider());
		table.setLinesVisible(true);
		table.setHeaderVisible(true);

		for (int i = 0; i < columnNameArr.length; i++) {
			final TableColumn tblColumn = new TableColumn(table, SWT.RIGHT);

			if (i == 0) {
				tblColumn.setAlignment(SWT.LEFT);
				tblColumn.addSelectionListener(new MySelectionAdapter(
						DataTypeSorter.SRC_DATATYPE_SORT));
			} else if (i == 3) {
				tblColumn.setAlignment(SWT.LEFT);
				tblColumn.addSelectionListener(new MySelectionAdapter(
						DataTypeSorter.TARGET_DATATYPE_SORT));
			}

			tblColumn.setText(columnNameArr[i]);
		}

		if (mappingItemCollection != null) {
			tableViewer.setInput(mappingItemCollection);
		}

		for (int i = 0; i < table.getColumnCount(); i++) {
			table.getColumn(i).pack();
		}

		refresh();

		tableViewer.setColumnProperties(columnNameArr);
		CellEditor[] editors = new CellEditor[6];
		editors[0] = null;
		editors[1] = null;
		editors[2] = null;
		editors[3] = new ColumnTypeComboBoxCellEditor(tableViewer, table, targetDataTypeArr,
				SWT.READ_ONLY);
		editors[4] = new TextCellEditor(table);
		editors[5] = new TextCellEditor(table);
		tableViewer.setCellEditors(editors);

		tableViewer.setCellModifier(cellModifier);
	}

	/**
	 * init
	 * 
	 * @param map Map<String, MapItem>
	 */
	private void init(Map<String, MapItem> map) {
		dataTypeMap = new HashMap<String, MapItem>();
		for (Map.Entry<String, MapItem> entry : map.entrySet()) {
			String key = entry.getKey();
			MapItem item = entry.getValue().clone();
			dataTypeMap.put(key, item);
		}
		mappingItemCollection = dataTypeMap.values();
	}

	/**
	 * Load from configuration file
	 * 
	 * @param fileName to be loaded
	 */
	public void load(String fileName) {
		try {
			StringBuffer xmlString = new StringBuffer();
			final FileInputStream in = new FileInputStream(fileName);
			InputStreamReader fr = new InputStreamReader(in, "UTF-8");
			try {
				char[] buff = new char[1024];
				int count = fr.read(buff);
				while (count > 0) {
					xmlString.append(buff, 0, count);
					count = fr.read(buff);
				}
			} finally {
				fr.close();
			}
			dataTypeMapping.loadFromPreference(xmlString.toString().trim());
			Map<String, MapItem> map = dataTypeMapping.getPreferenceConfigMap();

			if (map == null) {
				dataTypeMap = null;
				mappingItemCollection = null;
			} else {
				init(map);
				tableViewer.setInput(mappingItemCollection);
				tableViewer.refresh();
				refresh();
			}
		} catch (Exception ex) {
			MessageDialog.openError(Display.getDefault().getActiveShell(), Messages.msgError,
					Messages.msgErrImportMappingFailed);
		}
	}

	/**
	 * perfromDefaults
	 */
	public void perfromDefaults() {
		Map<String, MapItem> map = dataTypeMapping.restoreDefault();

		if (map == null) {
			dataTypeMap = null;
			mappingItemCollection = null;

		} else {
			init(map);
			tableViewer.setInput(mappingItemCollection);
			tableViewer.refresh();
			refresh();
		}
	}

	/**
	 * refresh
	 */
	private void refresh() {
		for (int i = 0; table != null && i < table.getItemCount(); i++) {
			for (int j = 0; j < 3; j++) {
				table.getItem(i).setBackground(j, Resources.getInstance().getColor(200, 200, 200));
			}
		}
	}

	/**
	 * save
	 */
	public void save() {
		dataTypeMapping.setDataTypeMap(dataTypeMap);
		DataTypeMappingUtil.save(dataTypeMapping, dataTypeMapping.getPreferenceConfigMap());
	}

	/**
	 * Save current configuration to a XML file.
	 * 
	 * @param fileName to save to
	 */
	public void saveAs(String fileName) {
		dataTypeMapping.setDataTypeMap(dataTypeMap);
		DataTypeMappingUtil.saveAs(dataTypeMapping, dataTypeMapping.getPreferenceConfigMap(),
				fileName);
	}
}
