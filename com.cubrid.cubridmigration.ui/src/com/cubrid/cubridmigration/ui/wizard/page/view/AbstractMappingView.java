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
package com.cubrid.cubridmigration.ui.wizard.page.view;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.viewers.ICellEditorValidator;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.TableItem;

import com.cubrid.common.ui.StructuredContentProviderAdaptor;
import com.cubrid.common.ui.swt.table.CellEditorFactory;
import com.cubrid.common.ui.swt.table.ObjectArrayRowCellModifier;
import com.cubrid.common.ui.swt.table.TableViewerBuilder;
import com.cubrid.common.ui.swt.table.celleditor.CheckboxCellEditorFactory;
import com.cubrid.common.ui.swt.table.celleditor.TextCellEditorFactory;
import com.cubrid.common.ui.swt.table.listener.CheckBoxColumnSelectionListener;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.engine.config.SourceConfig;
import com.cubrid.cubridmigration.ui.common.CompositeUtils;
import com.cubrid.cubridmigration.ui.common.tableviewer.cell.validator.CUBRIDNameValidator;
import com.cubrid.cubridmigration.ui.wizard.IMigrationWizardStatus;
import com.cubrid.cubridmigration.ui.wizard.utils.VerifyResultMessages;

/**
 * AbstractMappingView Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-7-25 created by Kevin Cao
 */
public abstract class AbstractMappingView {
	public static final String CONTENT_TYPE = "content_type";
	public static final String CT_TABLE = "table";
	public static final String CT_VIEW = "view";
	public static final String CT_SERIAL = "serial";
	public static final String CT_COLUMN = "column";
	public static final String CT_FK = "fk";
	public static final String CT_INDEX = "index";

	protected static final int PART_WIDTH = 250;
	//Initialize the config, just it doesn't need to throw null point error.
	protected MigrationConfiguration config = new MigrationConfiguration();
	protected IMigrationWizardStatus wizardStatus;
	private Object model;

	public AbstractMappingView(Composite parent) {
		createControl(parent);
	}

	/**
	 * Create controls in this will, this method will be called in constructor
	 * 
	 * @param parent Composite
	 */
	protected abstract void createControl(Composite parent);

	/**
	 * Retrieves the model which this view is showing.
	 * 
	 * @return Object
	 */
	public final Object getModel() {
		return model;
	}

	/**
	 * Make the view invisible.
	 * 
	 */
	public abstract void hide();

	/**
	 * It can initialize table viewer of DB view and serial and FK and indexes.
	 * 
	 * @param tv Table Viewer
	 */
	protected final void initSourceConfigTableViewer(TableViewer tv) {
		CompositeUtils.setTableColumnSelectionListener(tv, new SelectionListener[] {null, null,
				new CheckBoxColumnSelectionListener(new int[] {3}, true, true),
				new CheckBoxColumnSelectionListener(new int[] {2}, true, false)});
	}

	/**
	 * It can initialize table viewer of DB view and serial and FK and indexes.
	 * 
	 * @param tvb Table Viewer
	 */
	protected final void initSourceConfigTableBuilder(TableViewerBuilder tvb) {
		tvb.setTableCursorSupported(true);
		tvb.setColumnWidths(new int[] {150, 150, 80, 90});
		tvb.setContentProvider(new StructuredContentProviderAdaptor() {

			@SuppressWarnings("unchecked")
			public Object[] getElements(Object inputElement) {
				List<Object> data = new ArrayList<Object>();
				for (SourceConfig sc : (List<SourceConfig>) inputElement) {
					//Add the SourceConfig to the end of the object array.
					data.add(new Object[] {sc.getName(), sc.getTarget(), sc.isCreate(),
							sc.isReplace(), sc});
				}
				return super.getElements(data);
			}
		});

		final CellEditorFactory[] cellEditors = new CellEditorFactory[] {null,
				new TextCellEditorFactory(), new CheckboxCellEditorFactory(),
				new CheckboxCellEditorFactory()};
		ObjectArrayRowCellModifier cellModifier = new ObjectArrayRowCellModifier() {

			protected void modify(TableItem ti, Object[] element, int columnIdx, Object value) {
				Object[] obj = (Object[]) ti.getData();
				if (value instanceof Boolean) {
					boolean bv = (Boolean) value;
					if (columnIdx == 2) {
						obj[3] = bv;
						ti.setImage(3, CompositeUtils.getCheckImage(bv));
						updateColumnImage(value, ti, columnIdx + 1);
					} else {
						obj[2] = (Boolean) obj[2] || bv;
						ti.setImage(2, CompositeUtils.getCheckImage((Boolean) obj[2]));
						updateColumnImage(value, ti, columnIdx - 1);
					}
				}
				super.modify(ti, element, columnIdx, value);
			}
		};
		tvb.setCellEditorClasses(cellEditors);
		tvb.setCellModifier(cellModifier);
		tvb.setCellValidators(new ICellEditorValidator[] {null, new CUBRIDNameValidator(), null,
				null});
	}

	/**
	 * Save view data to model object. Some subclass should override this
	 * method.
	 * 
	 * @return VerifyResultMessages
	 */
	public VerifyResultMessages save() {
		//Do nothing
		return new VerifyResultMessages();
	}

	/**
	 * Set the migration configuration.It should not be NULL.
	 * 
	 * @param config MigrationConfiguration
	 */
	public void setMigrationConfig(MigrationConfiguration config) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration can't be NULL");
		}
		this.config = config;
	}

	public void setWizardStatus(IMigrationWizardStatus wizardStatus) {
		this.wizardStatus = wizardStatus;
	}

	/**
	 * Bring the view onto the top
	 * 
	 */
	public abstract void show();

	/**
	 * Show the data onto UI. Subclass should override this method and call
	 * super at method start.
	 * 
	 * @param obj to be shown
	 */
	public void showData(Object obj) {
		if (obj == null) {
			throw new IllegalArgumentException("The object to be show can't be Null.");
		}
		this.model = obj;
	}
}
