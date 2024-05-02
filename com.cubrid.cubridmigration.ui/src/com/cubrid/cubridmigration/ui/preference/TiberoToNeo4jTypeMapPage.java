package com.cubrid.cubridmigration.ui.preference;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;

import com.cubrid.cubridmigration.core.dbtype.DatabaseType;
import com.cubrid.cubridmigration.core.trans.MigrationTransFactory;
import com.cubrid.cubridmigration.ui.message.Messages;

public class TiberoToNeo4jTypeMapPage extends
		PreferencePage implements
		IWorkbenchPreferencePage  {

	private DataTypeMappingComposite container;

	public TiberoToNeo4jTypeMapPage() {
		// TODO Auto-generated constructor stub
		super("CUBRID to Neo4j", null);
	}

	/**
	 * createContents
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createContents(Composite parent) {
		Composite com = new Composite(parent, SWT.NONE);
		com.setLayout(new GridLayout());
		com.setLayoutData(new GridData(SWT.FILL));
		container = new DataTypeMappingComposite(com, MigrationTransFactory.getTransformHelper(
				DatabaseType.TIBERO, DatabaseType.NEO4J).getDataTypeMappingHelper());
		return com;
	}

	/**
	 * init
	 * 
	 * @param workbench IWorkbench
	 */
	public void init(IWorkbench workbench) {
		//empty
	}

	/**
	 * performOk
	 * 
	 * @return boolean
	 */
	public boolean performOk() {
		if (container == null) {
			return true;
		}
		if (!checkValues()) {
			return false;
		}

		container.save();
		return true;
	}

	/**
	 * performDefaults
	 */
	protected void performDefaults() {
		if (container == null) {
			return;
		}
		if (!checkValues()) {
			return;
		}

		container.perfromDefaults();
	}

	/**
	 * check input value
	 * 
	 * @return boolean
	 */
	private boolean checkValues() {
		return true;
	}

	/**
	 * Create save and load button
	 * 
	 * @param parent of the buttons
	 */
	protected void contributeButtons(Composite parent) {
		parent.setLayout(new GridLayout(4, false));

		Button btnSave = new Button(parent, SWT.NONE);
		btnSave.setText(Messages.lblExport);
		btnSave.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				FileDialog dialog = new FileDialog(getShell(), SWT.SAVE);
				dialog.setOverwrite(true);
				dialog.setFileName("Tibero2Neo4j.xml");
				dialog.setFilterExtensions(new String[] {"*.xml"});
				String fn = dialog.open();
				if (StringUtils.isBlank(fn)) {
					return;
				}
				container.saveAs(fn);
			}
		});
		Button btnLoad = new Button(parent, SWT.NONE);
		btnLoad.setText(Messages.lblImport);
		btnLoad.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent ev) {
				if (!MessageDialog.openConfirm(getShell(), Messages.msgWarning,
						Messages.msgMappingOverride)) {
					return;
				}
				FileDialog dialog = new FileDialog(getShell(), SWT.OPEN | SWT.SINGLE);
				dialog.setFilterExtensions(new String[] {"*.xml"});
				String fn = dialog.open();
				if (StringUtils.isBlank(fn)) {
					return;
				}
				container.load(fn);
			}
		});
		super.contributeButtons(parent);
	}

	/**
	 * Create controls
	 * 
	 * @param parent Composite
	 */
	public void createControl(Composite parent) {
		super.createControl(parent);
		Button btnDB = this.getDefaultsButton();
		btnDB.setText(Messages.btnDefault);
		Button btnAB = this.getApplyButton();
		btnAB.setText(Messages.btnApply);
	}

}
