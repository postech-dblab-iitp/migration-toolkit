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

public class Neo4jToTiberoTypeMapPage extends
		PreferencePage implements
		IWorkbenchPreferencePage {
	private DataTypeMappingComposite container;

	public Neo4jToTiberoTypeMapPage() {
		super("Neo4j To Tibero", null);
	}
	
	@Override
	public void init(IWorkbench workbench) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Control createContents(Composite parent) {
		Composite com = new Composite(parent, SWT.NONE);
		com.setLayout(new GridLayout());
		com.setLayoutData(new GridData(SWT.FILL));
		container = new DataTypeMappingComposite(com, MigrationTransFactory.getTransformHelper(
				DatabaseType.NEO4J, DatabaseType.TIBERO).getDataTypeMappingHelper());
		return com;
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
				dialog.setFileName("NEO4J2Tibero.xml");
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
