/*
 * Copyright (C) 2009 Search Solution Corporation. All rights reserved by Search
 * Solution.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: -
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. - Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. - Neither the name of the <ORGANIZATION> nor the names
 * of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 */
package com.cubrid.cubridmigration.ui.script.dialog;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.TitleAreaDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.cubrid.common.ui.navigator.ICUBRIDGroupNodeManager;
import com.cubrid.common.ui.navigator.ICUBRIDNode;
import com.cubrid.common.ui.navigator.node.AbstractGroupNode;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * Filter setting dialog
 * 
 * @author Kevin
 * @version 1.0 - 2011-03-24 created by Kevin
 */
public class GroupEditDialog extends
		TitleAreaDialog {
	private static final Logger LOGGER = LogUtil.getLogger(GroupEditDialog.class);

	private List allItemList;
	private List groupItemList;
	private final java.util.List<AbstractGroupNode> allGroup;
	private final ICUBRIDGroupNodeManager nodeManager;

	private AbstractGroupNode group;
	private Text txtGroupName;
	private Button okButton;
	private Button btnAdd;
	private Button btnAddAll;
	private Button btnRemove;
	private Button btnRemoveAll;

	/**
	 * The constructor
	 * 
	 * @param parentShell
	 * @param tv
	 */
	public GroupEditDialog(Shell parentShell, ICUBRIDGroupNodeManager nodeManager,
			java.util.List<AbstractGroupNode> allGroup, AbstractGroupNode group) {
		super(parentShell);
		this.nodeManager = nodeManager;
		this.allGroup = allGroup;
		if (group == null) {
			this.group = null;
		} else {
			this.group = group;
		}
	}

	/**
	 * Create dialog area content
	 * 
	 * @param parent the parent composite
	 * @return the control
	 */
	protected Control createDialogArea(Composite parent) {
		this.getShell().setMinimumSize(380, 420);
		Composite parentComp = (Composite) super.createDialogArea(parent);
		Composite composite = new Composite(parentComp, SWT.NONE);
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));
		GridLayout layout = new GridLayout();
		layout.marginHeight = convertVerticalDLUsToPixels(IDialogConstants.VERTICAL_MARGIN);
		layout.marginWidth = convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_MARGIN);
		layout.verticalSpacing = convertVerticalDLUsToPixels(IDialogConstants.VERTICAL_SPACING);
		layout.horizontalSpacing = convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_SPACING);
		composite.setLayout(layout);
		createGroupList(composite);
		return parentComp;
	}

	/**
	 * Create manager buttons such as add,edit,remove.
	 * 
	 * @param parentComp parent composite.
	 */
	private void createManagerButtons(Composite parentComp) {
		Composite group2 = new Composite(parentComp, SWT.NONE);
		{
			group2.setLayout(new GridLayout());

			btnAdd = new Button(group2, SWT.NONE);
			btnAdd.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
			btnAdd.setText(">");
			btnAdd.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent event) {
					for (String ss : allItemList.getSelection()) {
						if (groupItemList.indexOf(ss) < 0) {
							groupItemList.add(ss);
						}
					}
					allItemList.remove(allItemList.getSelectionIndices());
					setButtonStatus();
				}

			});

			btnAddAll = new Button(group2, SWT.NONE);
			btnAddAll.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
			btnAddAll.setText(">>");
			btnAddAll.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent event) {
					for (String ss : allItemList.getItems()) {
						if (groupItemList.indexOf(ss) < 0) {
							groupItemList.add(ss);
						}
					}
					allItemList.removeAll();
					setButtonStatus();
				}

			});

			btnRemove = new Button(group2, SWT.NONE);
			btnRemove.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
			btnRemove.setText("<");
			btnRemove.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent event) {
					for (String ss : groupItemList.getSelection()) {
						if (allItemList.indexOf(ss) < 0) {
							allItemList.add(ss);
						}
					}
					groupItemList.remove(groupItemList.getSelectionIndices());
					setButtonStatus();
				}

			});

			btnRemoveAll = new Button(group2, SWT.NONE);
			btnRemoveAll.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
			btnRemoveAll.setText("<<");
			btnRemoveAll.addSelectionListener(new SelectionAdapter() {

				public void widgetSelected(SelectionEvent event) {
					for (String ss : groupItemList.getItems()) {
						if (allItemList.indexOf(ss) < 0) {
							allItemList.add(ss);
						}
					}
					groupItemList.removeAll();
					setButtonStatus();
				}

			});
		}
	}

	/**
	 * Override the create contents.
	 * 
	 * @param parent Composite
	 * @return Control
	 */
	protected Control createContents(Composite parent) {
		Control createContents = super.createContents(parent);
		//Fill group data to list.
		fillData();
		setButtonStatus();
		return createContents;
	}

	/**
	 * Create group list composite.
	 * 
	 * @param parentComp parent composite.
	 */
	private void createGroupList(Composite parentComp) {
		Composite groupName = new Composite(parentComp, SWT.NONE);
		{
			groupName.setLayout(new GridLayout());
			groupName.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

			Label btnSort = new Label(groupName, SWT.LEFT);
			btnSort.setText(Messages.labelGroupName);

			txtGroupName = new Text(groupName, SWT.LEFT | SWT.BORDER);
			txtGroupName.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
			txtGroupName.setText("");
			txtGroupName.addModifyListener(new ModifyListener() {

				public void modifyText(ModifyEvent event) {
					setButtonStatus();
				}
			});
		}

		Composite group1 = new Composite(parentComp, SWT.NONE);
		{
			group1.setLayout(new GridLayout(3, false));
			group1.setLayoutData(new GridData(GridData.FILL_BOTH));

			allItemList = new List(group1, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL);
			{
				allItemList.setLayoutData(new GridData(GridData.FILL_BOTH));
				allItemList.addSelectionListener(new SelectionAdapter() {
					public void widgetSelected(SelectionEvent event) {
						setButtonStatus();
					}
				});
			}
			createManagerButtons(group1);
			groupItemList = new List(group1, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL);
			{
				groupItemList.setLayoutData(new GridData(GridData.FILL_BOTH));
				groupItemList.addSelectionListener(new SelectionAdapter() {
					public void widgetSelected(SelectionEvent event) {
						setButtonStatus();
					}
				});
			}

		}

	}

	/**
	 * 
	 * Get default group node
	 * 
	 * @return CubridGroupNode
	 */
	private AbstractGroupNode getDefaultGroup() {
		for (AbstractGroupNode grp : allGroup) {
			if (nodeManager.isDefaultGroup(grp)) {
				return grp;
			}
		}
		return null;
	}

	/**
	 * Fill group information to dialog.
	 */
	private void fillData() {
		if (this.group == null) {
			txtGroupName.setText(Messages.defaultGroupNodeName);
			txtGroupName.setFocus();
			txtGroupName.selectAll();
		} else {
			txtGroupName.setText(group.getLabel());
			java.util.List<ICUBRIDNode> groups = this.group.getChildren();
			if (groups == null) {
				LOGGER.warn("groups = {}");
			} else {
				for (ICUBRIDNode node : groups) {
					groupItemList.add(node.getName());
				}
			}
		}

		AbstractGroupNode groupNode = getDefaultGroup();
		if (groupNode == null || allItemList == null) {
			LOGGER.warn("groupNode = {}, allItemList = {}");
			return;
		}

		java.util.List<ICUBRIDNode> nodes = groupNode.getChildren();
		if (nodes == null) {
			LOGGER.warn("nodes = {}");
		} else {
			for (ICUBRIDNode node : nodes) {
				if (node == null) {
					LOGGER.warn("CubridNode is a null.");
				} else {
					allItemList.add(node.getName());
				}
			}
		}
	}

	/**
	 * Retrieves the group with input name.
	 * 
	 * @param name of group which is being found.
	 * @return the group with input name.
	 */
	private AbstractGroupNode getGroupByName(String name) {
		return nodeManager.getGroupByName(allGroup, name);
	}

	/**
	 * Validate the group name.
	 * 
	 * @return valid is true;otherwise false;
	 */
	private boolean validateName() {
		String name = txtGroupName.getText().trim();
		if (group == null) {
			if (getGroupByName(name) != null) {
				//name exists
				this.setErrorMessage(Messages.groupNameExisted);
				return false;
			}
		} else if (!group.getName().equals(name) && getGroupByName(name) != null) {
			//name exists
			this.setErrorMessage(Messages.groupNameExisted);
			return false;
		}

		boolean result = name.length() > 0 && name.length() <= 50;
		if (result) {
			this.setErrorMessage(null);
		} else {
			this.setErrorMessage(Messages.groupNameInvalid);
		}
		return result;
	}

	/**
	 * Set buttons status.
	 * 
	 */
	private void setButtonStatus() {
		okButton.setEnabled(validateName());
		if (allItemList.getItemCount() == 0) {
			btnAddAll.setEnabled(false);
			btnAdd.setEnabled(false);
		} else if (allItemList.getSelectionCount() == 0) {
			btnAddAll.setEnabled(true);
			btnAdd.setEnabled(false);
		} else {
			btnAddAll.setEnabled(true);
			btnAdd.setEnabled(true);
		}
		if (groupItemList.getItemCount() == 0) {
			btnRemoveAll.setEnabled(false);
			btnRemove.setEnabled(false);
		} else if (groupItemList.getSelectionCount() == 0) {
			btnRemove.setEnabled(false);
			btnRemoveAll.setEnabled(true);
		} else {
			btnRemove.setEnabled(true);
			btnRemoveAll.setEnabled(true);
		}
	}

	/**
	 * Constrain the shell size
	 */
	protected void constrainShellSize() {
		super.constrainShellSize();
		//CommonUITool.centerShell(getShell());
		String titleMessage = "";
		if (group == null) {
			titleMessage = Messages.addNewGroup;
		} else {
			titleMessage = Messages.editGroup;
		}
		setTitle(titleMessage);
		setMessage(titleMessage);
		getShell().setText(Messages.titleGroupSettingDialog);
	}

	/**
	 * Create buttons for button bar
	 * 
	 * @param parent the parent composite
	 */
	protected void createButtonsForButtonBar(Composite parent) {
		okButton = createButton(parent, IDialogConstants.OK_ID, Messages.btnOK, true);
		createButton(parent, IDialogConstants.CANCEL_ID, Messages.btnCancel, false);
	}

	/**
	 * Generate a random unique group id for new group.
	 * 
	 * @return new group id.
	 */
	private String generateGroupId() {
		return UUID.randomUUID().toString();
	}

	/**
	 * Event when OK pressed.
	 */
	protected void okPressed() {
		String groupName = txtGroupName.getText().trim();
		if (group == null) {
			//add group
			group = nodeManager.createGroupNode(generateGroupId(), groupName);
		} else {
			AbstractGroupNode gnode = nodeManager.getGroupByName(group.getName());
			if (gnode != null) {
				gnode.setLabel(groupName);
			}
		}

		group.setLabel(groupName);
		//group children.
		group.removeAllChild();
		for (String name : groupItemList.getItems()) {
			ICUBRIDNode node = nodeManager.getGroupItemByItemName(name);
			if (node == null) {
				continue;
			}
			group.addChild(node);
		}

		AbstractGroupNode defaultGrp = nodeManager.getDefaultGroup();
		defaultGrp.removeAllChild();
		for (String name : allItemList.getItems()) {
			ICUBRIDNode node = nodeManager.getGroupItemByItemName(name);
			if (node == null) {
				continue;
			}
			defaultGrp.addChild(node);
		}

		super.okPressed();
	}

	/**
	 * Retrieves the edited group
	 * 
	 * @return the edited group.
	 */
	public AbstractGroupNode getGroup() {
		return group;
	}

}
