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

package com.cubrid.common.ui.handler;

import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtension;
import org.eclipse.core.runtime.RegistryFactory;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Widget;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.commands.ICommandService;
import org.eclipse.ui.menus.CommandContributionItem;
import org.eclipse.ui.menus.CommandContributionItemParameter;

import com.cubrid.cubridmigration.ui.MigrationUIPlugin;

/**
 * 
 * This action is responsible to drop a list of tool bar menu.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2014-3-07 created by Kevin Cao
 */
public abstract class AbstractDropdownMenuHandler extends
		AbstractHandler {

	private MenuManager menuManager;

	/**
	 * Pop menus.
	 * 
	 * @param event ExecutionEvent
	 * @return null
	 * @throws ExecutionException ex
	 */
	public Object execute(ExecutionEvent event) throws ExecutionException {
		if (event.getTrigger() instanceof Event) {
			Event ev = (Event) event.getTrigger();
			if (ev.widget instanceof ToolItem) {
				Widget widget = ev.widget;
				ToolItem toolItem = (ToolItem) widget;
				Composite parent = toolItem.getParent();
				Rectangle rect = toolItem.getBounds();
				Point pt = new Point(rect.x, rect.y + rect.height);
				pt = parent.toDisplay(pt);
				if (menuManager == null) {
					menuManager = getMenuManager();
				}
				if (menuManager == null) {
					return null;
				}
				menuManager.update(null);
				Menu contextMenu = menuManager.createContextMenu(parent);
				contextMenu.setLocation(pt.x, pt.y);
				contextMenu.setVisible(true);
			}
		}
		return null;
	}

	/**
	 * getMenuManager
	 * 
	 * @return MenuManager
	 */
	protected MenuManager getMenuManager() {
		try {
			MenuManager menuManager = new MenuManager();
			Object oo = PlatformUI.getWorkbench().getService(ICommandService.class);
			if (oo == null) {
				return null;
			}
			IExtension extension = RegistryFactory.getRegistry().getExtension(getMenuID());
			IConfigurationElement[] configurationElements = extension.getConfigurationElements();
			for (IConfigurationElement ce : configurationElements) {
				IConfigurationElement[] chs = ce.getChildren();
				for (IConfigurationElement chce : chs) {
					String commandId = chce.getAttribute("commandId");
					if (commandId == null) {
						menuManager.add(new Separator());
						continue;
					}
					CommandContributionItemParameter ppp = new CommandContributionItemParameter(
							PlatformUI.getWorkbench(), "", commandId,
							CommandContributionItem.STYLE_PUSH);
					if (chce.getAttribute("icon") != null) {
						ppp.icon = MigrationUIPlugin.getImageDescriptor(chce.getAttribute("icon"));
					}
					ppp.tooltip = chce.getAttribute("tooltip");
					ppp.label = chce.getAttribute("label");
					CommandContributionItem item = new CommandContributionItem(ppp);
					menuManager.add(item);
				}
			}
			return menuManager;
		} catch (Exception ex) {
			ex.printStackTrace();
			//Do nothing here.
		}
		return null;
	}

	/**
	 * Users should override this method to pop tool bar menus.
	 * 
	 * @return Menu ID in the extensions
	 */
	protected abstract String getMenuID();
}
