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
package com.cubrid.cubridmigration.ui.common;

import org.eclipse.core.runtime.IStatus;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.PlatformUI;

import com.cubrid.cubridmigration.ui.message.Messages;

/**
 * 
 * CommonTool
 * 
 * @author JessieHuang
 * @version 1.0 - 2009-10-12
 */
public final class UICommonTool {
	private UICommonTool() {
	}

	//	/**
	//	 * check IP
	//	 * 
	//	 * @param str IP string
	//	 * @return boolean
	//	 */
	//	public static boolean isIP(final String str) {
	//
	//		if (StringUtils.isBlank(str)) {
	//			return false;
	//		} else if ("localhost".equals(str)) {
	//			return true;
	//		}
	//		final String reg = "^([\\d]{1,3})\\.([\\d]{1,3})\\.([\\d]{1,3})\\.([\\d]{1,3})$";
	//		if (str.matches(reg)) {
	//			final String[] ipArray = str.split("\\.");
	//
	//			for (int i = 0; i < ipArray.length; i++) {
	//				if (Integer.parseInt(ipArray[i]) > 255) {
	//					return false;
	//				}
	//				if (ipArray[i].length() != 1 && ipArray[i].indexOf(0) == 0) {
	//					return false;
	//				}
	//			}
	//			if (Integer.parseInt(ipArray[0]) > 223) {
	//				return false;
	//			}
	//
	//			return true;
	//		}
	//		return false;
	//	}

	/**
	 * getMostSevere
	 * 
	 * @param status List<IStatus>
	 * @return IStatus
	 */
	public static IStatus getMostSevere(final java.util.List<IStatus> status) {
		IStatus max = null;

		for (IStatus curr : status) {
			if (curr == null) {
				continue;
			}

			if (curr.matches(IStatus.ERROR)) {
				max = curr;
				break;
			}

			if (max == null || curr.getSeverity() > max.getSeverity()) {
				max = curr;
			}
		}

		return max;
	}

	/**
	 * 
	 * Center this shell
	 * 
	 * @param shell Shell
	 */
	public static void centerShell(final Shell shell) {
		if (shell == null) {
			return;
		}

		Rectangle mainBounds;
		final Rectangle displayBounds = shell.getDisplay().getClientArea();

		if (shell.getShell() == null) {
			mainBounds = displayBounds;
		} else {
			mainBounds = PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell().getBounds();
		}

		final Rectangle shellBounds = shell.getBounds();

		int xpos = mainBounds.x + (mainBounds.width - shellBounds.width) / 2;
		int ypos = mainBounds.y + (mainBounds.height - shellBounds.height) / 2;

		if (xpos < 0) {
			xpos = 0;
		}

		if (ypos < 0) {
			ypos = 0;
		}

		if ((xpos + shellBounds.width) > displayBounds.width) {
			xpos = displayBounds.width - shellBounds.width;
		}

		if ((ypos + shellBounds.height) > displayBounds.height) {
			ypos = displayBounds.height - shellBounds.height;
		}

		shell.setLocation(xpos, ypos);
	}

	/**
	 * Create Grid data
	 * 
	 * @param horSpan int
	 * @param verSpan int
	 * @param widthHint int
	 * @param heightHint int
	 * @return GridData
	 */
	public static GridData createGridData(final int horSpan, final int verSpan,
			final int widthHint, final int heightHint) {
		final GridData gridData = new GridData();
		gridData.horizontalSpan = horSpan;
		gridData.verticalSpan = verSpan;

		if (heightHint >= 0) {
			gridData.heightHint = heightHint;
		}

		if (widthHint >= 0) {
			gridData.widthHint = widthHint;
		}

		return gridData;
	}

	/**
	 * 
	 * create grid data
	 * 
	 * @param style int
	 * @param horSpan int
	 * @param verSpan int
	 * @param widthHint int
	 * @param heightHint int
	 * @return GridData int
	 */
	public static GridData createGridData(final int style, final int horSpan, final int verSpan,
			final int widthHint, final int heightHint) {
		final GridData gridData = new GridData(style);
		gridData.horizontalSpan = horSpan;
		gridData.verticalSpan = verSpan;

		if (widthHint >= 0) {
			gridData.widthHint = widthHint;
		}

		if (heightHint >= 0) {
			gridData.heightHint = heightHint;
		}

		return gridData;
	}

	/**
	 * 
	 * Open confirm box
	 * 
	 * @param shell the shell object
	 * @param msg the detail message
	 * @return <code>true</code> if confirm;<code>false</code> otherwise
	 */
	public static boolean openConfirmBox(final Shell shell, final String msg) {
		return openMsgBox(shell, MessageDialog.WARNING, Messages.titleConfirm, msg, new String[] {
				Messages.btnYes, Messages.btnNo}) == 0;
	}

	/**
	 * 
	 * Open confirm box
	 * 
	 * @param msg the detail message
	 * @return <code>true</code> if confirm;<code>false</code> otherwise
	 */
	public static boolean openConfirmBox(final String msg) {
		return openConfirmBox(PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell(), msg);
	}

	/**
	 * 
	 * Open error box
	 * 
	 * @param msg the detail message
	 */
	public static void openErrorBox(final String msg) {
		openErrorBox(PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell(), msg);
	}

	/**
	 * 
	 * Open error box
	 * 
	 * @param shell the shell object
	 * @param msg the detail message
	 */
	public static void openErrorBox(final Shell shell, final String msg) {
		openMsgBox(shell, MessageDialog.ERROR, Messages.titleError, msg,
				new String[] {Messages.btnOK});
	}

	/**
	 * 
	 * Open information box
	 * 
	 * @param shell the shell object
	 * @param title the title
	 * @param msg the detail message
	 */
	public static void openInformationBox(final Shell shell, final String title, final String msg) {
		openMsgBox(shell, MessageDialog.INFORMATION, title, msg, new String[] {Messages.btnOK});
	}

	/**
	 * 
	 * Open Message dialog
	 * 
	 * @param shell the shell object
	 * @param dialogImageType the image type
	 * @param title the title
	 * @param msg the detail message
	 * @param dialogButton the button string array
	 * @return the integer value
	 */
	public static int openMsgBox(final Shell shell, final int dialogImageType, final String title,
			final String msg, final String[] dialogButton) {
		Shell newShell = shell;

		if (newShell == null) {
			newShell = PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell();
		}

		final MessageDialog dialog = new MessageDialog(newShell, title, null, msg, dialogImageType,
				dialogButton, 0);
		return dialog.open();
	}

	/**
	 * 
	 * This method encodes the url, removes the spaces from the url and replaces
	 * the same with <code>"%20"</code>.
	 * 
	 * @param input the input char array
	 * @return the string
	 */
	public static String urlEncodeForSpaces(char[] input) {
		StringBuffer retu = new StringBuffer(input.length);
		for (int i = 0; i < input.length; i++) {
			if (input[i] == ' ') {
				retu.append("%20");
			} else {
				retu.append(input[i]);
			}
		}
		return retu.toString();
	}
}
