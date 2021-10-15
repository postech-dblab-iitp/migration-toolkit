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
package com.cubrid.common.ui.listener;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;

/**
 * KeyStateMaskAdapter Description
 * 
 * @author Kevin Cao
 * @version 1.0 - 2012-8-28 created by Kevin Cao
 */
public class KeyStateMaskAdapter extends
		KeyAdapter {

	private boolean isShift;
	private boolean isCtrl;
	private boolean isAlt;

	public boolean isShift() {
		return isShift;
	}

	public boolean isCtrl() {
		return isCtrl;
	}

	public boolean isAlt() {
		return isAlt;
	}

	/**
	 * Record the mask key status ( shift/alt/ctrl)
	 * 
	 * @param ev KeyEvent
	 */
	public void keyPressed(KeyEvent ev) {
		isShift = (ev.keyCode == SWT.SHIFT || ev.stateMask == SWT.SHIFT);
		isAlt = (ev.keyCode == SWT.ALT || ev.stateMask == SWT.ALT);
		isCtrl = (ev.keyCode == SWT.CTRL || ev.stateMask == SWT.CTRL);
	}

	/**
	 * When key released
	 * 
	 * @param ev KeyEvent
	 */
	public void keyReleased(KeyEvent ev) {
		isShift = false;
		isAlt = false;
		isCtrl = false;
	}

}