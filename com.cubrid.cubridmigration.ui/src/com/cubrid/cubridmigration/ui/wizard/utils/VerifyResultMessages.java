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
package com.cubrid.cubridmigration.ui.wizard.utils;

import org.apache.commons.lang.StringUtils;

/**
 * VerifyResultMessages is used to store the verify result messages.
 * 
 * @author Kevin Cao
 * @version 1.0 - 2011-7-8 created by Kevin Cao
 */
public class VerifyResultMessages {
	private String errorMessage;
	private String warningMessage;
	private String confirmMessage;

	public VerifyResultMessages() {
		//do nothing.
	}

	public VerifyResultMessages(String errorMessage, String warningMessage, String confirmMessage) {
		this.errorMessage = errorMessage;
		this.warningMessage = warningMessage;
		this.confirmMessage = confirmMessage;
	}

	/**
	 * Retrieves that the result has any error message.
	 * 
	 * @return true if has any error message.
	 */
	public boolean hasError() {
		return !StringUtils.isEmpty(errorMessage);
	}

	/**
	 * Retrieves that the result has any warning message.
	 * 
	 * @return true if has any warning message.
	 */
	public boolean hasWarning() {
		return !StringUtils.isEmpty(warningMessage);
	}

	/**
	 * Retrieves that the result has any confirm message.
	 * 
	 * @return true if has any confirm message.
	 */
	public boolean hasConfirm() {
		return !StringUtils.isEmpty(confirmMessage);
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getWarningMessage() {
		return warningMessage;
	}

	public void setWarningMessage(String warningMessage) {
		this.warningMessage = warningMessage;
	}

	public String getConfirmMessage() {
		return confirmMessage;
	}

	public void setConfirmMessage(String confirmMessage) {
		this.confirmMessage = confirmMessage;
	}

}