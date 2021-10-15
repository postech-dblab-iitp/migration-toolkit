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
package com.cubrid.cubridmigration.ui.common.navigator.event;

import java.util.EventObject;

import com.cubrid.common.ui.navigator.ICUBRIDNode;

/**
 * 
 * Instances of this class are sent as a result of CUBRID Node changed
 * 
 * @author pangqiren
 * @version 1.0 - 2009-6-4 created by pangqiren
 */
@SuppressWarnings("serial")
public class CubridNodeChangedEvent extends
		EventObject {
	private ICUBRIDNode cubridNode;

	private CubridNodeChangedEventType type;

	/**
	 * The constructor
	 * 
	 * @param source
	 */
	public CubridNodeChangedEvent(Object source) {
		super(source);
	}

	/**
	 * The constructor
	 * 
	 * @param cubridNode
	 * @param type
	 */
	public CubridNodeChangedEvent(ICUBRIDNode cubridNode,
			CubridNodeChangedEventType type) {
		this(cubridNode);
		this.cubridNode = cubridNode;
		this.type = type;
	}

	/**
	 * 
	 * Get CubridNode object
	 * 
	 * @return ICubridNode
	 */
	public ICUBRIDNode getCubridNode() {
		return cubridNode;
	}

	/**
	 * 
	 * Get envent type
	 * 
	 * @return CubridNodeChangedEventType
	 */
	public CubridNodeChangedEventType getType() {
		return type;
	}

}
