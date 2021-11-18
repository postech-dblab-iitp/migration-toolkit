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
package com.cubrid.cubridmigration.core.mapping.model;

import java.util.ArrayList;
import java.util.List;

import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;

/**
 * 
 * MapItem Description
 * 
 * @author Kevin.Wang
 * @version 1.0 - 2011-11-29 created by Kevin.Wang
 */
public final class MapItem implements
		Cloneable {

	private MapObject source;
	private MapObject target;
	private AbstractDataTypeMappingHelper mappingHelper;

	private List<MapObject> availableTargetList = new ArrayList<MapObject>();

	public MapItem(AbstractDataTypeMappingHelper mappingHelper) {
		this.mappingHelper = mappingHelper;
	}

	/**
	 * the constructor
	 * 
	 * @param mappingHelper
	 * @param source
	 * @param target
	 */
	public MapItem(AbstractDataTypeMappingHelper mappingHelper,
			MapObject source, MapObject target) {
		this.mappingHelper = mappingHelper;
		this.source = source;
		this.target = target;
	}

	/**
	 * get the source object
	 * 
	 * @return source MapObject
	 */
	public MapObject getSource() {
		return source;
	}

	/**
	 * set the source object
	 * 
	 * @param sourceItem MapObject
	 */
	public void setSource(MapObject sourceItem) {
		this.source = sourceItem;
	}

	/**
	 * get AvailableTargetList
	 * 
	 * @return availableTargetList List<MapObject>
	 */
	public List<MapObject> getAvailableTargetList() {
		return availableTargetList;
	}

	/**
	 * set AvailableTargetList
	 * 
	 * @param targetItems List<MapObject>
	 */
	public void setAvailableTargetList(List<MapObject> targetItems) {
		this.availableTargetList = targetItems;
	}

	/**
	 * get first target
	 * 
	 * @return first target MapObject
	 */
	public MapObject getFirstTarget() {
		if (!availableTargetList.isEmpty()) {
			return availableTargetList.get(0);
		}

		return null;
	}

	/**
	 * get the target object
	 * 
	 * @return target MapObject
	 */
	public MapObject getTarget() {
		return target;
	}

	/**
	 * 
	 * set the target object
	 * 
	 * @param target MapObject
	 */
	public void setTarget(MapObject target) {
		this.target = target;
	}

	/**
	 * get the mappingHelper
	 * 
	 * @return mappingHelper AbstractDataTypeMappingHelper
	 */
	public AbstractDataTypeMappingHelper getMappingHelper() {
		return mappingHelper;
	}

	/**
	 * set the mapping helper
	 * 
	 * @param mappingHelper AbstractDataTypeMappingHelper
	 */
	public void setMappingHelper(AbstractDataTypeMappingHelper mappingHelper) {
		this.mappingHelper = mappingHelper;
	}

	/**
	 * Clone
	 * 
	 * @return MapItem
	 */
	public MapItem clone() {
		MapItem item = new MapItem(mappingHelper, source.clone(),
				target.clone());

		if (availableTargetList != null) {
			for (MapObject object : availableTargetList) {
				item.getAvailableTargetList().add(object.clone());
			}
		}
		return item;
	}
}
