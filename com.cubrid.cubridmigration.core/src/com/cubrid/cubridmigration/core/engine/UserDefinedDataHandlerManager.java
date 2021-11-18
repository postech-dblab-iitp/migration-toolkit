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
package com.cubrid.cubridmigration.core.engine;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.PathUtils;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.engine.exception.UserDefinedHandlerException;

/**
 * UserDefinedDataHandlerManager
 * 
 * @author Kevin Cao
 * @version 1.0 - 2013-7-16 created by Kevin Cao
 */
public final class UserDefinedDataHandlerManager {
	private static final String METHOD_CONVERT = "convert";

	private static final Logger LOG = LogUtil.getLogger(UserDefinedDataHandlerManager.class);

	private final static UserDefinedDataHandlerManager MANAGER = new UserDefinedDataHandlerManager();

	private final Map<String, Object> handlerMap = new HashMap<String, Object>();

	private UserDefinedDataHandlerManager() {
		//
	}

	/**
	 * Single-ton pattern
	 * 
	 * @return UserDefinedDataHandlerManager
	 */
	public static UserDefinedDataHandlerManager getInstance() {
		return MANAGER;
	}

	/**
	 * Put data handler string to manager: the format must be
	 * "xxx.jar:ClassName"
	 * 
	 * @param dataHandler data handler string
	 * @param replace if true, the new value will replace the old value
	 * @return true if put successfully
	 */
	public boolean putColumnDataHandler(String dataHandler, boolean replace) {
		if (StringUtils.isBlank(dataHandler)) {
			return false;
		}
		try {
			if (!replace && getColumnDataHandler(dataHandler) != null) {
				return true;
			}
			final String[] split = dataHandler.split(":");
			String handlerClass, handlerJar, handlerFullPath;
			if (split.length == 2) {
				handlerJar = split[0];
				handlerClass = split[1];
			} else {
				handlerJar = split[0];
				handlerClass = "";
			}
			handlerFullPath = PathUtils.getHandlersDir() + handlerJar;

			final File fileFull = new File(handlerFullPath);
			URLClassLoader loader = new URLClassLoader(
					new URL[]{fileFull.toURI().toURL() });
			Object model = loader.loadClass(handlerClass).newInstance();
			if (getHandlerMethod(model) != null) {
				handlerMap.put(dataHandler, model);
			}
			return true;
		} catch (Exception e) {
			LOG.error("", e);
		}
		return false;
	}

	/**
	 * Get the handler by handler string
	 * 
	 * @param dataHandler handler string
	 * @return Object's instance with method "public Object convert(Object)"
	 */
	public Object getColumnDataHandler(String dataHandler) {
		return handlerMap.get(dataHandler);
	}

	/**
	 * 
	 * Convert data with data handler.
	 * 
	 * @param handler Object's instance with method
	 *        "public Object convert(Object)"
	 * @param recordMap the hole record: key is the column name and the value is
	 *        the column's data.
	 * @param columnName the column name to be converted
	 * @return converted data
	 */
	public Object handleColumnData(Object handler,
			Map<String, Object> recordMap, String columnName) {
		try {
			final Method method = getHandlerMethod(handler);
			return method.invoke(handler, recordMap, columnName);
		} catch (Exception e) {
			throw new UserDefinedHandlerException(
					"Can't convert data by user defined data handler.", e);
		}
	}

	/**
	 * Retrieves the handler method
	 * 
	 * @param handler user defined data handler
	 * @return The method
	 * @throws NoSuchMethodException ex
	 */
	private Method getHandlerMethod(Object handler) throws NoSuchMethodException {
		return handler.getClass().getMethod(METHOD_CONVERT, Map.class,
				String.class);
	}
}
