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
package com.cubrid.cubridmigration.ui.common;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.Closer;
import com.cubrid.cubridmigration.core.common.log.LogUtil;
import com.cubrid.cubridmigration.core.common.xml.IXMLMemento;
import com.cubrid.cubridmigration.core.common.xml.XMLMemento;

/**
 * 
 * This util class is responsible to connect some urls
 * 
 * @author pangqiren
 */
public final class UrlConnUtils {

	public static final String CHECK_NEW_INFO_URL_KO = "http://www.cubrid.com/news.htm";
	public static final String CHECK_NEW_INFO_URL_EN = "http://www.cubrid.org/news.php";
	public static final String CHECK_NEW_VERSION_URL = "http://www.cubrid.com/check_version.cub";
	private static final Logger LOGGER = LogUtil.getLogger(UrlConnUtils.class);

	private static final int TIME_OUT_MILL = 2000;

	/**
	 * The constructor
	 */
	private UrlConnUtils() {
		//empty
	}

	/**
	 * 
	 * Return whether the url can be connected
	 * 
	 * @param url the url str
	 * @return <code>true</code> if the url exist;<code>false</code>otherwise
	 */
	public static boolean isUrlExist(String url) {
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("HEAD");
			conn.setConnectTimeout(TIME_OUT_MILL);
			conn.setReadTimeout(TIME_OUT_MILL);
			return conn.getResponseCode() == HttpURLConnection.HTTP_OK;
		} catch (Exception ignored) {
			LOGGER.error(ignored);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return false;
	}

	//	/**
	//	 * 
	//	 * Get Url Content
	//	 * 
	//	 * @param urlStr the url string
	//	 * @return string the page content
	//	 */
	//	public static String getContent(String urlStr) {
	//		HttpURLConnection conn = null;
	//		BufferedReader in = null;
	//		try {
	//			URL url = new URL(urlStr);
	//			conn = (HttpURLConnection) url.openConnection();
	//			conn.setRequestProperty("Http-User-Agent", "CUBRID-MIGRATION");
	//			conn.setConnectTimeout(TIME_OUT_MILL);
	//			conn.setReadTimeout(TIME_OUT_MILL);
	//			in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	//			StringBuffer sb = new StringBuffer();
	//			String inputLine;
	//			while ((inputLine = in.readLine()) != null) {
	//				sb.append(inputLine);
	//				sb.append("\n");
	//			}
	//			return sb.toString();
	//		} catch (Exception ignored) {
	//			LOGGER.error(ignored);
	//		} finally {
	//			if (in != null) {
	//				try {
	//					in.close();
	//				} catch (IOException ignored) {
	//					LOGGER.error(ignored);
	//				}
	//			}
	//			if (conn != null) {
	//				conn.disconnect();
	//			}
	//		}
	//		return "";
	//	}

	/**
	 * 
	 * Get Url Content
	 * 
	 * @param urlStr the url string
	 * @param userAgent String
	 * @return string the page content
	 */
	protected static String getContent(String urlStr, String userAgent) {
		try {
			URL url = new URL(urlStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestProperty("Http-User-Agent", userAgent);
			conn.setConnectTimeout(TIME_OUT_MILL);
			conn.setReadTimeout(TIME_OUT_MILL);
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			try {
				StringBuffer sb = new StringBuffer();
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					sb.append(inputLine);
					sb.append("\n");
				}
				return sb.toString();
			} finally {
				Closer.close(in);
				conn.disconnect();
			}
		} catch (Exception ignored) {
			LOGGER.error(ignored);
		}
		return "";
	}

	/**
	 * 
	 * Return whether CUBRID new version exist
	 * 
	 * @param localVersion String
	 * @param userAgent String
	 * @return <code>true</code> if new cubrid version exist;<code>false</code>
	 *         otherwise
	 */
	public static boolean isExistNewCubridVersion(String localVersion, String userAgent) {
		if (!isUrlExist(CHECK_NEW_VERSION_URL)) {
			return false;
		}
		String content = getContent(CHECK_NEW_VERSION_URL, userAgent);
		if (StringUtils.isBlank(content)) {
			return false;
		}
		content = content.toUpperCase(Locale.getDefault());
		if (content.indexOf("<HTML") >= 0) {
			content = content.substring(content.indexOf("<HTML"));
		}
		ByteArrayInputStream in = null;
		try {
			in = new ByteArrayInputStream(content.getBytes("UTF-8"));
			IXMLMemento memento = XMLMemento.loadMemento(in);
			if (memento == null) {
				return false;
			}
			IXMLMemento[] children = memento.getChildren("BODY");
			if (children != null && children.length == 1) {
				content = children[0].getTextData();
			}
			return compareVersion(content, localVersion);
		} catch (UnsupportedEncodingException e) {
			LOGGER.error(e);
			return false;
		}
	}

	/**
	 * Compare the version
	 * 
	 * @param serverVersion the server version
	 * @param localVersion the local version
	 * @return <code>true</code> if new cubrid version exist;<code>false</code>
	 *         otherwise
	 */
	protected static boolean compareVersion(String serverVersion, String localVersion) {
		if (!isCubridVersionString(serverVersion)) {
			return false;
		}
		if (!isCubridVersionString(localVersion)) {
			return false;
		}
		String[] latestBuildIdArr = serverVersion.trim().split("\\.");
		String[] localBuildIdArr = localVersion.split("\\.");
		for (int i = 0; i < localBuildIdArr.length && i < latestBuildIdArr.length; i++) {
			int localBuildId = Integer.parseInt(localBuildIdArr[i]);
			int latestBuildId = Integer.parseInt(latestBuildIdArr[i]);
			if (latestBuildId > localBuildId) {
				return true;
			} else if (latestBuildId < localBuildId) {
				return false;
			}
		}
		return false;
	}

	/**
	 * @param versionStr version string
	 * @return true if it is like 8.1.0
	 */
	protected static boolean isCubridVersionString(String versionStr) {
		return StringUtils.isNotBlank(versionStr)
				&& versionStr.trim().matches("^(\\d+\\.){3}\\d+$");
	}
}
