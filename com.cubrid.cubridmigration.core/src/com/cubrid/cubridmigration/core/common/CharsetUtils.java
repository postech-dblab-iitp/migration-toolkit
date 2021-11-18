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
package com.cubrid.cubridmigration.core.common;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;

/**
 * charset util
 * 
 * @author moulinwang
 * 
 */
public class CharsetUtils { //NOPMD	
	private static final Logger LOG = LogUtil.getLogger(CharsetUtils.class);

	private final static Map<String, Integer> CHARSET_SIZE_MAP = new HashMap<String, Integer>();
	static {
		Properties prop = new Properties();
		try {
			//Load from configuration file
			InputStream resource = CharsetUtils.class.getResourceAsStream("/com/cubrid/cubridmigration/core/common/Charsetprops.properties");
			try {
				prop.load(resource);
				for (Map.Entry<Object, Object> entry : prop.entrySet()) {
					CHARSET_SIZE_MAP.put(entry.getKey().toString(),
							Integer.parseInt(entry.getValue().toString()));
				}
			} finally {
				if (resource != null) {
					resource.close();
				}
			}
		} catch (Exception e) {
			CHARSET_SIZE_MAP.clear();
			CHARSET_SIZE_MAP.put("iso8859-1", 1);
			CHARSET_SIZE_MAP.put("iso8859-2", 1);
			CHARSET_SIZE_MAP.put("iso8859-3", 1);
			CHARSET_SIZE_MAP.put("iso8859-4", 1);
			CHARSET_SIZE_MAP.put("iso8859-5", 1);
			CHARSET_SIZE_MAP.put("iso8859-6", 1);
			CHARSET_SIZE_MAP.put("iso8859-7", 1);
			CHARSET_SIZE_MAP.put("iso8859-8", 1);
			CHARSET_SIZE_MAP.put("iso8859-9", 1);
			CHARSET_SIZE_MAP.put("iso8859-13", 1);
			CHARSET_SIZE_MAP.put("iso8859-15", 1);
			CHARSET_SIZE_MAP.put("latin1", 1);
			CHARSET_SIZE_MAP.put("ascii", 1);

			CHARSET_SIZE_MAP.put("al32utf8", 4);
			CHARSET_SIZE_MAP.put("utf-8", 3);
			CHARSET_SIZE_MAP.put("iso-8859-1", 1);
			CHARSET_SIZE_MAP.put("euc-kr", 2);
			CHARSET_SIZE_MAP.put("euc-jp", 2);
			CHARSET_SIZE_MAP.put("gb2312", 2);
			CHARSET_SIZE_MAP.put("gbk", 2);
			CHARSET_SIZE_MAP.put("zhs16gbk", 2);
			LOG.error(e);
		}
	}

	/**
	 * return java charset byte length
	 * 
	 * @param javaCharsetName String
	 * @return int
	 */
	public static int getCharsetByte(String javaCharsetName) {
		if (StringUtils.isBlank(javaCharsetName)) {
			return 3;
		}
		String chs = javaCharsetName.trim().toLowerCase();
		if (CHARSET_SIZE_MAP.containsKey(chs)) {
			return CHARSET_SIZE_MAP.get(chs);
		} else {
			LOG.error("Can't find the charset:\t" + chs);
		}
		return 2;
	}

	//	/**
	//	 * Retrieves if the size should be change when the char-set has been
	//	 * changed.
	//	 * 
	//	 * @param oldCharset the old charset to be changed.
	//	 * @param newCharset the new charset.
	//	 * @return if size should be changed,return true;
	//	 */
	//	public static boolean charSizeChange(String oldCharset, String newCharset) {
	//		return CHARSET_SIZE_MAP.get(oldCharset) == CHARSET_SIZE_MAP.get(newCharset);
	//	}

	/**
	 * getCharsets
	 * 
	 * @return String[]
	 */
	public static String[] getCharsets() {
		final List<String> list = new ArrayList<String>(8);
		list.add("");
		list.add("UTF-8");
		list.add("ISO-8859-1");
		list.add("EUC-KR");
		list.add("EUC-JP");
		list.add("GB2312");
		list.add("GBK");

		if (!list.contains(CUBRIDIOUtils.DEFAULT_CHARSET)) {
			list.add(2, CUBRIDIOUtils.DEFAULT_CHARSET);
		}

		return list.toArray(new String[list.size()]);
	}

	private final static String[][] CHARSETS = new String[][]{
			{"ASCII", "ASCII" }, {"ISO8859P1", "ISO8859-1" },
			{"ISO8859P2", "ISO8859-2" }, {"ISO8859P3", "ISO8859-3" },
			{"ISO8859P4", "ISO8859-4" }, {"ISO8859P5", "ISO8859-5" },
			{"ISO8859P6", "ISO8859-6" }, {"ISO8859P7", "ISO8859-7" },
			{"ISO8859P8", "ISO8859-8" }, {"ISO8859P9", "ISO8859-9" },
			{"ISO8859P13", "ISO8859-13" }, {"ISO8859P15", "ISO8859-15" },
			{"UTF8", "UTF8" }, {"UTF16", "UTF16" }, {"GBK", "GBK" },
			{"GB2312", "GB2312" } };

	/**
	 * Turn the oracle char set string into the java supported char set string
	 * 
	 * @param cs oracle char set string
	 * @return java supported char set string such as ASCII, UTF8 and etc
	 */
	public static String turnOracleCharset2Normal(String cs) {
		for (int i = 0; i < CHARSETS.length; i++) {
			if (cs.indexOf(CHARSETS[i][0]) >= 0) {
				return CHARSETS[i][1];
			}
		}
		return Charset.defaultCharset().name();
	}

	/**
	 * return oracle charset length
	 * 
	 * @param oracleCharset String
	 * @return int
	 */
	public static int getOracleCharsetByte(String oracleCharset) {
		String patternString = "\\w+?(\\d+)\\w.+";
		Pattern pattern = Pattern.compile(patternString);
		Matcher matcher = pattern.matcher(oracleCharset);

		if (matcher.matches()) {
			int g = matcher.groupCount();

			if (g > 0) {
				int b = Integer.parseInt(matcher.group(1));
				return (b + 1) / 8;
			}
		}

		if ("UTF8".equals(oracleCharset) || "UTFE".equals(oracleCharset)) {
			return 3;
		}

		return 2;
	}

}
