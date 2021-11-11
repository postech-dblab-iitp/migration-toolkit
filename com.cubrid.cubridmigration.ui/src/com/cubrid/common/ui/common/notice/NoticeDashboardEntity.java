/*
 * Copyright (C) 2013 Search Solution Corporation. All rights reserved by Search
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
package com.cubrid.common.ui.common.notice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.eclipse.core.runtime.Platform;

import com.sun.syndication.feed.synd.SyndCategory;
import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

/**
 * CUBRID Manager dashboard entity.
 * 
 * @author Tobi
 */
public class NoticeDashboardEntity {
	private static final SimpleDateFormat dateformat = new SimpleDateFormat(
			"yyyy-MM-dd");
	private static final String NOTICE_CONTENT_CACHE_FILE = "notice.cache";

	private String rssurl;
	private SyndFeed rssData;
	private Map<Set<String>, Set<SyndEntry>> contents = new HashMap<Set<String>, Set<SyndEntry>>();
	private Date cacheDate = new Date();

	public NoticeDashboardEntity(String rssurl) {
		this.rssurl = rssurl;
	}

	private static File rssCacheFile;

	static {
		String path = Platform.getInstanceLocation().getURL().getPath();
		rssCacheFile = new File(path, NOTICE_CONTENT_CACHE_FILE);
	}

	/**
	 * Refresh data form RSS.
	 * 
	 * @throws Exception
	 */
	public int refresh() {
		// success
		int statusCode = RssStatusCode.SUCCESS;
		try {
			readFeedFromRemote();
			organizeContentByCategory();
		} catch (IllegalArgumentException e) {
			statusCode = RssStatusCode.FAILED_ERROR_FORMAT;
			e.printStackTrace();
		} catch (FeedException e) {
			statusCode = RssStatusCode.FAILED_ERROR_FORMAT;
			e.printStackTrace();
		} catch (IOException e) {
			statusCode = RssStatusCode.FAILED_NETWORK;
			e.printStackTrace();
		}

		if (statusCode != RssStatusCode.SUCCESS) {
			loadRssFromCache();
			if (!contents.isEmpty()) {
				statusCode = RssStatusCode.SUCCESS_GET_FROM_CACHE;
			}
		}

		return statusCode;
	}

	private void saveRssToCache() {
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream(new FileOutputStream(rssCacheFile));
			out.writeObject(new Date());
			out.writeObject(contents);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			close(out);
		}
	}

	@SuppressWarnings("unchecked")
	private void loadRssFromCache() {
		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream(new FileInputStream(rssCacheFile));
			cacheDate = (Date) in.readObject();
			contents = (Map<Set<String>, Set<SyndEntry>>) in.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			close(in);
		}
	}

	/**
	 * get syndFeed from rss
	 * 
	 * @param source
	 * @return
	 * @throws IllegalArgumentException
	 * @throws FeedException
	 * @throws IOException
	 */
	private void readFeedFromRemote() throws IllegalArgumentException,
			FeedException,
			IOException {
		SyndFeedInput input = new SyndFeedInput();
		// Locale.setDefault(Locale.ENGLISH);
		URLConnection feedUrl = new URL(rssurl).openConnection();
		// java.io.IOException: Server returned HTTP response code: 403
		feedUrl.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)");
		feedUrl.setConnectTimeout(5000);

		XmlReader xmlReader = new XmlReader(feedUrl);
		rssData = input.build(xmlReader);
	}

	@SuppressWarnings("unused")
	private void readFeedFromLocalCache() throws IllegalArgumentException,
			FeedException,
			IOException {
		String file = "";
		SyndFeedInput input = new SyndFeedInput();
		File feedUrl = new File(file);
		rssData = input.build(new XmlReader(feedUrl));
	}

	private void organizeContentByCategory() {
		if (rssData != null) {
			// Get RSS item entities
			@SuppressWarnings("unchecked")
			List<SyndEntry> entries = rssData.getEntries();
			for (SyndEntry entry : entries) {
				// category: language, type, client
				@SuppressWarnings("unchecked")
				List<SyndCategory> categoryList = entry.getCategories();
				Set<String> categories = new HashSet<String>();
				if (categoryList != null) {
					for (SyndCategory category : categoryList) {
						categories.add(category.getName());
					}
				}
				Set<SyndEntry> syndEntries;
				if (!contents.containsKey(categories)) {
					syndEntries = new HashSet<SyndEntry>();
					contents.put(categories, syndEntries);
				} else {
					syndEntries = contents.get(categories);
				}
				syndEntries.add(entry);
			}
			saveRssToCache();
		}
	}

	/**
	 * Get HTML content by category.
	 * 
	 * @param categories
	 * @return
	 */
	public String getHtmlContent(String... categories) {
		Set<String> categorySet = new HashSet<String>();
		if (categories == null) {
			return "";
		}

		for (String category : categories) {
			if (category != null) {
				categorySet.add(category);
			}
		}

		Set<SyndEntry> syndEntries = new HashSet<SyndEntry>();
		if (contents == null || contents.entrySet() == null) {
			return "";
		}

		for (Entry<Set<String>, Set<SyndEntry>> entity : contents.entrySet()) {
			if (entity.getKey().containsAll(categorySet)) {
				syndEntries.addAll(entity.getValue());
			}
		}

		List<SyndEntry> syndEntryList = new ArrayList<SyndEntry>(syndEntries);
		Collections.sort(syndEntryList, new Comparator<SyndEntry>() {
			public int compare(SyndEntry o1, SyndEntry o2) {
				if (o1 == null || o2 == null || o1.getPublishedDate() == null
						|| o2.getPublishedDate() == null) {
					return 0;
				}
				return o2.getPublishedDate().compareTo(o1.getPublishedDate());
			}
		});

		StringBuilder sb = new StringBuilder();
		sb.append("<p>");
		if (syndEntryList != null && !syndEntryList.isEmpty()) {
			for (SyndEntry entry : syndEntryList) {
				SyndContent description = entry.getDescription();
				sb.append("<li>");
				sb.append("<img href=\"icons/action/host_connect.png\"/>");
				sb.append("<a href=\"").append(entry.getLink()).append("\">");
				sb.append(entry.getTitle());
				sb.append("</a>");
				sb.append("</li>");
				if (!isEmpty(description.getValue())) {
					sb.append("<p>");
					sb.append(description.getValue().trim());
					sb.append("</p>");
				}
			}
		} else {
			sb.append("<p>No data.</p>");
		}
		sb.append("</p>");
		return sb.toString();
	}

	public String getCacheDate() {
		return cacheDate != null ? dateformat.format(cacheDate) : "";
	}

	public static void close(InputStream is) {
		try {
			if (is != null) {
				is.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void close(OutputStream os) {
		try {
			if (os != null) {
				os.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * return true value whether string is empty.
	 * 
	 * @param string String The source string
	 * @return boolean
	 */
	public static boolean isEmpty(String string) {

		return string == null || string.trim().length() == 0;

	}
}
