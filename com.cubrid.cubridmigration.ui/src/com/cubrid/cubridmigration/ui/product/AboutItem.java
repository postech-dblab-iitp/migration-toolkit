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
package com.cubrid.cubridmigration.ui.product;

/**
 * 
 * About item can scan the hyperlink
 * 
 * @author moulinwang
 * @version 1.0 - 2009-12-24
 */
public class AboutItem {

	private final String text;
	private final int[][] linkRanges;
	private final String[] hrefs;

	/**
	 * The constructor
	 * 
	 * @param text
	 * @param linkRanges
	 * @param hrefs
	 */
	public AboutItem(String text, int[][] linkRanges, String[] hrefs) {
		this.text = text;
		if (linkRanges == null || linkRanges.length <= 0) {
			this.linkRanges = null;
		} else {
			this.linkRanges = new int[linkRanges.length][];
			for (int i = 0; i < linkRanges.length; i++) {
				this.linkRanges[i] = (int[]) linkRanges[i].clone();
			}
		}
		this.hrefs = hrefs == null ? null : (String[]) hrefs.clone();
	}

	/**
	 * Returns the link ranges (character locations)
	 * 
	 * @return the link ranges
	 */
	public int[][] getLinkRanges() {
		if (linkRanges == null || linkRanges.length <= 0) {
			return new int[][]{};
		} else {
			int copy[][] = new int[linkRanges.length][];
			for (int i = 0; i < linkRanges.length; i++) {
				copy[i] = (int[]) linkRanges[i].clone();
			}
			return copy;
		}
	}

	/**
	 * Returns the text to display
	 * 
	 * @return the text
	 */
	public String getText() {
		return text;
	}

	/**
	 * Returns true if a link is present at the given character location
	 * 
	 * @param offset the offset
	 * @return <code>true</code> if has link;<code>false</code>otherwise
	 */
	public boolean isLinkAt(int offset) {
		// Check if there is a link at the offset
		for (int i = 0; i < linkRanges.length; i++) {
			if (offset >= linkRanges[i][0]
					&& offset < linkRanges[i][0] + linkRanges[i][1]) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns the link at the given offset (if there is one), otherwise returns
	 * <code>null</code>.
	 * 
	 * @param offset the offset
	 * @return the hyperlink string
	 */
	public String getLinkAt(int offset) {
		// Check if there is a link at the offset
		for (int i = 0; i < linkRanges.length; i++) {
			if (offset >= linkRanges[i][0]
					&& offset < linkRanges[i][0] + linkRanges[i][1]) {
				return hrefs[i];
			}
		}
		return null;
	}
}
