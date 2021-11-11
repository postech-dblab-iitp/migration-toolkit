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
package com.cubrid.cubridmigration.core.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

import com.sun.org.apache.xml.internal.utils.XMLChar;

/**
 * a reader to read characters from a xml file but remove invalid characters
 * 
 * @author moulinwang
 * 
 */
@SuppressWarnings("restriction")
public class RmInvalidXMLCharReader extends
		InputStreamReader {

	private IReaderEvent readerEvent;

	private List<Character> invalidateChars;

	public RmInvalidXMLCharReader(InputStream in) {
		super(in);
	}

	public RmInvalidXMLCharReader(InputStream in, Charset cs) {
		super(in, cs);
	}

	public RmInvalidXMLCharReader(InputStream in, CharsetDecoder dec) {
		super(in, dec);
	}

	public RmInvalidXMLCharReader(InputStream in, String charsetName) throws UnsupportedEncodingException {
		super(in, charsetName);
	}

	public RmInvalidXMLCharReader(InputStream in, String charsetName,
			List<Character> invalidateChars) throws UnsupportedEncodingException {
		super(in, charsetName);
		this.invalidateChars = invalidateChars;
	}

	/**
	 * read a char
	 * 
	 * @return int
	 * @throws IOException e
	 */
	public int read() throws IOException {
		int c = super.read();
		if (c == -1) {
			return c;
		} else if (XMLChar.isInvalid(c)) {
			return ' ';
		} else {
			return c;
		}
	}

	/**
	 * Read characters into a portion of an array.
	 * 
	 * @param cbuf Destination buffer
	 * @param offset Offset at which to start storing characters
	 * @param length Maximum number of characters to read
	 * 
	 * @return The number of characters read, or -1 if the end of the stream has
	 *         been reached
	 * 
	 * @exception IOException If an I/O error occurs
	 */
	public int read(char[] cbuf, int offset, int length) throws IOException {
		int count = super.read(cbuf, offset, length);
		if (count == -1) {
			return count;
		}
		for (int i = 0; i < length; i++) {
			char c = cbuf[offset + i];
			if (null == invalidateChars) {
				if (XMLChar.isInvalid(c)) {
					cbuf[offset + i] = ' ';
					continue;
				}
			} else if (XMLChar.isInvalid(c) || c == 0xfffd) {
				invalidateChars.add(c);
				cbuf[offset + i] = 0xfffd;
			}
		}
		if (readerEvent != null) {
			readerEvent.readChars(count);
		}
		return count;
	}

	public void setReaderEvent(IReaderEvent readerEvent) {
		this.readerEvent = readerEvent;
	}
}
