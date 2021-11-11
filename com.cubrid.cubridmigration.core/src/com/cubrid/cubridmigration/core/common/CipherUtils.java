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
package com.cubrid.cubridmigration.core.common;

import java.security.SecureRandom;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cubrid.cubridmigration.core.common.log.LogUtil;

/**
 * 
 * Cipher utility class
 * 
 * @author pangqiren
 * @version 1.0 - 2011-3-1 created by pangqiren
 */
public final class CipherUtils {
	private CipherUtils() {
	}

	public static final Logger LOG = LogUtil.getLogger(CipherUtils.class);

	private static final int[] KEY = {1, 2, 3, 4 };
	private static final boolean IS_CPU_ENDIAN_LE;

	static {
		String val = System.getProperty("sun.cpu.endian");
		if ("little".equals(val)) {
			IS_CPU_ENDIAN_LE = true;
		} else if ("big".equals(val)) {
			IS_CPU_ENDIAN_LE = false;
		} else {
			throw new RuntimeException("Unknown CPU endian");
		}
	}

	/**
	 * 
	 * Encrypt the text
	 * 
	 * @param text String
	 * @return String
	 */
	public static String encrypt(String text) {
		if (StringUtils.isEmpty(text)) {
			return "";
		}
		String encStr = text;
		byte[] data = encStr.getBytes();
		int length = data.length;

		int newLength = (length + 31) / 32 * 32;
		byte[] buf = new byte[newLength];

		System.arraycopy(data, 0, buf, 0, length);

		buf[length] = 0;
		Random random = new SecureRandom();
		for (int i = length + 1; i < 32; i++) {
			buf[i] = (byte) random.nextInt(100);
		}

		int[] buf2 = toIntArray(buf);
		for (int i = 0; i < buf2.length; i += 2) {
			encrypt(KEY, buf2, i);
		}

		byte[] buf3 = fromIntArray(buf2);
		StringBuffer encStrBuf = new StringBuffer();
		for (int i = 0; i < buf3.length; i++) {
			byte value = buf3[i];
			String hexStr = Integer.toHexString(value);
			int hexLength = hexStr.length();
			if (hexLength == 1) {
				encStrBuf.append("0").append(hexStr.charAt(0));
			} else {
				encStrBuf.append(hexStr.charAt(hexLength - 2)).append(
						hexStr.charAt(hexLength - 1));
			}
		}
		return encStrBuf.toString();
	}

	/**
	 * 
	 * Encrypt
	 * 
	 * @param kk int[]
	 * @param text int[]
	 * @param offset int
	 */
	private static void encrypt(int[] kk, int[] text, int offset) {
		int y = bigEndian(text[offset + 0]);
		int z = bigEndian(text[offset + 1]);
		int delta = 0x9e3779b9;
		int sum = 0;
		int n;

		for (n = 0; n < 32; n++) {
			sum += delta;
			y += ((z << 4) + kk[0]) ^ (z + sum) ^ ((z >>> 5) + kk[1]);
			z += ((y << 4) + kk[2]) ^ (y + sum) ^ ((y >>> 5) + kk[3]);
		}
		text[offset + 0] = bigEndian(y);
		text[offset + 1] = bigEndian(z);
	}

	/**
	 * decrypt user password
	 * 
	 * @param text text
	 * @return password
	 */
	public static String decrypt(String text) {

		if (StringUtils.isBlank(text)) {
			return text;
		}

		byte[] data = text.getBytes();
		int align = (data.length + 15) / 16 * 16;
		data = copyOf(data, align);

		byte[] buf = new byte[data.length / 2];

		for (int i = 0; i < data.length; i += 2) {
			buf[i / 2] = getHex(data, i);
		}

		int[] buf2 = toIntArray(buf);

		for (int i = 0; i < buf2.length; i += 2) {
			decrypt(KEY, buf2, i);
		}

		buf = fromIntArray(buf2);

		for (int i = 0; i < buf.length; i++) {
			if (buf[i] == 0) {
				return new String(copyOf(buf, i));
			}
		}

		return new String(buf);
	}

	/**
	 * byte array to hex
	 * 
	 * @param data data
	 * @param offset offset
	 * @return hex
	 */
	private static byte getHex(byte[] data, int offset) {

		byte b = data[offset];

		if ('0' <= b && b <= '9') {
			b -= '0';
		} else if ('a' <= b && b <= 'f') {
			b -= 'a';
			b += 10;
		} else if ('A' <= b && b <= 'F') {
			b -= 'A';
			b += 10;
		} else {
			b = 0;
		}

		byte res = (byte) (b << 4);

		b = data[offset + 1];

		if ('0' <= b && b <= '9') {
			b -= '0';
		} else if ('a' <= b && b <= 'f') {
			b -= 'a';
			b += 10;
		} else if ('A' <= b && b <= 'F') {
			b -= 'A';
			b += 10;
		} else {
			b = 0;
		}

		res |= b;
		return res;
	}

	/**
	 * byte array to integer array
	 * 
	 * @param data byte array
	 * @return integer array
	 */
	private static int[] toIntArray(byte[] data) {
		int len = (data.length + 3) / 4 * 4;
		byte[] tmp;
		if (data.length < len) {
			tmp = copyOf(data, len);
		} else {
			tmp = data;
		}

		int[] res = new int[len / 4];
		for (int i = 0; i < len; i += 4) {
			int n = 0;
			if (IS_CPU_ENDIAN_LE) {
				n |= (tmp[i + 0] << 0) & 0x000000ff;
				n |= (tmp[i + 1] << 8) & 0x0000ff00;
				n |= (tmp[i + 2] << 16) & 0x00ff0000;
				n |= (tmp[i + 3] << 24) & 0xff000000;
			} else {
				n |= (tmp[i + 0] << 24) & 0xff000000;
				n |= (tmp[i + 1] << 16) & 0x00ff0000;
				n |= (tmp[i + 2] << 8) & 0x0000ff00;
				n |= (tmp[i + 3] << 0) & 0x000000ff;
			}

			res[i / 4] = n;
		}

		return res;
	}

	/**
	 * integer array to byte array
	 * 
	 * @param data int array
	 * @return byte array
	 */
	private static byte[] fromIntArray(int[] data) {
		byte[] res = new byte[data.length * 4];

		for (int i = 0; i < data.length; i++) {
			int n = data[i];
			if (IS_CPU_ENDIAN_LE) {
				res[i * 4 + 0] = (byte) ((n >> 0) & 0xff);
				res[i * 4 + 1] = (byte) ((n >> 8) & 0xff);
				res[i * 4 + 2] = (byte) ((n >> 16) & 0xff);
				res[i * 4 + 3] = (byte) ((n >> 24) & 0xff);
			} else {
				res[i * 4 + 0] = (byte) ((n >> 24) & 0xff);
				res[i * 4 + 1] = (byte) ((n >> 16) & 0xff);
				res[i * 4 + 2] = (byte) ((n >> 8) & 0xff);
				res[i * 4 + 3] = (byte) ((n >> 0) & 0xff);
			}
		}

		return res;
	}

	/**
	 * endian
	 * 
	 * @param val value
	 * @param le le
	 * @return le
	 */
	private static int endian(int val, boolean le) {
		if (le) {
			int res = 0;
			res |= ((val & 0x000000FF) << 24) & 0xFF000000;
			res |= ((val & 0x0000FF00) << 8) & 0x00FF0000;
			res |= ((val & 0x00FF0000) >> 8) & 0x0000FF00;
			res |= ((val & 0xFF000000) >> 24) & 0x000000FF;
			return res;
		} else {
			return val;
		}
	}

	/**
	 * beg endian
	 * 
	 * @param val val
	 * @return endian
	 */
	private static int bigEndian(int val) {
		return endian(val, IS_CPU_ENDIAN_LE);
	}

	/**
	 * decrypt
	 * 
	 * @param kk k
	 * @param text text
	 * @param offset offset
	 */
	private static void decrypt(int[] kk, int[] text, int offset) {

		int y = bigEndian(text[offset + 0]);
		int z = bigEndian(text[offset + 1]);

		int delta = 0x9e3779b9;
		int sum = delta << 5;
		int n;

		for (n = 0; n < 32; n++) {
			z -= ((y << 4) + kk[2]) ^ (y + sum) ^ ((y >>> 5) + kk[3]);
			y -= ((z << 4) + kk[0]) ^ (z + sum) ^ ((z >>> 5) + kk[1]);
			sum -= delta;
		}
		text[offset + 0] = bigEndian(y);
		text[offset + 1] = bigEndian(z);
	}

	/**
	 * Implementation of JDK 1.6 Arrays.copyOf() method
	 * 
	 * @param original original byte array
	 * @param newLength new length
	 * @return new allocated byte array
	 */
	private static byte[] copyOf(byte[] original, int newLength) {
		byte[] copy = new byte[newLength];
		System.arraycopy(original, 0, copy, 0,
				Math.min(original.length, newLength));
		return copy;
	}
}
