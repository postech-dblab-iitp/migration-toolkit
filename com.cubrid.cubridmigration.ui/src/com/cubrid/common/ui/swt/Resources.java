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
package com.cubrid.common.ui.swt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.widgets.Display;

public class Resources {

	private static Resources instance = new Resources();
	private HashMap<RGB, Color> colors = new HashMap<RGB, Color>();
	private static HashMap<Integer, Cursor> cursors = new HashMap<Integer, Cursor>();
	
	private Resources() {
	}
	
	public static Resources getInstance() {
		return instance;
	}
	
	public void dispose() {
		disposeColors();
		disposeCursors();
	}
	
	private void disposeColors() {
		for (Entry<RGB, Color> entry : colors.entrySet()) {
			entry.getValue().dispose();
		}
		colors.clear();
	}
	
	private void disposeCursors() {
		for (Entry<Integer, Cursor> entry : cursors.entrySet()) {
			entry.getValue().dispose();
		}
		cursors.clear();
	}
	
	public Color getColor(int colorId) {
		return Display.getCurrent().getSystemColor(colorId);
	}
	
	public Color getColor(int red, int green, int blue) {
		return getColor(new RGB(red, green, blue));
	}
	
	public Color getColor(RGB rgb) {
		Color color = colors.get(rgb);
		if (color == null) {
			Display display = Display.getCurrent();
			color = new Color(display, rgb);
			colors.put(rgb, color);
		}
		return color;
	}
	
	public Cursor getCursor(int id) {
		Integer key = Integer.valueOf(id);
		Cursor cursor = cursors.get(key);
		if (cursor == null) {
			cursor = new Cursor(Display.getDefault(), id);
			cursors.put(key, cursor);
		}
		return cursor;
	}
}
