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
package com.cubrid.cubridmigration.core.dbobject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * Schema
 * 
 * @author moulinwang
 * @version 1.0 - 2009-9-15 created by moulinwang
 * @version 2.0 - 2011-11-04 fulei
 */
public class Schema implements
		Serializable {

	private static final long serialVersionUID = -2968882292650432179L;
	private Catalog catalog;
	private String name;
	private List<Table> tables = new ArrayList<Table>();
	private List<View> views = new ArrayList<View>();
	private List<Procedure> procedures = new ArrayList<Procedure>();
	private List<Function> functions = new ArrayList<Function>();
	private List<Trigger> triggers = new ArrayList<Trigger>();
	private List<Sequence> sequenceList = new ArrayList<Sequence>();

	public Schema() {
		//do nothing
	}

	public Schema(Catalog catalog) {
		this.catalog = catalog;
	}

	public Catalog getCatalog() {
		return catalog;
	}

	public void setCatalog(Catalog catalog) {
		this.catalog = catalog;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Table> getTables() {
		return tables;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	public List<View> getViews() {
		return views;
	}

	/**
	 * Set views of schema
	 * 
	 * @param views views
	 */
	public void setViews(List<View> views) {
		this.views = views;
	}

	/**
	 * add table
	 * 
	 * @param table Table
	 */
	public void addTable(Table table) {
		if (table == null) {
			return;
		}
		if (tables == null) {
			tables = new ArrayList<Table>();
		}
		tables.add(table);
		table.setSchema(this);
	}

	/**
	 * add view
	 * 
	 * @param view View
	 */
	public void addView(View view) {
		if (views == null) {
			views = new ArrayList<View>();
		}

		views.add(view);
		view.setSchema(this);
	}

	/**
	 * get table by table's name
	 * 
	 * @param tableName String
	 * @return Table
	 */
	public Table getTableByName(String tableName) {
		for (Table tbl : tables) {
			if (tbl.getName().equals(tableName)) {
				return tbl;
			}
		}
		return null;
	}

	/**
	 * getViewByName
	 * 
	 * @param viewName String
	 * @return View
	 */
	public View getViewByName(String viewName) {
		for (View view : views) {
			if (viewName.equals(view.getName())) {
				return view;
			}

		}
		return null;
	}

	/**
	 * getSequenceByName
	 * 
	 * @param sequenceName String
	 * @return Sequence
	 */
	public Sequence getSequenceByName(String sequenceName) {
		for (Sequence sequence : sequenceList) {
			if (sequenceName.equals(sequence.getName())) {
				return sequence;
			}

		}
		return null;
	}

	public List<Procedure> getProcedures() {
		return procedures;
	}

	public void setProcedures(List<Procedure> procedures) {
		this.procedures = procedures;
	}

	public List<Function> getFunctions() {
		return functions;
	}

	public void setFunctions(List<Function> functions) {
		this.functions = functions;
	}

	public List<Trigger> getTriggers() {
		return triggers;
	}

	public void setTriggers(List<Trigger> triggers) {
		this.triggers = triggers;
	}

	public List<Sequence> getSequenceList() {
		return sequenceList;
	}

	/**
	 * Add sequence into schema object.
	 * 
	 * @param sq Sequence
	 */
	public void addSequence(Sequence sq) {
		if (sq == null) {
			return;
		}
		if (sequenceList == null) {
			sequenceList = new ArrayList<Sequence>();
		}
		if (!sequenceList.contains(sq)) {
			sequenceList.add(sq);
			sq.setOwner(getName());
		}
	}

	public void setSequenceList(List<Sequence> sequenceList) {
		this.sequenceList = sequenceList;
	}

	/**
	 * return hash code
	 * 
	 * @return int
	 */
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	/**
	 * return equal flag
	 * 
	 * @param obj Object
	 * @return boolean
	 */
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		if (getClass() != obj.getClass()) {
			return false;
		}

		Schema other = (Schema) obj;

		if (catalog == null) {
			if (other.catalog != null) {
				return false;
			}
		} else if (!catalog.equals(other.catalog)) {
			return false;
		}

		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}

		return true;
	}

	/**
	 * Retrieves the function by name
	 * 
	 * @param name function name
	 * @return Function definition
	 */
	public Function getFunction(String name) {
		for (Function func : functions) {
			if (func.getName().equals(name)) {
				return func;
			}
		}
		return null;
	}

	/**
	 * Retrieves the procedure by name
	 * 
	 * @param name Procedure name
	 * @return Procedure definition
	 */
	public Procedure getProcedure(String name) {
		for (Procedure func : procedures) {
			if (func.getName().equals(name)) {
				return func;
			}
		}
		return null;
	}

	/**
	 * Retrieves the Trigger by name
	 * 
	 * @param name Trigger name
	 * @return Trigger definition
	 */
	public Trigger getTrigger(String name) {
		for (Trigger func : triggers) {
			if (func.getName().equals(name)) {
				return func;
			}
		}
		return null;
	}
}
