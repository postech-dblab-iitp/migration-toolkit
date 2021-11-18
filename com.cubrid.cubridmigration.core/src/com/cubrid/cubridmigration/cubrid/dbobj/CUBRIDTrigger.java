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
package com.cubrid.cubridmigration.cubrid.dbobj;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.cubrid.cubridmigration.core.dbobject.Trigger;

/**
 * 
 * CUBRIDTrigger
 * 
 * @author JessieHuang
 * @version 1.0 - 2009-11-13
 */
public class CUBRIDTrigger extends
		Trigger {
	private static final long serialVersionUID = 8862741326422351730L;

	private final static Map<String, String> ACTION_TIME_MAP = new HashMap<String, String>();
	private final static Map<String, String> EVENT_TYPE_MAP = new HashMap<String, String>();
	private final static Map<String, String> ACTION_TYPE_MAP = new HashMap<String, String>();
	private final static Map<String, String> CONDITION_TIME_MAP = new HashMap<String, String>();
	private final static Map<String, String> STATUS_MAP = new HashMap<String, String>();

	static {
		ACTION_TIME_MAP.put("1", "");
		ACTION_TIME_MAP.put("2", "AFTER");
		ACTION_TIME_MAP.put("3", "DEFERRED");

		EVENT_TYPE_MAP.put("0", "UPDATE");
		EVENT_TYPE_MAP.put("1", "STATEMENT UPDATE");
		EVENT_TYPE_MAP.put("2", "DELETE");
		EVENT_TYPE_MAP.put("3", "STATEMENT DELETE");
		EVENT_TYPE_MAP.put("4", "INSERT");
		EVENT_TYPE_MAP.put("5", "STATEMENT INSERT");
		EVENT_TYPE_MAP.put("8", "COMMIT");
		EVENT_TYPE_MAP.put("9", "ROLLBACK");

		ACTION_TYPE_MAP.put("1", "OTHER STATEMENT");
		ACTION_TYPE_MAP.put("2", "REJECT");
		ACTION_TYPE_MAP.put("3", "INVALIDATE TRANSACTION");
		ACTION_TYPE_MAP.put("4", "PRINT");

		CONDITION_TIME_MAP.put("1", "BEFORE");
		CONDITION_TIME_MAP.put("2", "AFTER");
		CONDITION_TIME_MAP.put("3", "DEFERRED");

		STATUS_MAP.put("1", "INACTIVE");
		STATUS_MAP.put("2", "ACTIVE");
	}

	/**
	 * get ActionTime
	 * 
	 * @param time ActionTime
	 * @return String
	 */
	public static String getActionTime(String time) {
		return ACTION_TIME_MAP.containsKey(time) ? ACTION_TIME_MAP.get(time)
				: "";
	}

	/**
	 * get EventType
	 * 
	 * @param event EventType
	 * @return String
	 */
	public static String getEventType(String event) {
		return EVENT_TYPE_MAP.containsKey(event) ? EVENT_TYPE_MAP.get(event)
				: "";
	}

	/**
	 * get ConditionTime
	 * 
	 * @param time ConditionTime
	 * @return String
	 */
	public static String getConditionTime(String time) {
		return CONDITION_TIME_MAP.containsKey(time) ? CONDITION_TIME_MAP.get(time)
				: "";
	}

	/**
	 * get ActionType
	 * 
	 * @param action ActionType
	 * @return String
	 */
	public static String getActionType(String action) {
		return ACTION_TYPE_MAP.containsKey(action) ? ACTION_TYPE_MAP.get(action)
				: "";
	}

	/**
	 * get Status
	 * 
	 * @param status String
	 * @return String
	 */
	public static String getStatus(String status) {
		return STATUS_MAP.containsKey(status) ? STATUS_MAP.get(status) : "";
	}

	// the time to evaluate trigger condition: before,after,deferred
	private String conditionTime;

	// 8 types: insert,update,delete(statement
	// insert,update,delete),commit,rollback
	private String eventType;

	// the class or class attribute on which the trigger operates
	private String targetClass;
	private String targetAttribute;

	// the condition
	private String condition;

	// the time to take action
	private String actionTime;

	// action type: print,reject, invalidate transaction, call statements
	private String actionType;

	// whether the trigger is active: ACTIVE or INACTIVE
	private String status;

	// the trigger order
	private String priority;

	//
	private String actionDefintion;

	public String getConditionTime() {
		return getConditionTime(conditionTime);
	}

	public void setConditionTime(String conditionTime) {
		this.conditionTime = conditionTime;
	}

	public String getEventType() {
		return getEventType(eventType);
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getTargetClass() {
		return targetClass;
	}

	public void setTargetClass(String targetClass) {
		this.targetClass = targetClass;
	}

	public String getTargetAttribute() {
		return targetAttribute;
	}

	public void setTargetAttribute(String targetAttribute) {
		this.targetAttribute = targetAttribute;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public String getActionTime() {
		return getActionTime(actionTime);
	}

	public void setActionTime(String actionTime) {
		this.actionTime = actionTime;
	}

	public String getActionType() {
		return getActionType(actionType);
	}

	public void setActionType(String actionType) {
		this.actionType = actionType;
	}

	public String getActionDefintion() {
		return actionDefintion;
	}

	public void setActionDefintion(String actionDefintion) {
		this.actionDefintion = actionDefintion;
	}

	public String getStatus() {
		return getStatus(status);
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getPriority() {
		return priority;
	}

	/**
	 * set Priority
	 * 
	 * @param priority String
	 */
	public void setPriority(String priority) {
		try {
			Double.parseDouble(priority);
			this.priority = formatPriority(priority);
		} catch (NumberFormatException e) {
			this.priority = priority;
		}
	}

	/**
	 * formatPriority
	 * 
	 * @param priority String
	 * @return String
	 */
	private String formatPriority(String priority) {
		double prior = Double.parseDouble(priority);
		DecimalFormat formatter = new DecimalFormat("##00.00");
		return formatter.format(prior);
	}

	/**
	 * get Trigger DDL
	 * 
	 * @return DDL String
	 */
	public String getDDL() {
		String newLine = "\r\n";
		String endLineChar = ";";
		StringBuffer buf = new StringBuffer();

		//CREATE TRIGGER trigger_name
		buf.append("CREATE TRIGGER ");
		String triggerName = this.getName();

		if (StringUtils.isEmpty(triggerName)) {
			buf.append("<trigger_name>");
		} else {
			buf.append('"').append(triggerName).append('"');
		}

		buf.append(newLine);

		//[ STATUS { ACTIVE | INACTIVE } ]
		String status = this.getStatus();

		if (!"ACTIVE".equals(status)) {
			buf.append("STATUS INACTIVE");
			buf.append(newLine);
		}

		//[ PRIORITY key ]
		String priority = this.getPriority();
		try {
			BigDecimal prior = new BigDecimal(priority);

			if (!prior.equals(new BigDecimal("0.00"))) {
				buf.append("PRIORITY ").append(priority);
				buf.append(newLine);
			}
		} catch (NumberFormatException e) {
			buf.append("PRIORITY ").append(priority);
			buf.append(newLine);
		}

		//event_time event_type [ event_target ]
		StringBuffer ifBF = new StringBuffer();
		StringBuffer execBF = new StringBuffer();

		String conditionTime = this.getConditionTime();
		String eventType = this.getEventType();
		String targetTable = this.getTargetClass();
		String targetColumn = this.getTargetAttribute();

		String condition = this.getCondition();
		String actionTime = this.getActionTime();

		//EXECUTE [ AFTER | DEFERRED ] action [ ; ]
		execBF.append("EXECUTE ");

		if (StringUtils.isEmpty(condition)) {
			if ("AFTER".equals(actionTime) || "DEFERRED".equals(actionTime)) {
				buf.append(actionTime);
			} else {
				buf.append("BEFORE");
			}
		} else {
			buf.append(conditionTime);

			//[ IF condition ]
			ifBF.append("IF ").append(condition);
			ifBF.append(newLine);
			execBF.append(actionTime);
		}

		buf.append(' ').append(eventType).append(' ');

		if (StringUtils.isEmpty(targetTable)) {
			if (!("COMMIT".equals(eventType) || "ROLLBACK".equals(eventType))) {
				buf.append("<event_target>");
			}
		} else {
			buf.append("ON \"").append(targetTable).append('"');

			if (StringUtils.isNotEmpty(targetColumn)) {
				buf.append("(\"").append(targetColumn).append("\")");
			}
		}

		buf.append(newLine);
		buf.append(ifBF.toString());
		buf.append(execBF.toString());
		buf.append(' ');
		String actionType = this.getActionType();

		if ("REJECT".equals(actionType)
				|| "INVALIDATE TRANSACTION".equals(actionType)) {
			buf.append(actionType);
		} else if ("PRINT".equals(actionType)) {
			buf.append(actionType);
			buf.append(newLine);

			if (this.getActionDefintion() != null) {
				buf.append('\'').append(
						this.getActionDefintion().replace("'", "''")).append(
						'\'');
			}
		} else {
			buf.append(newLine);

			if (this.getActionDefintion() != null) {
				buf.append(this.getActionDefintion());
			}
		}

		buf.append(endLineChar);

		this.setDDL(buf.toString());

		return this.triggerDDL;
	}

}
