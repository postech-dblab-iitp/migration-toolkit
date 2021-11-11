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

import junit.framework.Assert;

import org.junit.Test;

import com.cubrid.cubridmigration.cubrid.dbobj.CUBRIDTrigger;

public class CUBRIDTriggerTest {

	@Test
	public void testCUBRIDTrigger() {
		CUBRIDTrigger trigger = new CUBRIDTrigger();
		CUBRIDTrigger.getEventType("0");
		CUBRIDTrigger.getEventType("10");

		CUBRIDTrigger.getStatus("0");
		CUBRIDTrigger.getStatus("1");

		trigger.setName("test_trigger");
		trigger.setPriority("1.0");

		trigger.setEventType("COMMIT");
		trigger.setActionDefintion("DELETE");
		trigger.setActionTime("AFTER");
		trigger.setActionType("REJECT");
		trigger.setCondition("1==1");
		trigger.setConditionTime("AFTER");
		trigger.setStatus("ACTIVE");
		trigger.setTargetAttribute("attr");
		trigger.setTargetClass("test_table");
		trigger.getDDL();

		trigger.setCondition("");
		trigger.setTargetClass("");
		trigger.setActionTime("DEFERRED");
		trigger.setStatus("INACTIVE");
		trigger.setActionType("INVALIDATE TRANSACTION");
		trigger.getDDL();

		trigger.setActionType("PRINT");
		trigger.setEventType("ROLLBACK");
		trigger.getDDL();
	}

	@Test
	public void testCUBRIDTrigger2() {
		CUBRIDTrigger trigger = new CUBRIDTrigger();
		trigger.setPriority("aa");
		String ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("<trigger_name>") != -1);

		trigger.setName("test_trigger");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("test_trigger") != -1);

		trigger.setStatus("INACTIVE");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("STATUS INACTIVE") != -1);

		trigger.setCondition(null);
		trigger.setActionTime("2");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("AFTER") != -1);

		trigger.setActionTime("1");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("BEFORE") != -1);

		trigger.setActionTime("3");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("DEFERRED") != -1);

		trigger.setCondition("AFTER");
		trigger.setActionTime("3");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("DEFERRED") != -1);

		trigger.setActionType("2");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("REJECT") != -1);

		trigger.setActionType("3");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("INVALIDATE TRANSACTION") != -1);

		trigger.setPriority("0.10");
		trigger.setTargetClass("table");
		trigger.setEventType("COMMIT");
		trigger.setTargetAttribute("column");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("ON \"table\"(\"column\")") != -1);

		trigger.setEventType("ROLLBACK");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("ON \"table\"(\"column\")") != -1);

		trigger.setTargetClass("");
		trigger.setEventType("OTHER");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("<event_target>") != -1);

		trigger.setActionType("10");
		trigger.setActionDefintion("test");
		ddl = trigger.getDDL();
		Assert.assertEquals(true, ddl.indexOf("test") != -1);
	}
}
