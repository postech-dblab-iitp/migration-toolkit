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

import org.junit.Assert;
import org.junit.Test;

public class SchemaTest {

	@Test
	public void testEqualsObject() {
		Schema schema1 = new Schema();
		Assert.assertTrue(schema1.equals(schema1));
		Assert.assertFalse(schema1.equals(12));
		Assert.assertFalse(schema1.equals(null));
		Assert.assertTrue(schema1.hashCode() == schema1.hashCode());

		Schema schema2 = new Schema();
		Assert.assertTrue(schema1.equals(schema2));
		Assert.assertTrue(schema1.hashCode() == schema2.hashCode());

		schema1.setName("testSchema");
		Assert.assertFalse(schema2.equals(schema1));
		schema2.setName("testSchema");
		Assert.assertTrue(schema1.equals(schema2));
		Assert.assertTrue(schema1.hashCode() == schema2.hashCode());

		Catalog catalog1 = new Catalog();
		schema1.setCatalog(catalog1);
		Assert.assertFalse(schema2.equals(schema1));

		schema2.setCatalog(catalog1);
		schema2.setName("testSchema2");
		Assert.assertFalse(schema1.equals(schema2));
		Assert.assertTrue(schema1.hashCode() != schema2.hashCode());
		
		catalog1.setName("catalog1");
		schema2.setCatalog(new Catalog());	
		schema2.setName("testSchema");
		Assert.assertFalse(schema1.equals(schema2));
	}

}
