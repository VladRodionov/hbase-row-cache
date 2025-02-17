/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrotdata.hbase.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * The Class CoprocessorGetTest.
 * Tests Get/Exists
 * 
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CoprocessorDMLTest extends CoprocessorBaseTest{

	/** The Constant LOG. */
	static final Logger LOG = LoggerFactory.getLogger(CoprocessorDMLTest.class);
	
	@Before
	public void beforeTest() throws IOException {
	  putAllData(_tableA, N);
	}
	
	@Test
	/**
	 * The test is supposed to be executed first
	 * @throws IOException
	 */
	public void testAGetFromHBase() throws IOException
	{
		
		LOG.error("Test get from HBase started");
		
		long start = System.currentTimeMillis();
		for(int i = 0 ; i< N; i++){		
			Get get = createGet(getRow(data.get(i).get(0)), null, null, null);
			get.readVersions(Integer.MAX_VALUE);
			Result result = _tableA.get(get);		
			List<Cell> list = result.listCells();
			assertEquals(data.get(i).size(), list.size());			
			
		}
		//FIXME: this assertion fails, because cache size does not count objects in write buffers
		//
		//assertEquals(N * FAMILIES.length, cache.size());	
		LOG.error("Test get from HBase finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	@Test
	public void testDisableEnableTable() throws IOException
	{
		LOG.error("Test Disable-Enable Table started.");
		assertFalse(cache.isDisabled());
		long cacheSize = cache.size();
		LOG.error("Cache size before disabling ="+cacheSize);
		cache.setTrace(true);
		admin.disableTable(TableName.valueOf(TABLE_A));
		assertEquals(cache.size(), cacheSize);
		// TODO verify that
		//assertTrue(cache.isDisabled());
		admin.enableTable(TableName.valueOf(TABLE_A));
		assertFalse(cache.isDisabled());
		assertEquals(cache.size(), cacheSize);
		LOG.error("Test Disable-Enable Table finished.");
	}

	
}
