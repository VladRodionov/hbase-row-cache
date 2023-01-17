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
package com.carrot.hbase.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * The Class CoprocessorBasicOpsTest.
 */
@SuppressWarnings("deprecation")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CoprocessorBasicOpsTest extends CoprocessorBaseTest{

  
  @Before
  public void beforeTest() throws IOException {
    putAllData(_tableA, N);
  }
  
  @Test
  public void testAll() throws IOException {
    testFirstGet();
    testSecondGet();
    testThirdGet();
    testThirdGetBatch();
    testThirdGetBatchCacheDisabled();
    xtestExistsInCache();
    testDML();
  }
  /**
	 * Test load co-processor.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
  
	private void testFirstGet() throws IOException
	{
		
		LOG.error("Test first get started");
		
		long start = System.currentTimeMillis();
		for(int i = 0 ; i< N; i++){		
			Get get = createGet(getRow(data.get(i).get(0)), null, null, null);
			get.readVersions(Integer.MAX_VALUE);
			Result result = _tableA.get(get);		
			List<Cell> list = result.listCells();
			assertEquals(data.get(i).size(), list.size());	
			assertEquals(0, cache.getFromCache());
		}
		LOG.error("Test first get finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	/**
	 * Test second get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	private void testSecondGet() throws IOException
	{
		
		LOG.error("Test second get started");
		
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < N; i++){		
			Get get = createGet(getRow(data.get(i).get(0)), null, null, null);
			get.readVersions(Integer.MAX_VALUE);
			Result result = _tableA.get(get);		
			List<Cell> list = result.listCells();
			assertEquals(data.get(i).size(), list.size());	
			assertEquals(list.size(), cache.getFromCache());
			
		}
		LOG.error("Test second get finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	/**
	 * _test third get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	private void testThirdGet() throws IOException
	{
		
		LOG.error("Test third (1 narrow) get started");
		
		long start = System.currentTimeMillis();
		List<byte[]> fam = new ArrayList<byte[]>();
		fam.add(FAMILIES[0]);
		List<byte[]> col = new ArrayList<byte[]>();
		col.add(COLUMNS[0]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);
		
		for(int i = 0 ; i < N; i++){		
			Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
			get.readVersions(1);
			Result result = _tableA.get(get);			
			List<Cell> list = result.listCells();
			assertEquals(1, list.size());	
			assertEquals(list.size(), cache.getFromCache());
			
		}
		LOG.error("Test third get finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	/**
	 * _test third get batch.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	private void testThirdGetBatch() throws IOException
	{
		
		LOG.error("Test third (1 narrow) get batch started");
		int BATCH_SIZE = 100;
		long start = System.currentTimeMillis();
		List<byte[]> fam = new ArrayList<byte[]>();
		fam.add(FAMILIES[0]);
		List<byte[]> col = new ArrayList<byte[]>();
		col.add(COLUMNS[0]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);
		
		for(int i = 0 ; i< N; i += BATCH_SIZE){		
			List<Get> batch = new ArrayList<Get>();
			for(int k =0; k < BATCH_SIZE; k++){
				Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
				get.readVersions(1);
				batch.add(get);
			}
									
			Result[] result = _tableA.get(batch);	
			assertEquals(BATCH_SIZE, result.length);
			for( int j =0; j < result.length; j++){				
				assertEquals(1, result[j].size());
			}
			
		}
		LOG.error("Test third get batch finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	/**
	 * Test third get batch cache disabled.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	private void testThirdGetBatchCacheDisabled() throws IOException
	{
		
		LOG.error("Test third (1 narrow) get batch cache disabled started");
		cache.setDisabled(true);
		int BATCH_SIZE = 100;
		long start = System.currentTimeMillis();
		List<byte[]> fam = new ArrayList<byte[]>();
		fam.add(FAMILIES[0]);
		List<byte[]> col = new ArrayList<byte[]>();
		col.add(COLUMNS[0]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);
		
		for(int i=0 ; i< N; i += BATCH_SIZE){		
			List<Get> batch = new ArrayList<Get>();
			for(int k =0; k < BATCH_SIZE; k++){
				Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
				get.readVersions(1);
				batch.add(get);
			}
			Result[] result = _tableA.get(batch);	
			assertEquals(BATCH_SIZE, result.length);
		}
		LOG.error("Test third get batch cache disabled finished in "+(System.currentTimeMillis() - start)+"ms");
	}
	
	/**
	 * Test exists in cache.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	private void xtestExistsInCache() throws IOException
	{
		
		LOG.error("Test exists in Cache started");
		// Enable row-cache
		cache.setDisabled(false);
		long start = System.currentTimeMillis();
		for(int i=0 ; i< N; i++){		
			Get get = createGet(getRow(data.get(i).get(0)), null, null, null);
			get.readVersions(1);
			boolean result = _tableA.exists(get);	
			assertTrue(result);
			assertEquals(FAMILIES.length * COLUMNS.length, cache.getFromCache());
		}
		LOG.error("Test exists in Cache finished in "+(System.currentTimeMillis() - start)+"ms");
	}
	
  private void testDML() throws IOException {

    TableName tableName = TableName.valueOf(TABLE_C);

    TableDescriptor desc = admin.getDescriptor(tableName);
    HTableDescriptor tdesc = new HTableDescriptor(desc);
    System.out.println("TABLE:\n" + desc);

    HColumnDescriptor[] dds = tdesc.getColumnFamilies();
    for (HColumnDescriptor d : dds) {
      System.out.println(d);
    }
    if (admin.isTableEnabled(tableName)) {
      admin.disableTable(tableName);
    }
    assertNull(tdesc.getValue(RConstants.ROWCACHE));
    assertNull(dds[0].getValue(RConstants.ROWCACHE));

    (tdesc).setValue(RConstants.ROWCACHE, RConstants.TRUE);
    (dds[0]).setValue(RConstants.ROWCACHE, RConstants.TRUE);
    
    admin.modifyColumnFamily(tableName, dds[0]);
    admin.modifyTable(tdesc);

    System.out.println("Updated CF . Waiting 2 sec");
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    System.out.println("Enabling table ");
    admin.enableTable(tableName);
    System.out.println("Enabling table done .");
    desc = admin.getDescriptor(tableName);
    tdesc = new HTableDescriptor(desc);
    dds = tdesc.getColumnFamilies();
    byte[] vvalue = tdesc.getValue(RConstants.ROWCACHE);
    assertTrue(Bytes.equals(RConstants.TRUE, vvalue));
    assertTrue(Bytes.equals(RConstants.TRUE, dds[0].getValue(RConstants.ROWCACHE)));

    System.out.println("TABLE:\n" + desc);
    for (ColumnFamilyDescriptor d : dds) {
      System.out.println(d);
    }
  }
}
