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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.hbase.cache.RowCache;
import com.carrotdata.hbase.cache.RowCacheConfig;

/**
 * The Class SingleThreadPerfTest.
 */
public class SingleThreadPerfTest extends BaseTest{
	  /** The Constant LOG. */
  	static final Log LOG = LogFactory.getLog(SingleThreadPerfTest.class);
	// Total number of Rows to test
	static int N = 100000;
	static Path dataDir;
	
	@BeforeClass
  public static void setUp() throws Exception {
    if (data != null) return;
    data = generateData(N);
    Configuration conf = new Configuration();
    // Cache configuration
    dataDir = Files.createTempDirectory("temp");
    // Cache configuration
    conf.set(CacheConfig.CACHE_DATA_DIR_PATHS_KEY, dataDir.toString());
    // Set cache type to 'offheap' - FIXME
    conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, "offheap");
    // set cache size to 1GB
    conf.set(RowCacheConfig.CACHE_OFFHEAP_NAME + "." + CacheConfig.CACHE_MAXIMUM_SIZE_KEY,
      Long.toString(1L<< 31));

    cache = new RowCache();
    cache.start(conf);
    createTables(Integer.MAX_VALUE);
    loadData();
  }
	
	/**
	 * Test load time.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private static void loadData () throws IOException
	{
		LOG.info("Test load started");
		long start = System.currentTimeMillis();
		for(int i = 0; i < N; i++){
			cacheRow(tableA, i);
		}
		LOG.info("Cache size  ="+ cache.getCache().getStorageAllocated()+": items ="+cache.getCache().size());
		LOG.info("Loading "+ (N * FAMILIES.length) +" full rows"+(COLUMNS.length * VERSIONS)+ " KV's each) took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getCache().getStorageAllocated());
		LOG.info("Cache items ="+cache.getCache().size());
		LOG.info("Test load finished");
		
	}
	
	/**
	 * Test get full row.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testGetFullRow() throws IOException
	{
		LOG.info("Test get full row started");
		int M = 10;
		long start = System.currentTimeMillis();
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(Arrays.asList(FAMILIES), null);
		for(int  k = 0; k < M; k++){
			long t = System.currentTimeMillis();
			for(int i = 0; i < N ; i++){
				List<Cell> list = data.get(i);
				Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
				get.readVersions(VERSIONS);
				List<Cell> results = new ArrayList<Cell>();
				boolean bypass = cache.preGet(tableA, get, results);
				assertTrue(bypass);
				assertEquals(list.size(), results.size());
				assertTrue(equals(list, results));
				
			}
			LOG.info("Get "+ (FAMILIES.length * N)  +" row:family (s) in "+ (System.currentTimeMillis() - t)+" ms");
		}
		LOG.info("Reading "+ (N * M * FAMILIES.length) +" full rows (30 KV's each) took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getCache().getStorageAllocated());
		LOG.info("Cache items ="+cache.getCache().size());
		LOG.info("Test get full finished");
	}
	
	/**
	 * Test get full row perf.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testGetFullRowPerf() throws IOException
	{
		LOG.info("Test get full row started - PERF");
		int M = 10;
		long start = System.currentTimeMillis();
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(Arrays.asList(FAMILIES), null);
		for(int  k = 0; k < M; k++){
			long t = System.currentTimeMillis();
			for(int i =0; i < N ; i++){
				List<Cell> list = data.get(i);
				Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
				get.readVersions(VERSIONS);
				List<Cell> results = new ArrayList<Cell>();
				boolean bypass = cache.preGet(tableA, get, results);
				assertTrue(bypass);
				assertEquals(list.size(), results.size());
				
			}
			LOG.info("Get "+ (FAMILIES.length * N)  +" row:family (s) in "+ (System.currentTimeMillis() - t)+" ms");
		}
		LOG.info("Reading "+ (N * M * FAMILIES.length) +" full rows (30 KV's each) took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getCache().getStorageAllocated());
		LOG.info("Cache items ="+cache.getCache().size());
		LOG.info("Test get full finished - PERF");
	}
	
	/**
	 * Test get full row perf last version.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testGetFullRowPerfLastVersion() throws IOException
	{
		LOG.info("Test get full row started - PERF LAST VERSION ONLY");
		int M = 10;
		long start = System.currentTimeMillis();
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(Arrays.asList(FAMILIES), null);
		for(int  k = 0; k < M; k++){
			long t = System.currentTimeMillis();
			for(int i = 0; i < N ; i++){
				List<Cell> list = data.get(i);
				Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
				get.readVersions(1);
				List<Cell> results = new ArrayList<Cell>();
				boolean bypass = cache.preGet(tableA, get, results);
				assertTrue(bypass);
				assertEquals(list.size()/VERSIONS, results.size());
				
			}
			LOG.info("Get "+ (FAMILIES.length * N)  +" row:family (s) in "+ (System.currentTimeMillis() - t)+" ms");
		}
		LOG.info("Reading "+ (N * M * FAMILIES.length) +" full rows "+ (COLUMNS.length)+ " each took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getCache().getStorageAllocated());
		LOG.info("Cache items ="+cache.getCache().size());
		LOG.info("Test get full finished - PERF LAST VERSION");
	}
	
	/**
	 * Test get row fam col perf last version.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testGetRowFamColPerfLastVersion() throws IOException
	{
		LOG.info("Test get row:fam:col started - PERF LAST VERSION ONLY");
		int M = 10;
		long start = System.currentTimeMillis();
		List<byte[]> cols = new ArrayList<byte[]>();
		cols.add(COLUMNS[0]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(Arrays.asList(FAMILIES), cols);
		for(int  k = 0; k < M; k++){
			long t = System.currentTimeMillis();
			for(int i = 0; i < N ; i++){
				List<Cell> list = data.get(i);
				Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
				get.readVersions(1);
				List<Cell> results = new ArrayList<Cell>();
				boolean bypass = cache.preGet(tableA, get, results);
				assertTrue(bypass);
				assertEquals(list.size()/(VERSIONS * COLUMNS.length), results.size());
				
			}
			LOG.info("Get "+ (FAMILIES.length * N)  +" row:fam:col (s) in "+ (System.currentTimeMillis() - t)+" ms");
		}
		LOG.info("Reading "+ (N * M * FAMILIES.length) +" row:fam:col "+ (COLUMNS.length)+ " each took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getCache().getStorageAllocated());
		LOG.info("Cache items ="+cache.getCache().size());
		LOG.info("Test get row:fam:col finished - PERF LAST VERSION");
	}	
}
