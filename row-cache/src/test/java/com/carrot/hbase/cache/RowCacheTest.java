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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.carrot.cache.util.CarrotConfig;
import com.carrot.hbase.cache.utils.IOUtils;


/**
 * The Class RowCacheTest.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)

public class RowCacheTest extends BaseTest{

	/** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(RowCacheTest.class);	
	/* Tables */

  /** The n. */
	static protected int N = 10000;
  
	static protected Path dataDir;
	
	static protected long memoryLimit = 1 << 30;
	
	@BeforeClass
  public static void setUp() throws Exception {

    data = generateData(N);

    Configuration conf = new Configuration();
    // Cache configuration
    dataDir = Files.createTempDirectory("temp");
    
    // Cache configuration
    conf.set(CarrotConfig.CACHE_ROOT_DIR_PATH_KEY, dataDir.toString());
    // Set cache type to 'offheap'
    conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, "offheap");
    // set cache size to 1GB
    conf.set(RowCacheConfig.CACHE_OFFHEAP_NAME + "." + CarrotConfig.CACHE_MAXIMUM_SIZE_KEY,
      Long.toString(memoryLimit));

    cache = new RowCache();
    cache.start(conf);

    createTables(Integer.MAX_VALUE);
    initAllRow();
  }

  @AfterClass
  public static void tearDown() throws Exception {
      LOG.error("\n Tear Down the test \n");
      IOUtils.deleteRecursively(dataDir.toFile());
  }
	/**
	 * Test simple put get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	
  public static void initAllRow() throws IOException
	{
		LOG.info("Test simple Put-Get started");
		
		// Take the whole row
		List<Cell> list = data.get(0);				
		Get get = createGet(getRow(list.get(0)), null, null, null);
		get.readVersions(10);
		List<Cell> results = new ArrayList<Cell>();
		cache.preGet(tableA, get, results);
		assertEquals(0, results.size());
		cache.postGet(tableA, get, list);
		get = createGet(getRow(list.get(0)), null, null, null);
		get.readVersions(10);
		cache.preGet(tableA, get, results);		
		assertEquals(list.size(), results.size());		
		assertTrue( equals(list, results));
		LOG.info("Test simple Put-Get finished");		
		
	}
	
	/**
	 * Test single family.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testSingleFamily() throws IOException
	{
		LOG.info("Test single family started");
		List<Cell> list = data.get(0);	
		
		List<Cell> toCmp = subList(list, TestUtils.getFamily(list.get(0)));
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		map.put(TestUtils.getFamily(list.get(0)), null);
		
		Get get = createGet(TestUtils.getRow(list.get(0)), map, null, null);
		get.readVersions(10);	
		List<Cell> results = new ArrayList<Cell>();
		cache.preGet(tableA, get, results);
		assertEquals(COLUMNS.length * VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test single family finished");
		
	}
	
	/**
	 * Test single family one column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testSingleFamilyOneColumn() throws IOException
	{
		LOG.info("Test single family one column started");
		List<Cell> list = data.get(0);	
		List<byte[]> cols = Arrays.asList( new byte[][]{COLUMNS[1]});
		List<Cell> toCmp = subList(list, TestUtils.getFamily(list.get(0)), cols);
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		map.put(TestUtils.getFamily(list.get(0)), getColumnSet(cols));
		
		Get get = createGet(TestUtils.getRow(list.get(0)), map, null, null);
		get.readVersions(10);	
		List<Cell> results = new ArrayList<Cell>();
		cache.preGet(tableA, get, results);
		assertEquals(VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test single family one column finished");
		
	}
	
	/**
	 * Test single family two columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testSingleFamilyTwoColumns() throws IOException
	{
		LOG.info("Test single family two columns started");
		List<Cell> list = data.get(0);	
		List<byte[]> cols = Arrays.asList( new byte[][]{COLUMNS[0], COLUMNS[1]});
		List<Cell> toCmp = subList(list, TestUtils.getFamily(list.get(0)), cols);
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		map.put(TestUtils.getFamily(list.get(0)), getColumnSet(cols));
		
		Get get = createGet(getRow(list.get(0)), map, null, null);
		get.readVersions(10);	
		List<Cell> results = new ArrayList<Cell>();
		cache.preGet(tableA, get, results);
		assertEquals(2 * VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test single family two columns finished");
		
	}

	
	/**
	 * Test single family three columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testSingleFamilyThreeColumns() throws IOException
	{
		LOG.info("Test single family three columns started");
		List<Cell> list = data.get(0);	
		List<byte[]> cols = Arrays.asList( new byte[][]{COLUMNS[0], COLUMNS[1], COLUMNS[2]});
		List<Cell> toCmp = subList(list, getFamily(list.get(0)), cols);
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		map.put(getFamily(list.get(0)), getColumnSet(cols));
		
		Get get = createGet(getRow(list.get(0)), map, null, null);
		get.readVersions(VERSIONS);	
		List<Cell> results = new ArrayList<Cell>();
		cache.preGet(tableA, get, results);
		assertEquals(3 * VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test single family three columns finished");
		
	}	
	
	/**
	 * Test two family three columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testTwoFamilyThreeColumns() throws IOException
	{
		LOG.info("Test two family three columns started");
		List<Cell> list = data.get(0);	
		List<byte[]> cols = Arrays.asList( new byte[][]{COLUMNS[0], COLUMNS[1], COLUMNS[2]});
		List<byte[]> fams = Arrays.asList(new byte[][]{FAMILIES[0], FAMILIES[1]});
		List<Cell> toCmp = subList(list, fams, cols);
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		
		map.put(FAMILIES[0], getColumnSet(cols));
		map.put(FAMILIES[1], getColumnSet(cols));
		
		Get get = createGet(getRow(list.get(0)), map, null, null);
		get.readVersions(VERSIONS);	
		List<Cell> results = new ArrayList<Cell>();
		cache.preGet(tableA, get, results);
		assertEquals(6 * VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test two family three columns finished");
		
	}	
	
	
	/**
	 * Test two family three columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testTwoFamilyThreeColumnsMaxVersions() throws IOException
	{
		
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") started");
			List<Cell> list = data.get(0);
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0],
					COLUMNS[1], COLUMNS[2] });
			List<byte[]> fams = Arrays.asList(new byte[][] { FAMILIES[0],
					FAMILIES[1] });
			List<Cell> toCmp = subList(list, fams, cols, maxVersions);

			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));
			map.put(FAMILIES[1], getColumnSet(cols));

			Get get = createGet(getRow(list.get(0)), map, null, null);
			get.readVersions(maxVersions);
			List<Cell> results = new ArrayList<Cell>();
			cache.preGet(tableA, get, results);
			assertEquals(6 * maxVersions, results.size());
			assertEquals(toCmp.size(), results.size());
			assertTrue(equals(toCmp, results));
			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") finished");
		}
	}	
	
	
	/**
	 * Test two family three columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testTwoFamilyThreeColumnsMaxVersionsTimeRangePast() throws IOException
	{
		
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range past started");
			List<Cell> list = data.get(0);
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0],
					COLUMNS[1], COLUMNS[2] });


			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));
			map.put(FAMILIES[1], getColumnSet(cols));
			
			TimeRange tr = TimeRange.between(0, System.currentTimeMillis() - ((long)24) * 3600 * 1000);
			
			Get get = createGet(getRow(list.get(0)), map, tr, null);
			get.readVersions(maxVersions);
			List<Cell> results = new ArrayList<Cell>();
			cache.preGet(tableA, get, results);
			assertEquals(0, results.size());

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range past finished");
		}
		
	}	
	
	/**
	 * Test two family three columns max versions time range future.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testTwoFamilyThreeColumnsMaxVersionsTimeRangeFuture() throws IOException
	{
		
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range future started");
			List<Cell> list = data.get(0);
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0],
					COLUMNS[1], COLUMNS[2] });

			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));
			map.put(FAMILIES[1], getColumnSet(cols));
			
			TimeRange tr = TimeRange.between(System.currentTimeMillis() + ((long)24)*3600 * 1000, 
					System.currentTimeMillis() + ((long)48) * 3600 * 1000);
			
			Get get = createGet(getRow(list.get(0)), map, tr, null);
			get.readVersions(maxVersions);
			List<Cell> results = new ArrayList<Cell>();
			cache.preGet(tableA, get, results);
			assertEquals(0, results.size());

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range future finished");
		}
		
	}	
	
	/**
	 * Test two family three columns max versions time range present.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testTwoFamilyThreeColumnsMaxVersionsTimeRangePresent() throws IOException
	{
		
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range present started");
			List<Cell> list = data.get(0);
			
			//dump(list);
			
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0],
					COLUMNS[1], COLUMNS[2] });
			List<byte[]> fams = Arrays.asList(new byte[][] { FAMILIES[0],
					FAMILIES[1] });
			List<Cell> toCmp = subList(list, fams, cols, maxVersions);

			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));
			map.put(FAMILIES[1], getColumnSet(cols));
			
			TimeRange tr = TimeRange.between(System.currentTimeMillis() - ((long)24)*3600 * 1000, 
					System.currentTimeMillis() + ((long)24) * 3600 * 1000);
			
			Get get = createGet(getRow(list.get(0)), map, tr, null);
			get.readVersions(maxVersions);
			List<Cell> results = new ArrayList<Cell>();
			cache.preGet(tableA, get, results);
			assertEquals(6 * maxVersions, results.size());
			assertEquals(toCmp.size(), results.size());
			assertTrue(equals(toCmp, results));
			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range present finished");
		}
		
	}	
	
	/**
	 * Test family three columns max versions time range partial.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testFamilyThreeColumnsMaxVersionsTimeRangePartial() throws IOException
	{
		int M = 3;
		List<Cell> list = data.get(0);
		dump(list);
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test family three columns (max versions:"
					+ maxVersions + ") time range partial started");

			long maxTS = list.get(0).getTimestamp();
			
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0]});
			//List<byte[]> fams = Arrays.asList(new byte[][] { FAMILIES[0]});
			//List<KeyValue> toCmp = subList(list, fams, cols, maxVersions);

			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));

			
			TimeRange tr = TimeRange.between( maxTS - M, maxTS +1);
			
			Get get = createGet(getRow(list.get(0)), map, tr, null);
			get.readVersions(maxVersions);
			List<Cell> results = new ArrayList<Cell>();
			cache.preGet(tableA, get, results);
			assertEquals( Math.min(M+1, maxVersions), results.size());
			//assertEquals(toCmp.size(), results.size());
			//assertTrue(equals(toCmp, results));
			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range partial finished");
		}
		
	}	
	
	/**
	 * Test verify get after pre get call row not cached.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
  public void testVerifyGetAfterPreGetCallRowNotCached() throws IOException {
    LOG.info("Test verify get after preGet row not cached started");
    int maxVersions = 5;
    // This row is not cached yet
    List<Cell> list = data.get(1);

    List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0], COLUMNS[2] });

    Map<byte[], NavigableSet<byte[]>> map =
        new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);

    map.put(FAMILIES[0], getColumnSet(cols));
    map.put(FAMILIES[2], getColumnSet(cols));

    TimeRange tr =
        TimeRange.between(System.currentTimeMillis() - 100000, System.currentTimeMillis());

    Get get = createGet(getRow(list.get(0)), map, tr, null);
    get.readVersions(maxVersions);
    List<Cell> results = new ArrayList<Cell>();
    cache.preGet(tableA, get, results);

    assertEquals(Integer.MAX_VALUE, get.getMaxVersions());
    TimeRange trr = get.getTimeRange();
    assertEquals(0, trr.getMin());
    assertEquals(Long.MAX_VALUE, trr.getMax());
    assertNull(get.getFilter());
    // Assert families have no columns
    assertEquals(2, get.numFamilies());
    Map<byte[], NavigableSet<byte[]>> fmap = get.getFamilyMap();
    assertTrue(fmap.containsKey(FAMILIES[0]));
    assertNull(fmap.get(FAMILIES[0]));
    assertTrue(fmap.containsKey(FAMILIES[2]));
    assertNull(fmap.get(FAMILIES[2]));
    assertFalse(fmap.containsKey(FAMILIES[1]));
    LOG.info("Test verify get after preGet row not cached finished");

  }
	
	/**
	 * Test put single family column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testPutSingleFamilyColumn() throws IOException
	{
		cache.resetRequestContext();
		
		LOG.info("Test Put single family:column started ");
		cacheRow(tableA, 1);		
		byte[] row = getRow(data.get(1).get(0));
		// Verify we have cached data
		List<Cell> result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		
		assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
		
		List<Cell> putList = new ArrayList<Cell>();
		LOG.info("Put single family:column");
		KeyValue kv = new KeyValue(row, FAMILIES[0], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		putList.add(kv);
		Put put = createPut(putList);		
		cache.prePut(tableA, put);
		
		// Verify that we cleared row: FAMILIES[0] ONLY;
		Get get = new Get(row);
		get.addFamily(FAMILIES[0]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);	
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-1) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		kv = new KeyValue(row, FAMILIES[1], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		putList.add(kv);
		put = createPut(putList);		
		cache.prePut(tableA, put);
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[1]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);	
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length - 2) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		kv = new KeyValue(row, FAMILIES[2], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		putList.add(kv);
		put = createPut(putList);		
		cache.prePut(tableA, put);
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[2]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-3) * COLUMNS.length * VERSIONS, result.size());		
		LOG.info("Test Put single family:column finished ");
		
	}
	
	/**
	 * Test delete single family column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testDeleteSingleFamilyColumn() throws IOException
	{
		cache.resetRequestContext();
		
		LOG.info("Test Delete single family:column started ");
		cacheRow(tableA, 1);
		byte[] row = getRow(data.get(1).get(0));
		// Verify we have cached data
		List<Cell> result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		
		assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
		
		List<Cell> deleteList = new ArrayList<Cell>();
		LOG.info("Delete single family:column");
		KeyValue kv = new KeyValue(row, FAMILIES[0], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		deleteList.add(kv);
		Delete delete = createDelete(deleteList);		
		cache.preDelete(tableA, delete);
		
		// Verify that we cleared row: FAMILIES[0] ONLY;
		Get get = new Get(row);
		get.addFamily(FAMILIES[0]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-1) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		kv = new KeyValue(row, FAMILIES[1], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		deleteList.add(kv);
		delete = createDelete(deleteList);		
		cache.preDelete(tableA, delete);
		
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[1]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-2) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		kv = new KeyValue(row, FAMILIES[2], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		deleteList.add(kv);
		delete = createDelete(deleteList);		
		cache.preDelete(tableA, delete);
		
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[2]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-3) * COLUMNS.length * VERSIONS, result.size());		
		LOG.info("Test Delete single family:column finished ");
		
	}
	
	/**
	 * Test increment single family column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testIncrementSingleFamilyColumn() throws IOException
	{
		cache.resetRequestContext();
		
		LOG.info("Test Increment single family:column started ");
		cacheRow(tableA, 1);
		byte[] row = getRow(data.get(1).get(0));
		// Verify we have cached data
		List<Cell> result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		
		assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
		
		List<byte[]> families = new ArrayList<byte[]>();
		List<byte[]> columns  = new ArrayList<byte[]>();
		families.add(FAMILIES[0]);
		columns.add(COLUMNS[0]);
		LOG.info("Increment single family:column");
		
		Increment incr = createIncrement(row, constructFamilyMap(families, columns), null, 1L);
		cache.preIncrement(tableA, incr, new Result());
		
		// Verify that we cleared row: FAMILIES[0] ONLY;
		Get get = new Get(row);
		get.addFamily(FAMILIES[0]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length - 1) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		families.add(FAMILIES[1]);		
		incr = createIncrement(row, constructFamilyMap(families, columns), null, 1L);
		cache.preIncrement(tableA, incr, new Result());		
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[1]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-2) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		families.add(FAMILIES[2]);		
		incr = createIncrement(row, constructFamilyMap(families, columns), null, 1L);
		cache.preIncrement(tableA, incr, new Result());	
		
		// Verify that we cleared row: FAMILIES[2] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[2]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-3) * COLUMNS.length * VERSIONS, result.size());		
		LOG.info("Test Increment single family:column finished ");
		
	}
	
	/**
	 * Test append single family column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testAppendSingleFamilyColumn() throws IOException
	{
		cache.resetRequestContext();
		
		LOG.info("Test Append single family:column started ");
		cacheRow(tableA, 1);
		byte[] row = getRow(data.get(1).get(0));
		// Verify we have cached data
		List<Cell> result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		
		assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
		
		List<byte[]> families = new ArrayList<byte[]>();
		List<byte[]> columns  = new ArrayList<byte[]>();
		families.add(FAMILIES[0]);
		columns.add(COLUMNS[0]);
		LOG.info("Append single family:column");
		
		
		Append append = createAppend(row, families, columns, getValue(1));
		cache.preAppend(tableA, append);
		
		// Verify that we cleared row: FAMILIES[0] ONLY;
		Get get = new Get(row);
		get.addFamily(FAMILIES[0]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-1) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		families.add(FAMILIES[1]);		
		append = createAppend(row, families, columns, getValue(1));
		cache.preAppend(tableA, append);	
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[1]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-2) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		families.add(FAMILIES[2]);		
		append = createAppend(row, families, columns, getValue(1));
		cache.preAppend(tableA, append);
		
		// Verify that we cleared row: FAMILIES[2] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[2]);
		
		result = new ArrayList<Cell>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length - 3) * COLUMNS.length * VERSIONS, result.size());		
		LOG.info("Test Append single family:column finished ");
		
	}
	
	
	/**
	 * Test point get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testPointGet() throws IOException
	{
		cache.resetRequestContext();
		LOG.info("Test point Get started. We test full get request: row:family:column:version ");
		cacheRow(tableA, 1);
		
		List<byte[]> families = new ArrayList<byte[]>();
		List<byte[]> columns = new ArrayList<byte[]>();
		
		families.add(FAMILIES[2]);
		columns.add(COLUMNS[0]);
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(families, columns);
		byte[] row = getRow(data.get(1).get(0));
		int expectedIndex = COLUMNS.length * VERSIONS * 2;
		
		long time =  data.get(1).get(expectedIndex).getTimestamp();
		TimeRange tr = TimeRange.between(time, time + 1);
		Get get = createGet(row, map, tr, null);

		List<Cell> results = new ArrayList<Cell>();
		List<Cell> expected = new ArrayList<Cell>();
		expected.add(data.get(1).get(expectedIndex));
		cache.preGet(tableA, get, results);
		
		assertEquals(1, results.size());		
		assertTrue(equals(expected, results));
		
		LOG.info("Test point Get finished. ");
		
	}
	
	/**
	 * Test multi point get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testMultiPointGet() throws IOException
	{
		cache.resetRequestContext();
		LOG.info("Test point Get started. We test full get request (multi): row:family:column:version ");
		cacheRow(tableA, 1);
		
		List<byte[]> families = new ArrayList<byte[]>();
		List<byte[]> columns = new ArrayList<byte[]>();
		
		families.add(FAMILIES[0]);
		families.add(FAMILIES[1]);
		families.add(FAMILIES[2]);
		columns.add(COLUMNS[0]);
		columns.add(COLUMNS[1]);
		columns.add(COLUMNS[2]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(families, columns);
		byte[] row = getRow(data.get(1).get(0));
		
		Get get = createGet(row, map, null, null);
		get.readVersions(1);
		
		List<Cell> results = new ArrayList<Cell>();
		List<Cell> expected = new ArrayList<Cell>();
		List<Cell> list = data.get(1);
		for(int i = 0; i < list.size(); i += VERSIONS){
			expected.add(list.get(i));
		}

		cache.preGet(tableA, get, results);
		
		assertEquals(expected.size(), results.size());		
		assertTrue(equals(expected, results));
		
		LOG.info("Test multi-point Get finished. ");
		
	}
}
