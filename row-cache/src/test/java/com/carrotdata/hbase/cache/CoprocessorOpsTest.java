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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;



/**
 * The Class CoprocessorGetTest.
 * Tests Get/Exists
 * 
 */
public class CoprocessorOpsTest extends CoprocessorBaseTest {

  /** The Constant LOG. */
  static final Logger LOG = LoggerFactory.getLogger(CoprocessorOpsTest.class);

  /**
   * Test put all.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testAll() throws IOException {
    putAllData(_tableA, N);
    testGetFromHBase();
    testGetFromCache();
    testGetFromCacheOneCell();
    testGetFromCacheBatch();
    testGetBatchCacheDisabled();
    testExistsInCache();
    testGetCacheMaxVersions();
    testGetCacheTimeRangeCurrent();
    testGetCacheTimeRangePast();
    testGetCacheTimeRangeFuture();
    testDelete();
    testUpdate();
    testAppend();
    testIncrement();
    testCheckAndPut();
    testCheckAndDelete();
    testSimpleScan();
  }

  /**
   * Test load co-processor.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testGetFromHBase() throws IOException {

    LOG.error("Test get from HBase started");

    long start = System.currentTimeMillis();
    int found = 0;
    for (int i = 0; i < N; i++) {
      Get get = createGet(getRow(data.get(i).get(0)), null, null, null);
      get.readVersions(Integer.MAX_VALUE);
      Result result = _tableA.get(get);
      if (result.isEmpty() == false) found++;
      // List<KeyValue> list = result.list();
      // assertEquals(data.get(i).size(), list.size());
      // assertEquals(0, cache.getFromCache());

    }
    assertEquals(N, found);
    LOG.error("Test get from HBase finished in " + (System.currentTimeMillis() - start)
        + "ms. Found " + found + " objects.");

  }

  /**
   * Test second get.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testGetFromCache() throws IOException {

    LOG.error("Test get from cache started");

    long start = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      Get get = createGet(getRow(data.get(i).get(0)), null, null, null);
      get.readVersions(Integer.MAX_VALUE);
      Result result = _tableA.get(get);
      List<Cell> list = result.listCells();
      assertEquals(data.get(i).size(), list.size());
      assertEquals(list.size(), cache.getFromCache());

    }
    //FIXME: cache size is not accurate b/c it does not count objects in write buffers
    assertEquals(N * FAMILIES.length, cache.size());
    LOG.error("Test get from cache finished in " + (System.currentTimeMillis() - start) + "ms");

  }

  /**
   * Test third get.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testGetFromCacheOneCell() throws IOException {

    LOG.error("Test get from cache one cell started");

    long start = System.currentTimeMillis();
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);

    for (int i = 0; i < N; i++) {
      Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
      get.readVersions(1);
      Result result = _tableA.get(get);
      List<Cell> list = result.listCells();
      assertEquals(1, list.size());
      assertEquals(list.size(), cache.getFromCache());

    }
    LOG.error(
      "Test get from cache one cell finished in " + (System.currentTimeMillis() - start) + "ms");

  }

  /**
   * Test third get batch.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testGetFromCacheBatch() throws IOException {

    LOG.error("Test get from cache batch started");
    int BATCH_SIZE = 100;
    long start = System.currentTimeMillis();
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);

    for (int i = 0; i < N; i += BATCH_SIZE) {
      List<Get> batch = new ArrayList<Get>();
      for (int k = 0; k < BATCH_SIZE; k++) {
        Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
        get.readVersions(1);
        batch.add(get);
      }
      Result[] result = _tableA.get(batch);
      assertEquals(BATCH_SIZE, result.length);
    }
    LOG.error(
      "Test get from cache batch finished in " + (System.currentTimeMillis() - start) + "ms");

  }

  /**
   * Test third get batch cache disabled.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testGetBatchCacheDisabled() throws IOException {

    LOG.error("Test get batch cache disabled started");
    cache.setDisabled(true);
    int BATCH_SIZE = 100;
    long start = System.currentTimeMillis();
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);

    for (int i = 0; i < N; i += BATCH_SIZE) {
      List<Get> batch = new ArrayList<Get>();
      for (int k = 0; k < BATCH_SIZE; k++) {
        Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
        get.readVersions(1);
        batch.add(get);
      }

      Result[] result = _tableA.get(batch);
      assertEquals(BATCH_SIZE, result.length);

      //
      // List<KeyValue> list = result.list();
      // assertEquals(1, list.size());
      // assertEquals(list.size(), cache.getFromCache());

    }
    LOG.error(
      "Test get batch cache disabled finished in " + (System.currentTimeMillis() - start) + "ms");

  }

  /**
   * Test exists in cache.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testExistsInCache() throws IOException {

    LOG.error("Test exists in Cache started");
    // Enable row-cache
    cache.setDisabled(false);
    long start = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      Get get = createGet(getRow(data.get(i).get(0)), null, null, null);
      get.readVersions(1);
      boolean result = _tableA.exists(get);
      assertTrue(result);
      assertEquals(FAMILIES.length * COLUMNS.length, cache.getFromCache());
    }
    LOG.error("Test exists in Cache finished in " + (System.currentTimeMillis() - start) + "ms");

  }

  /**
   * Test get cache max versions.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testGetCacheMaxVersions() throws IOException {
    int maxVersions = 7;
    LOG.error("Test get from cache one cell (max versions=" + maxVersions + ") started");

    long start = System.currentTimeMillis();
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);

    for (int i = 0; i < N; i++) {
      Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
      get.readVersions(maxVersions);
      Result result = _tableA.get(get);
      List<Cell> list = result.listCells();
      assertEquals(maxVersions, list.size());
      assertEquals(list.size(), cache.getFromCache());

    }
    LOG.error(
      "Test get from cache one cell finished in " + (System.currentTimeMillis() - start) + "ms");
  }

  /**
   * Test get cache time range future.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testGetCacheTimeRangeFuture() throws IOException {

    TimeRange tr =
        TimeRange.between(System.currentTimeMillis() + 100, System.currentTimeMillis() + 1000);
    LOG.error("Test get from cache one cell (TimeRange is in future) started");

    long start = System.currentTimeMillis();
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);

    for (int i = 0; i < N; i++) {
      Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
      get.readVersions(Integer.MAX_VALUE);
      get.setTimeRange(tr.getMin(), tr.getMax());
      Result result = _tableA.get(get);
      List<Cell> list = result.listCells();
      assertNull(list);
      assertEquals(0, cache.getFromCache());

    }
    LOG.error("Test get from cache one cell (TimeRange is in future) finished in "
        + (System.currentTimeMillis() - start) + "ms");
  }

  /**
   * Test get cache time range past.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testGetCacheTimeRangePast() throws IOException {

    TimeRange tr = TimeRange.between(System.currentTimeMillis() - 10000000,
      System.currentTimeMillis() - 1000000);
    LOG.error("Test get from cache one cell (TimeRange is in the past) started");

    long start = System.currentTimeMillis();
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);

    for (int i = 0; i < N; i++) {
      Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
      get.readVersions(Integer.MAX_VALUE);
      get.setTimeRange(tr.getMin(), tr.getMax());
      Result result = _tableA.get(get);
      List<Cell> list = result.listCells();
      assertNull(list);
      assertEquals(0, cache.getFromCache());

    }
    LOG.error("Test get from cache one cell (TimeRange is in the past) finished in "
        + (System.currentTimeMillis() - start) + "ms");
  }

  /**
   * Test get cache time range current.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testGetCacheTimeRangeCurrent() throws IOException {

    TimeRange tr = TimeRange.between(System.currentTimeMillis() - 10000000,
      System.currentTimeMillis() + 1000000);
    LOG.error("Test get from cache one cell (TimeRange is current) started");

    long start = System.currentTimeMillis();
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);

    for (int i = 0; i < N; i++) {
      Get get = createGet(getRow(data.get(i).get(0)), map, null, null);
      get.readVersions(Integer.MAX_VALUE);
      get.setTimeRange(tr.getMin(), tr.getMax());
      Result result = _tableA.get(get);
      List<Cell> list = result.listCells();
      assertEquals(VERSIONS, list.size());
      assertEquals(VERSIONS, cache.getFromCache());

    }
    LOG.error("Test get from cache one cell (TimeRange is current) finished in "
        + (System.currentTimeMillis() - start) + "ms");
  }

  /**
   * Test delete.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testDelete() throws IOException {
    LOG.error("Test delete started");
    byte[] row = getRow(data.get(0).get(0));
    LOG.error(" Delete row: " + new String(row) + " cache size="+ cache.size());

    Delete del = createDelete(row);
    _tableA.delete(del);

    // Verify size : 1 row = 3 KVs deleted
    //FIXME: cache size is inaccurate
    assertEquals((N - 1) * FAMILIES.length, cache.size());
    Get get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    Result result = _tableA.get(get);
    assertTrue(result.isEmpty());
    assertEquals(0, cache.getFromCache());

    // Restore row back
    data.set(0, generateRowData(0));

    Put put = createPut(data.get(0));

    // dumpPut(put);

    _tableA.put(put);
    // _tableA.flushCommits();

    // Check it is not in cache yet
    assertEquals((N - 1) * FAMILIES.length, cache.size());
    // Load to cache
    get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);

    result = _tableA.get(get);
    assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
    // Check it is in cache
    assertEquals((N) * FAMILIES.length, cache.size());

    LOG.error(" Delete row:family: " + new String(row) + ":" + new String(FAMILIES[0]));
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);

    sleep(10);

    del = createDelete(row, fam);

    _tableA.delete(del);
    // _tableA.flushCommits();
    // Verify size : -1
    assertEquals((N) * FAMILIES.length - 1, cache.size());

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, null);
    // Verify not in cache
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertTrue(result.isEmpty());
    assertEquals(0, cache.getFromCache());
    LOG.error("1) Cache size=" + cache.size());
    // Verify that other families are in cache
    fam.clear();
    fam.add(FAMILIES[1]);
    fam.add(FAMILIES[2]);
    map = constructFamilyMap(fam, null);
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals((FAMILIES.length - 1) * COLUMNS.length * VERSIONS, result.size());
    // Verify all from cache
    assertEquals((FAMILIES.length - 1) * COLUMNS.length * VERSIONS, cache.getFromCache());
    LOG.error("2) Cache size=" + cache.size());

    // Delete row:family:col
    del = new Delete(row);
    del.addColumns(FAMILIES[1], COLUMNS[0]);
    _tableA.delete(del);
    LOG.error("3) Cache size=" + cache.size());

    // Verify what is still in cache (only FAMILY[2])
    fam.clear();
    fam.add(FAMILIES[1]);
    fam.add(FAMILIES[2]);
    map = constructFamilyMap(fam, null);
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals((FAMILIES.length - 1) * COLUMNS.length * VERSIONS - (1) * VERSIONS, result.size());
    // Verify all from cache
    assertEquals((FAMILIES.length - 2) * COLUMNS.length * VERSIONS, cache.getFromCache());
    LOG.error("4) Cache size=" + cache.size());

    // Restore row
    restoreRow(0);
    LOG.error("5) Cache size=" + cache.size());

    LOG.error("Test delete finished OK");
  }

  /**
   * Test update.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testUpdate() throws IOException {
    int index = 1;
    LOG.error("Test update started. Testing " + index + " row, cache size="+ cache.size());

    byte[] row = getRow(data.get(index).get(0));
    LOG.error(" Update row: " + new String(row));

    Put put = createPut(data.get(index));
    _tableA.put(put);

    // Verify that cache size decreased
    assertEquals((N - 1) * FAMILIES.length, cache.size());
    Get get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    Result result = _tableA.get(get);
    assertEquals(data.get(index).size(), result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());
    // Do second get
    get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);
    assertEquals(data.get(index).size(), result.size());
    // Verify that not from cache
    assertEquals(data.get(index).size(), cache.getFromCache());
    // Check it is in cache
    assertEquals((N) * FAMILIES.length, cache.size());

    LOG.error(" Update row:family: " + new String(row) + ":" + new String(FAMILIES[0]));
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);

    List<Cell> list = filter(data.get(index), FAMILIES[0], null);
    assertEquals(COLUMNS.length * VERSIONS, list.size());

    put = createPut(list);
    _tableA.put(put);
    // Verify that cache size decreased by 1 (row:family)
    assertEquals((N) * FAMILIES.length - 1, cache.size());

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, null);
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(COLUMNS.length * VERSIONS, result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());

    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(COLUMNS.length * VERSIONS, result.size());
    // Verify that all from cache
    assertEquals(COLUMNS.length * VERSIONS, cache.getFromCache());
    LOG.error(" Update row:family:column: " + new String(row) + ":" + new String(FAMILIES[0]) + ":"
        + new String(COLUMNS[0]));
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    list = filter(data.get(index), FAMILIES[0], COLUMNS[0]);
    assertEquals(VERSIONS, list.size());
    put = createPut(list);
    _tableA.put(put);
    // Verify that cache size decreased by 1 (row:family)
    assertEquals((N) * FAMILIES.length - 1, cache.size());
    map = constructFamilyMap(fam, col);
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(VERSIONS, result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());

    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(VERSIONS, result.size());
    // Verify that all from cache
    assertEquals(VERSIONS, cache.getFromCache());

    LOG.error("Test update finished OK");
  }

  /**
   * Test append.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testAppend() throws IOException {
    int index = 2;

    LOG.error("Test append started. Testing " + index + " row");

    byte[] row = getRow(data.get(index).get(0));
    byte[] toAppend = "APPEND".getBytes();
    LOG.error(" Append row: " + new String(row));

    Get get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    Result result = _tableA.get(get);
    List<Cell> ll = result.listCells();
    assertEquals(data.get(index).size(), ll.size());

    Append append = createAppend(row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS), toAppend);
    _tableA.append(append);

    // Verify that cache size decreased
    assertEquals((N - 1) * FAMILIES.length, cache.size());
    get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);

    result = _tableA.get(get);

    // This is BUG or Feature in HBase 0.94.x - Append operation - NOPE
    // deletes all versions of a cell except the last one
    assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());
    // Do second get
    get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);
    assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
    // Verify that not from cache
    assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, cache.getFromCache());
    // Check it is in cache
    assertEquals((N) * FAMILIES.length, cache.size());

    LOG.error(" Append row:family: " + new String(row) + ":" + new String(FAMILIES[0]));
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);

    List<Cell> list = filter(data.get(index), FAMILIES[0], null);
    assertEquals(COLUMNS.length * VERSIONS, list.size());

    append = createAppend(row, fam, Arrays.asList(COLUMNS), toAppend);
    _tableA.append(append);
    // Verify that cache size decreased by 1 (row:family)
    assertEquals((N) * FAMILIES.length - 1, cache.size());

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, null);
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(COLUMNS.length * VERSIONS, result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());

    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(COLUMNS.length * VERSIONS, result.size());
    // Verify that all from cache
    assertEquals(COLUMNS.length * VERSIONS, cache.getFromCache());
    LOG.error(" Append row:family:column: " + new String(row) + ":" + new String(FAMILIES[0]) + ":"
        + new String(COLUMNS[0]));
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    list = filter(data.get(index), FAMILIES[0], COLUMNS[0]);
    assertEquals(VERSIONS, list.size());
    append = createAppend(row, fam, col, toAppend);

    _tableA.append(append);
    // Verify that cache size decreased by 1 (row:family)
    assertEquals((N) * FAMILIES.length - 1, cache.size());
    map = constructFamilyMap(fam, col);
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);
    assertEquals(VERSIONS, result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());

    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(VERSIONS, result.size());
    // Verify that all from cache
    assertEquals(VERSIONS, cache.getFromCache());
    restoreRow(2);
    LOG.error("Test append finished OK");
  }

  /**
   * Test increment.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testIncrement() throws IOException {

    LOG.error("Test incremenent started");
    byte[] row = "row_inc".getBytes();
    long inc = 10;
    LOG.error(" Increment row: " + new String(row));
    List<Cell> data = generateRowForIncrement();
    Put put = createPut(data);
    _tableA.put(put);
    // Verify that cache size not increased
    assertEquals((N) * FAMILIES.length, cache.size());

    Get get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    Result result = _tableA.get(get);
    assertEquals(FAMILIES.length * COLUMNS.length, result.size());
    // Verify that cache size increased
    assertEquals((N + 1) * FAMILIES.length, cache.size());

    Map<byte[], NavigableSet<byte[]>> familyMap =
        constructFamilyMap(Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
    Increment increment = createIncrement(row, familyMap, null, inc);
    cache.setTrace(true);
    _tableA.increment(increment);// Creates second version for every cell: row:fam:col

    cache.setTrace(false);
    // Verify that cache size decreased
    assertEquals((N) * FAMILIES.length, cache.size());

    get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);

    result = _tableA.get(get);

    // This is BUG or Feature in HBase 0.94.x - Append operation
    // deletes all versions of a cell except the last one
    assertEquals(FAMILIES.length * COLUMNS.length * 2, result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());
    // Do second get
    get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);
    assertEquals(FAMILIES.length * COLUMNS.length * 2, result.size());
    // Verify that not from cache
    assertEquals(FAMILIES.length * COLUMNS.length * 2, cache.getFromCache());
    // Check it is in cache
    assertEquals((N + 1) * FAMILIES.length, cache.size());

    LOG.error(" Increment row:family:column: " + new String(row) + ":" + new String(FAMILIES[0])
        + ":" + new String(COLUMNS[0]));

    _tableA.incrementColumnValue(row, FAMILIES[0], COLUMNS[0], inc);

    // Check cache size decreased by 1
    assertEquals((N + 1) * FAMILIES.length - 1, cache.size());

    get = new Get(row);
    get.addColumn(FAMILIES[0], COLUMNS[0]);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(3 /* now we have 3 versions of a cell */ , result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());

    get = new Get(row);
    get.addColumn(FAMILIES[0], COLUMNS[0]);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(3, result.size());
    // Verify that not from cache
    assertEquals(3, cache.getFromCache());
    // Delete row
    Delete delete = new Delete(row);
    _tableA.delete(delete);
    // Verify cache size
    assertEquals((N) * FAMILIES.length, cache.size());
    LOG.error("Test increment finished OK");
  }

  /**
   * Test check and put.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("deprecation")
  private void testCheckAndPut() throws IOException {
    int index = 4;
    LOG.error("Test checkAndPut started. Testing " + index + " row");

    byte[] row = getRow(data.get(index).get(0));
    LOG.error(" CheckAndPut row: " + new String(row));

    Put put = createPut(data.get(index));
    _tableA.checkAndPut(row, FAMILIES[0], COLUMNS[0], ("value" + index).getBytes(), put);

    // Verify that cache size
    assertEquals((N - 1) * FAMILIES.length, cache.size());
    Get get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    Result result = _tableA.get(get);
    assertEquals(data.get(index).size(), result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());
    // Do second get
    get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);
    assertEquals(data.get(index).size(), result.size());
    // Verify that not from cache
    assertEquals(data.get(index).size(), cache.getFromCache());
    // Check it is in cache
    assertEquals((N) * FAMILIES.length, cache.size());

    LOG.error(" CheckAndPut row:family: " + new String(row) + ":" + new String(FAMILIES[0]));
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);

    List<Cell> list = filter(data.get(index), FAMILIES[0], null);
    assertEquals(COLUMNS.length * VERSIONS, list.size());

    put = createPut(list);
    _tableA.checkAndPut(row, FAMILIES[0], COLUMNS[0], ("value" + index).getBytes(), put);
    // Verify that cache size decreased by 1 (row:family)
    assertEquals((N) * FAMILIES.length - 1, cache.size());

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, null);
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(COLUMNS.length * VERSIONS, result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());

    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(COLUMNS.length * VERSIONS, result.size());
    // Verify that all from cache
    assertEquals(COLUMNS.length * VERSIONS, cache.getFromCache());
    LOG.error(" CheckAndPut row:family:column: " + new String(row) + ":" + new String(FAMILIES[0])
        + ":" + new String(COLUMNS[0]));
    List<byte[]> col = new ArrayList<byte[]>();
    col.add(COLUMNS[0]);

    list = filter(data.get(index), FAMILIES[0], COLUMNS[0]);
    assertEquals(VERSIONS, list.size());
    put = createPut(list);
    _tableA.checkAndPut(row, FAMILIES[0], COLUMNS[0], ("value" + index).getBytes(), put);
    // Verify that cache size decreased by 1 (row:family)
    assertEquals((N) * FAMILIES.length - 1, cache.size());
    map = constructFamilyMap(fam, col);
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(VERSIONS, result.size());
    // Verify that not from cache
    assertEquals(0, cache.getFromCache());

    get = createGet(row, map, null, null);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals(VERSIONS, result.size());
    // Verify that all from cache
    assertEquals(VERSIONS, cache.getFromCache());

    LOG.error("Test checkAndPut finished OK");
  }

  /**
   * Test check and delete.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("deprecation")
  private void testCheckAndDelete() throws IOException {
    int index = 5;
    LOG.error("Test CaheckAndDelete started");
    byte[] row = getRow(data.get(index).get(0));
    LOG.error(" CheckAndDelete row: " + new String(row));

    Delete del = createDelete(row);
    _tableA.checkAndDelete(row, FAMILIES[0], COLUMNS[0], ("value" + index).getBytes(), del);

    // Verify size : 1 row = 3 KVs deleted
    assertEquals((N - 1) * FAMILIES.length, cache.size());
    Get get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    Result result = _tableA.get(get);
    assertTrue(result.isEmpty());
    assertEquals(0, cache.getFromCache());

    // Restore row back
    data.set(index, generateRowData(index));

    Put put = createPut(data.get(index));

    // dumpPut(put);

    _tableA.put(put);
    // _tableA.flushCommits();

    // Check it is not in cache yet
    assertEquals((N - 1) * FAMILIES.length, cache.size());
    // Load to cache
    get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);

    result = _tableA.get(get);
    assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
    // Check it is in cache
    assertEquals((N) * FAMILIES.length, cache.size());

    LOG.error(" CheckAndDelete row:family: " + new String(row) + ":" + new String(FAMILIES[0]));
    List<byte[]> fam = new ArrayList<byte[]>();
    fam.add(FAMILIES[0]);

    sleep(10);

    del = createDelete(row, fam);

    _tableA.checkAndDelete(row, FAMILIES[0], COLUMNS[0], ("value" + index).getBytes(), del);
    // _tableA.flushCommits();
    // Verify size : -1
    assertEquals((N) * FAMILIES.length - 1, cache.size());

    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, null);
    // Verify not in cache
    get = createGet(row, map, null, null);
    get.readVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertTrue(result.isEmpty());
    assertEquals(0, cache.getFromCache());
    // Verify that other families are in cache
    fam.clear();
    fam.add(FAMILIES[1]);
    fam.add(FAMILIES[2]);
    map = constructFamilyMap(fam, null);
    get = createGet(row, map, null, null);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);

    assertEquals((FAMILIES.length - 1) * COLUMNS.length * VERSIONS, result.size());
    // Verify all from cache
    assertEquals((FAMILIES.length - 1) * COLUMNS.length * VERSIONS, cache.getFromCache());

    // Delete row:family:col
    del = new Delete(row);
    del.addColumns(FAMILIES[1], COLUMNS[0]);

    boolean res = _tableA.checkAndDelete(row, FAMILIES[2], COLUMNS[0],
      ("value" + (index + 10000000)).getBytes(), del);
    assertFalse(res);
    _tableA.checkAndDelete(row, FAMILIES[2], COLUMNS[0], ("value" + index).getBytes(), del);

    // Verify what is still in cache (only FAMILY[2])
    fam.clear();
    fam.add(FAMILIES[1]);
    fam.add(FAMILIES[2]);
    map = constructFamilyMap(fam, null);
    get = createGet(row, map, null, null);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = _tableA.get(get);
    assertEquals((FAMILIES.length - 1) * COLUMNS.length * VERSIONS - (1) * VERSIONS, result.size());
    // Verify all from cache
    assertEquals((FAMILIES.length - 2) * COLUMNS.length * VERSIONS, cache.getFromCache());
    // Restore row
    restoreRow(index);
    LOG.error("Test CheckAndDelete finished OK");
  }

  private void testSimpleScan() throws IOException {
    LOG.info("Test simple scan started");
    byte[] startRow = getRow(data.get(0).get(0));
    byte[] stopRow = getRow(data.get(1).get(1));

    Scan scan = createSimpleScan(startRow, stopRow);

    ResultScanner scanner = _tableA.getScanner(scan);
    Iterator<Result> it = scanner.iterator();
    while (it.hasNext()) {
      it.next();
    }
    scanner.close();
  }

  /**
   * Generate row for increment.
   * @return the list
   */
  List<Cell> generateRowForIncrement() {
    byte[] row = "row_inc".getBytes();
    byte[] value = Bytes.toBytes(0L);
    long startTime = System.currentTimeMillis();
    ArrayList<Cell> list = new ArrayList<Cell>();

    for (byte[] f : FAMILIES) {
      for (byte[] c : COLUMNS) {
        KeyValue kv = new KeyValue(row, f, c, startTime, value);
        list.add(kv);
      }
    }
    Collections.sort(list, CellComparator.getInstance());
    return list;

  }

  /**
   * Sleep.
   * @param ms the ms
   */
  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Restore row.
   * @param n the n
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void restoreRow(int n) throws IOException {

    byte[] row = getRow(data.get(n).get(0));

    LOG.error("Restoring row: " + new String(row));

    Delete del = new Delete(row);
    _tableA.delete(del);

    sleep(10);

    data.set(n, generateRowData(n));
    Put put = createPut(data.get(n));
    _tableA.put(put);

    // Get data into cache
    Get get = createGet(row, null, null, null);
    get.readVersions(Integer.MAX_VALUE);
    Result r = _tableA.get(get);
    assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, r.size());
    assertEquals(0, cache.getFromCache());

    // Repeat request
    r = _tableA.get(get);
    assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, r.size());
    assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, cache.getFromCache());
    LOG.error("Restoring row: " + new String(row) + " done.");
  }

  /**
   * Test put delete put get.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unused")
  private void testPutDeletePutGet() throws IOException {
    long ts = System.currentTimeMillis();
    Put p = new Put("ROW".getBytes(), ts);
    p.addColumn("fam_a".getBytes(), "col_a".getBytes(), "value".getBytes());
    _tableA.put(p);

    Delete d = new Delete("ROW".getBytes());
    _tableA.delete(d);

    p = new Put("ROW".getBytes(), ts + 1000);
    p.addColumn("fam_a".getBytes(), "col_a".getBytes(), "value".getBytes());
    _tableA.put(p);

    // _tableA.flushCommits();

    Get get = new Get("ROW".getBytes());
    Result r = _tableA.get(get);
    assertFalse(r.isEmpty());

  }

  // public void testPutDeletePutGetOK() throws IOException
  // {
  // long ts = System.currentTimeMillis();
  // Put p = new Put("ROW".getBytes(), ts);
  // p.add("fam_a".getBytes(), "col_a".getBytes(), "value".getBytes());
  // _tableA.put(p);
  //
  // Delete d = new Delete("ROW".getBytes());
  // _tableA.delete(d);
  //
  // p = new Put("ROW".getBytes(), ts+1);
  // p.add("fam_a".getBytes(), "col_a".getBytes(), "value".getBytes());
  // _tableA.put(p);
  //
  // Get get = new Get("ROW".getBytes());
  // Result r = _tableA.get(get);
  // assertFalse( r.isEmpty());
  //
  //
  //
  // }
}
