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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.hbase.cache.CacheType;
import com.carrotdata.hbase.cache.RowCache;
import com.carrotdata.hbase.cache.RowCacheConfig;
import com.carrotdata.hbase.cache.RowCacheCoprocessor;
import com.carrotdata.hbase.cache.utils.IOUtils;


/**
 * The Class CoprocessorLoadTest.
 */
public class CoprocessorCachePersistenceTest extends BaseTest{

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(CoprocessorCachePersistenceTest.class);
    
  /** The util. */
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();  
  
  /** The cp class name. */
  private static String CP_CLASS_NAME = RowCacheCoprocessor.class.getName();
  
  /** The n. */
  static int N = 10000;
  
  /** The cluster. */
  static MiniHBaseCluster cluster;
  
  /** The cache. */
  static RowCache cache;
  
  /** The _table c. */
  static HTable _tableA, _tableB, _tableC;
  
  static boolean loadOnStartup = false;
  
  Path dataDir;
  
  @Before
  public void setUp() throws Exception {

    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, CP_CLASS_NAME);
    conf.set("hbase.zookeeper.useMulti", "false");

    dataDir = Files.createTempDirectory("temp");
    
    // Cache configuration
    conf.set(CacheConfig.CACHE_DATA_DIR_PATHS_KEY, dataDir.toString());
    // Set cache type to 'offheap'
    conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, CacheType.OFFHEAP.getType());
    // set cache size to 1GB
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP, CacheConfig.CACHE_MAXIMUM_SIZE_KEY),
      Integer.toString(1 << 30));
    
    // Enable snapshot
    UTIL.startMiniCluster(1);
    
    // Row Cache
    if( data != null) return;   
    data = generateData(N);  
    cluster = UTIL.getMiniHBaseCluster();
    createTables(VERSIONS);
    createHBaseTables();
      
    while( cache == null){
      cache = RowCache.instance;
      Thread.sleep(1000);
      LOG.error("WAIT 1s for row cache to come up");
    }
    LOG.error("cache = "+cache);
      
  }
  
  /**
   * Creates the h base tables.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void createHBaseTables() throws IOException {   
    Configuration cfg = cluster.getConf();
    try (Connection conn = ConnectionFactory.createConnection(cfg);
         Admin admin = conn.getAdmin();){
    if( admin.tableExists(tableA.getTableName()) == false){
      admin.createTable(tableA);
      LOG.error("Created table "+tableA);
    }
    if( admin.tableExists(tableB.getTableName()) == false){
      admin.createTable(tableB);
      LOG.error("Created table "+tableB);
    } 
    if( admin.tableExists(tableC.getTableName()) == false){
      admin.createTable(tableC);
      LOG.error("Created table "+tableC);
    } 
    _tableA = (HTable) conn.getTable(TableName.valueOf(TABLE_A));
    _tableB = (HTable) conn.getTable(TableName.valueOf(TABLE_B));
    _tableC = (HTable) conn.getTable(TableName.valueOf(TABLE_C));
    }
  }

  @After
  public void tearDown() throws Exception {
      LOG.error("\n Tear Down the cluster and test \n");
      IOUtils.deleteRecursively(dataDir.toFile());
      Thread.sleep(2000);
      UTIL.shutdownMiniCluster();
  }
    
  /**
   * Put all data.
   *
   * @param table the table
   * @param n the n
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void putAllData(HTable table, int n) throws IOException
  {
    LOG.error ("Put all " + n +" rows  starts.");
    long start = System.currentTimeMillis();
    for(int i=0; i < n; i++){
      Put put = createPut(data.get(i));
      table.put(put);
    }
    //table.flushCommits();
    LOG.error ("Put all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
  }
  
  public void testFirstGet() throws IOException
  {
    
    LOG.error("Test first get started");
    
    long start = System.currentTimeMillis();
    for(int i=0 ; i< N; i++){   
      Get get = createGet(getRow(data.get(i).get(0)), null, null, null);
      
      
      get.readVersions(Integer.MAX_VALUE);
      Result result = _tableA.get(get);   
      //LOG.info(i+" Result is null = "+ result.isEmpty());
      List<Cell> list = result.listCells();
      assertEquals(data.get(i).size(), list.size()); 
      
      assertTrue(equalsNoTS(data.get(i), list));

      if(loadOnStartup == false){
        assertEquals(0, cache.getFromCache());
      } 
      
    }
    LOG.error("Test first get finished in "+(System.currentTimeMillis() - start)+"ms");
    
  }
  
  
  
  /**
   * Test second get.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void testSecondGet() throws IOException
  {
    
    LOG.error("Test second get started");
    
    long start = System.currentTimeMillis();
    for(int i=0 ; i< N; i++){   
      Get get = createGet(getRow(data.get(i).get(0)), null, null, null);
      get.readVersions(Integer.MAX_VALUE);
    
      Result result = _tableA.get(get);   
      List<Cell> list = result.listCells();
      assertEquals(data.get(i).size(), list.size());  
      assertEquals(list.size(), cache.getFromCache());
      assertTrue(equalsNoTS(data.get(i), list));
    }
    LOG.error("Test second get finished in "+(System.currentTimeMillis() - start)+"ms");
    
  }
  
  
  /**
   * Delete all data.
   *
   * @param table the table
   * @param n the n
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void deleteAllData(HTable table, int n) throws IOException
  {
    LOG.error ("Delete all " + n +" rows  starts.");
    long start = System.currentTimeMillis();
    for(int i=0; i < n; i++){
      Delete delete = createDelete(getRow(data.get(i).get(0)));
      delete.setTimestamp(start);
      table.delete(delete);
    }
    //table.flushCommits();
    LOG.error ("Delete all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
  }

  /**
   * Filter.
   *
   * @param list the list
   * @param fam the fam
   * @param col the col
   * @return the list
   */
  protected List<KeyValue> filter (List<KeyValue> list, byte[] fam, byte[] col)
  {
    List<KeyValue> newList = new ArrayList<KeyValue>();
    for(KeyValue kv: list){
      if(doFilter(kv, fam, col)){
        continue;
      }
      newList.add(kv);
    }
    return newList;
  }
  
  /**
   * Do filter.
   *
   * @param kv the kv
   * @param fam the fam
   * @param col the col
   * @return true, if successful
   */
  private final boolean doFilter(Cell kv, byte[] fam, byte[] col){
    if (fam == null) return false;
    byte[] f = getFamily(kv);
    if(Bytes.equals(f, fam) == false) return true;
    if( col == null) return false;
    byte[] c = getQualifier(kv);
    if(Bytes.equals(c, col) == false) return true;
    return false;
  }
  
  /**
   * Dump put.
   *
   * @param put the put
   */
  protected void dumpPut(Put put) {
    Map<byte[], List<Cell>> map = put.getFamilyCellMap();
    for(byte[] row: map.keySet()){
      List<Cell> list = map.get(row);
      for(Cell kv : list){
        LOG.error(kv);
      }
    }
  }
  
  public static void main(String[] args) throws Exception
  {
    CoprocessorCachePersistenceTest test = new CoprocessorCachePersistenceTest();
    test.setUp();
    if(CoprocessorCachePersistenceTest.loadOnStartup){
      assertEquals(3 *N, cache.size());
      LOG.info("Loaded "+ cache.size()+" objects into cache");
      //test.deleteAllData(CoprocessorCachePersistenceTest._tableA, CoprocessorCachePersistenceTest.N);
      //UTIL.compact(_tableA.getTableName(), true);
      //LOG.info("Cache size after deletion: "+cache.size());
      //assertEquals(0, cache.size());
    } else{
      LOG.info("No saved data found");
      test.putAllData(CoprocessorCachePersistenceTest._tableA, CoprocessorCachePersistenceTest.N);
    }   
    
    
    //test.deleteAllData(CoprocessorCachePersistenceTest._tableA, CoprocessorCachePersistenceTest.N);
    //test.putAllData(CoprocessorCachePersistenceTest._tableA, CoprocessorCachePersistenceTest.N);

    // done
    
    test.testFirstGet();

    test.testSecondGet();
    System.exit(0);
  }
}
