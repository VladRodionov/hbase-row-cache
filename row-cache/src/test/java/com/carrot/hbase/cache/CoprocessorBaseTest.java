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
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrot.cache.util.CarrotConfig;
import com.carrot.hbase.cache.utils.IOUtils;;


/**
 * The Class CoprocessorLoadTest.
 */
public class CoprocessorBaseTest extends BaseTest {

	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(CoprocessorBaseTest.class);
	  
	/** The util. */
	private static HBaseTestingUtility UTIL = new HBaseTestingUtility();	
	
	/** The cp class name. */
	private static String CP_CLASS_NAME = RowCacheCoprocessor.class.getName();
	
	/** The cluster. */
	static MiniHBaseCluster cluster;
	
	/** The cache. */
	static RowCache cache;
	
	/** The _table c. */
	static HTable _tableA, _tableB, _tableC;
	
	/** Data directory */
	static Path dataDir;
	
	/* HBase cluster connection */
	static Connection conn;
	
	/* HBase Admin interface*/
	static Admin admin;
	
	static boolean dataLoaded = false;
	
	static boolean clusterStarted = false;
	/**
	 * Section to override
	 * 
	 */
	 /** The n. */
  static int N = 10000;
  
  static CacheType cacheType = CacheType.OFFHEAP;
  
  static long offheapCacheSize = 1L << 30;
  
  static long fileCacheSize = 1L << 37;
  
	
	@BeforeClass
  public static void setUp() throws Exception {
	  
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, CP_CLASS_NAME);
    conf.set("hbase.zookeeper.useMulti", "false");
    
    dataDir = Files.createTempDirectory("temp");
    
    // Cache configuration
    conf.set(CarrotConfig.CACHE_ROOT_DIR_PATH_KEY, dataDir.toString());
    // Set cache type to 'offheap'
    conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, cacheType.getType());
    
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP, CarrotConfig.CACHE_MAXIMUM_SIZE_KEY),
      Long.toString(offheapCacheSize));
    
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.CACHE_MAXIMUM_SIZE_KEY),
      Long.toString(fileCacheSize));
    
    // Enable snapshot
    UTIL.startMiniCluster(1);

    // Row Cache
    if (data != null) return;
    data = generateData(N);
    cluster = UTIL.getMiniHBaseCluster();
    createTables(VERSIONS);
    createHBaseTables();

    while (cache == null) {
      cache = RowCache.instance;
      Thread.sleep(1000);
      LOG.error("WAIT 1s for row cache to come up");
    }
    LOG.error("cache = " + cache);
  }
	
	
	/**
	 * Creates the hbase tables.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected static void createHBaseTables() throws IOException {		
		Configuration cfg = cluster.getConf();
		conn = ConnectionFactory.createConnection(cfg);
		admin = conn.getAdmin();
		
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

	@AfterClass
	public static void tearDown() throws Exception {
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
		if (dataLoaded) {
		  return;
		}
	  LOG.error ("Put all " + n +" rows  starts.");
		long start = System.currentTimeMillis();
		for(int i = 0; i < n; i++){
			Put put = createPut(data.get(i));
			table.put(put);
		}
		//table.flushCommits();
		dataLoaded = true;
		LOG.error ("Put all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
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
			Delete delete = createDelete(data.get(i));
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
	protected List<Cell> filter (List<Cell> list, byte[] fam, byte[] col)
	{
		List<Cell> newList = new ArrayList<Cell>();
		for(Cell kv: list){
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
}
