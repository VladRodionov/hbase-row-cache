/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrot.hbase.cache;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;

import com.carrot.cache.Cache;

import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.hbase.cache.utils.Hints;
import com.carrot.hbase.cache.utils.IOUtils;
import com.carrot.hbase.cache.utils.RequestContext;
import com.carrot.hbase.cache.utils.Utils;



/**
 * 
 * This is simplified version of ScanCache. 
 * 
 * 1) We always cache data on Get by CF (the entire column family is cached)
 * 2) We invalidate cached CF on every Put/Delete/Append/Increment operation if it involve cached CF
 *
 * TODO: There is one issue unresolved:
 * 
 *   1. Delete table or Delete column family MUST invalidate all the cache for the table
 *   
 *     Check preClose/preOpen, postClose/postOpen. I think when we disable/enable table all regions are closed/opened
 *     Yep, this is what we should do ...
 *     
 *     
 *  1. On preClose we add table (if it ROWCACHE) to the list of closed and record as well ALL its column families
 *  
 *  2. On preGet we need to check table if it is closed/disabled
 *  
 *  3. On preOpen we check table and ALL its families and compare them with those from the list of closed. If we detect
 *     deleted family (ROWCACHE) than we need to:
 *     
 *  A. temp disable cache and delete all records from cache for this CF   
 * 
 * 
 * Some ideas:
 * 
 * 1. Region HA - have two RS (master and slave) to serve the same region - HA
 * 2. Mitigate block cache purge on compaction                            - CAP (continuously available performance) 
 * 3. Cell level security and access control (from Accumulo)              - Security
 * 4. Minimize MTTR (faster failure detection + Region HA). Goal - sub-second  MTTR 
 * 5. Compaction storms?   Avoid network transfers (requires HDFS support)
 * 6. Region splits. Rolling splits?   
 * 7. Improve block locality (how - ask FB)
 * 8. BlockCache persistence SSD (use FIFO eviction)        
 * 
 * 9. Flushing & compactions (Acumulo compaction algorithm)
 *   *
 */

/**
 * TODO 
 * 1. Invalidate cache or do something on bulk load!!!
 * + 2. ScanCache configuration per CF (Done, but we need tool to set ROWCACHE per table/cf)
 * 3. store on shutdown, load on start up, periodic snapshots (LARGE)
 * 4. metrics, JMX - HBase integration (LARGE) 
 * 5. Smarter cache invalidation on a per table:family basis (MEDIUM)
 * 6. Cache configuration - ROWCACHE On/Off must be integrated into HBase console. (HBase patch) (MEDIUM)
 * 7. Separate tool to enable/disable ROWCACHE per table:family. (SMALL-MEDIUM)
 * 8. Row+filter cache. Filter must support ID (byte[] array). ???
 * 
 */


public class RowCache {

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(RowCache.class);
  /*
   *  Default buffer size is 256K (It does not make sense to cache rows larger
   *  than 256K anyway.
   */ 
  public final static int DEFAULT_BUFFER_SIZE = 256 * 1024;
  /*
   *  The byte buffer thread - local storage. 
   */
  private static ThreadLocal<ByteBuffer> bufTLS = new ThreadLocal<ByteBuffer>();
  /**
   * I/O buffer size
   */
  private static int ioBufferSize = DEFAULT_BUFFER_SIZE;
  /*
   * The Row Cache.
   * 
   * The single instance per region server
   * 
   */
  private static Cache rowCache;
  /*
   * The instance 
   */
  public static RowCache instance;
  
  /**
   * Cache type: offheap, disk or hybrid
   */
  private static CacheType cacheType;
  /*
   *  The disabled 
   */
  private static volatile AtomicBoolean disabled = new AtomicBoolean(false);
  /*
   *  Query (GET) context (thread local). 
   */
  private static ThreadLocal<RequestContext> contextTLS = new ThreadLocal<RequestContext>() {

    @Override
    protected RequestContext initialValue() {
      return new RequestContext();
    }

  };

  /*
   * List of pending tables in close state We need to persist it to be able to
   * handle serious failures TODO.
   */
  // private static TreeMap<byte[], List<byte[]>> pendingClose =
  // new TreeMap<byte[], List<byte[]>>(Bytes.BYTES_COMPARATOR);

  /*
   *  The mutations in progress. 
   *  Current number of a cache mutate operations 
   */
  private static AtomicLong mutationsInProgress = new AtomicLong(0);

  /*
   * Is cache persistent
   */
  private static boolean isPersistentCache = false;

  /*
   * The families TTL map. This is optimization, as since standard HBase API to
   * get CF's TTL is not very efficient We update this map on preOpen.
   * 
   */
  private static TreeMap<byte[], Integer> familyTTLMap = new TreeMap<byte[], Integer>(
      Bytes.BYTES_COMPARATOR);
  
  /** The trace. */
  private boolean trace = false;

  /**
   * Get cache type
   * @return cache type
   */
  public static CacheType getCacheType() {
    return cacheType;
  }
  
  public static void reset() {
    instance = null;
    //TODO: FIXME
    rowCache = null;
  }

  /**
   * 
   * Sets row cache disabled.
   * 
   * @param b the new disabled
   * @return true, if successful
   */
  public boolean setDisabled(boolean b) {
    return disabled.compareAndSet(!b, b);
  }

  /**
   * Checks if is disabled.
   * 
   * @return true, if is disabled
   */
  public boolean isDisabled() {
    return disabled.get();
  }

  /**
   * Sets the trace.
   * 
   * @param v
   *          the new trace
   */
  public void setTrace(boolean v) {
    trace = v;
  }

  /**
   * Checks if is trace.
   * 
   * @return true, if is trace
   */
  public boolean isTrace() {
    return trace;
  }

  private void setCacheType(CarrotConfig config, CacheType type) {
    if (type == CacheType.HYBRID) {
      addCacheType(config, CacheType.OFFHEAP);
      addCacheType(config, CacheType.FILE);
    } else {
      addCacheType(config, type);
    }
  }
  
  /**
   * Add single cache type (memory or disk)
   * @param confug
   * @param type
   */
  private void addCacheType(CarrotConfig config, CacheType type) {
    String[] names = config.getCacheNames();
    String[] types = config.getCacheTypes();
    
    String cacheName = type.getCacheName();
    String[] newNames = new String[names.length + 1];
    System.arraycopy(names, 0, newNames, 0, names.length);
    
    newNames[newNames.length - 1] = cacheName;
    
    String[] newTypes = new String[types.length + 1];
    System.arraycopy(types, 0, newTypes, 0, types.length);
    newTypes[newTypes.length - 1] = type.getType();
    String cacheNames = Utils.join(newNames, ",");
    String cacheTypes = Utils.join(newTypes, ",");
    config.setCacheNames(cacheNames);
    config.setCacheTypes(cacheTypes);
  }
  
  /**
   * Start co-processor - cache.
   * 
   * @param cfg
   *          the cfg
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void start(Configuration cfg) throws IOException {

    // Get all configuration from Configuration object
    // Start - load cache

    synchronized (RowCache.class) {
      if (rowCache != null)
        return;
      RowCacheConfig rowCacheConfig = RowCacheConfig.fromHadoopConfiguration(cfg);
      RowCache.isPersistentCache = rowCacheConfig.isCachePersistent();
      RowCache.ioBufferSize = rowCacheConfig.getIOBufferSize();
      cacheType = rowCacheConfig.getCacheType();  
      CarrotConfig carrotConfig = CarrotConfig.getInstance();
      setCacheType(carrotConfig, cacheType);
      loadRowCache();
      LOG.info("[row-cache] coprocessor started ");
      RowCache.instance = this;
      // Register shutdown hook
      registerShutdownHook();
    }
  }

  private void loadRowCache() throws IOException {
    CarrotConfig config = CarrotConfig.getInstance();
    if (isPersistentCache) {
      try {
        if (cacheType != CacheType.HYBRID) {
          rowCache = Cache.loadCache(cacheType.getCacheName());
          if (rowCache != null) {
            LOG.info(String.format("Loaded cache[%s] from the path: %s\n", rowCache.getName(),
              config.getCacheRootDir(rowCache.getName())));
          }
        } else {
          rowCache = Cache.loadCache(RowCacheConfig.CACHE_OFFHEAP_NAME);
          if (rowCache != null) {
            LOG.info(String.format("Loaded cache[%s] from the path: %s\n", rowCache.getName(),
              config.getCacheRootDir(rowCache.getName())));
            Cache victimCache = Cache.loadCache(RowCacheConfig.CACHE_FILE_NAME);
            if (victimCache != null) {
              rowCache.setVictimCache(victimCache);
              LOG.info(String.format("Loaded cache[%s] from the path: %s\n", victimCache.getName(),
                config.getCacheRootDir(victimCache.getName())));
            }
          }
        }
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }
    if (rowCache == null) {
      // Create new instance
      LOG.info("Creating new cache");
      if (cacheType != CacheType.HYBRID) {
        rowCache = new Cache(cacheType.getCacheName(), config);
        LOG.info(String.format("Created new cache[%s]\n", rowCache.getName()));
      } else {
        rowCache = new Cache(RowCacheConfig.CACHE_OFFHEAP_NAME, config);
        LOG.info(String.format("Created new cache[%s]\n", rowCache.getName()));
        Cache victimCache = new Cache(RowCacheConfig.CACHE_FILE_NAME, config);
        rowCache.setVictimCache(victimCache);
        LOG.info(String.format("Created new cache[%s]\n", victimCache.getName()));
        LOG.info(String.format("Type set to hybrid: cache[%s]->cache[%s]\n", 
          rowCache.getName(), victimCache.getName()));
      }
    }
    RowCacheConfig sconfig = RowCacheConfig.getInstance();
    boolean metricsEnabled = sconfig.isJMXMetricsEnabled();

    if (metricsEnabled) {
      String domainName = sconfig.getJMXDomainName();
      LOG.info(String.format("row-cache JMX enabled for data cache, domain=%s\n", domainName));
      rowCache.registerJMXMetricsSink(domainName);
      Cache victimCache = rowCache.getVictimCache();
      if (victimCache != null) {
        victimCache.registerJMXMetricsSink(domainName);
      }
    }
    if (cacheType != CacheType.HYBRID) {
      LOG.info(String.format("Initialized cache[%s]\n", rowCache.getName()));
    } else {
      LOG.info(String.format("Initialized hybrid cache: cache[%s]->cache[%s]\n", rowCache.getName(), 
        rowCache.getVictimCache().getName()));
    }
  }
  
  void saveRowCache() throws IOException {
    long start = System.currentTimeMillis();
    LOG.info(String.format("Shutting down cache[%s]\n", RowCacheConfig.CACHE_OFFHEAP_NAME));
    rowCache.shutdown();
    long end = System.currentTimeMillis();
    LOG.info(String.format("Shutting down cache[%s] done in %dms\n", RowCacheConfig.CACHE_OFFHEAP_NAME, (end - start)));
  }
  
  void deleteCacheData() {
    CarrotConfig config = CarrotConfig.getInstance();
    String offheapCachePath = config.getCacheRootDir(CacheType.OFFHEAP.getCacheName());
    // delete recursively
    File dir = new File(offheapCachePath);
    IOUtils.deleteRecursively(dir);
    if (cacheType == CacheType.HYBRID) {
      String fileCachePath =  config.getCacheRootDir(CacheType.FILE.getCacheName());
      dir = new File(fileCachePath);
      IOUtils.deleteRecursively(dir);
    }
  }
  
  private void registerShutdownHook() {

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        if (isPersistentCache) {
          LOG.info("Shutting down row-cache, saving data started ...");
          long startTime = System.currentTimeMillis();
          long totalRows = rowCache.size();
          long totalSize = rowCache.getStorageAllocated();
          try {
            rowCache.shutdown();
          } catch (IOException e) {
            LOG.error("Failed to save row-cache", e);
            return;
          }
          LOG.info("Saved " + StringUtils.byteDesc(totalSize) + " Total Rows:" + totalRows + " in "
              + (System.currentTimeMillis() - startTime) + " ms");
        } else {
          LOG.info("Shutting down row-cache, deleting data started ...");
          deleteCacheData();
          LOG.info("All data has been deleted successfully");
        }
      }
    });
    LOG.info("[row-cache] Registered shutdown hook");
  }

  private void checkLocalBufferAllocation() {
    if (bufTLS.get() != null)
      return;
    synchronized(this) {
      if (bufTLS.get() != null) {
        return;
      }
      bufTLS.set(ByteBuffer.allocate(RowCache.ioBufferSize));
    }
  }

  private final ByteBuffer getLocalBuffer() {
    checkLocalBufferAllocation();
    ByteBuffer buf = bufTLS.get();
    buf.clear();
    return buf;
  }

  /**
   * Stop.
   * 
   * @param e
   *          the e
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("rawtypes")
  public void stop(CoprocessorEnvironment e) throws IOException {
    // TODO - save row cache data?
    LOG.info("[row-cache] Stopping row-cache started ...");
    LOG.info("[row-cache] Stopping row-cache finished.");
  }

  /**
   * Just updates family TTL map
   */
  public void preOpen(TableDescriptor desc) throws IOException {

    if (isTrace()) {
      LOG.info("[row-cache] preOpen: " +desc.getTableName());
    }
    synchronized (familyTTLMap) {
      TableName tableName = desc.getTableName();
      ColumnFamilyDescriptor[] coldesc = desc.getColumnFamilies();
      for (ColumnFamilyDescriptor d : coldesc) {
        int ttl = d.getTimeToLive();
        byte[] key = Bytes.add(tableName.toBytes(), d.getName());
        familyTTLMap.put(key, ttl);
      }
    }
  }

  /**
   * Pre - bulkLoad HFile. Bulk load for CF with rowcache enabled is not a good
   * practice and should be avoided as since we clear all cache entries for this
   * CF (in a future) Currently, bulk load operation which involves at least one
   * cachable CF will result in an entire cached data loss.
   * 
   * @param tableDesc
   *          the table desc
   * @param familyPaths
   *          the family paths
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void preBulkLoadHFile(TableDescriptor tableDesc,
      List<Pair<byte[], String>> familyPaths) throws IOException {
    // we need to keep list of tables to cache and if this
    // TODO - OPTIMIZE!!!
    // This MUST be blocking operation
    // Disable cache for read operations only
    if (isDisabled()) {
      LOG.info("[row-cache][preBulkLoadHFile] Cache disabled, skip operation.");
      return;
    }

    List<byte[]> families = new ArrayList<byte[]>();
    for (Pair<byte[], String> p : familyPaths) {
      families.add(p.getFirst());
    }

    if (isRowCacheEnabledForFamilies(tableDesc, families) == false) {
      LOG.info("[row-cache][preBulkLoadHFile] skipped. No families cached.");
      return;
    }

    if (setDisabled(true) == false)
      return;
    // Run cache cleaning in a separate thread
    clearCache();

  }

  /**
   * Clear cache.
   */
  private void clearCache() {
    // TODO we can not clear cache w/o proper locking
    // as since we can not block delete operations on cache
    // during cache cleaning , but we can do this if we purge data entirely

    Runnable r = new Runnable() {
      public void run() {

        // Check if cache is disabled already, bail out if - yes
        // if(setDisabled(true) == false) return ;
        // Wait for all mutation operations to finish
        waitMutationsZero();

//        try {
//          //TODO: rowCache.clear();
//          rowCache.dispose();
//        } catch (IOException e) {
//          LOG.error("[row-cache] Failed to clear row-cache", e);
//        } finally {
//          setDisabled(false);
//        }
      }
    };

    Thread t = new Thread(r);
    t.start();
  }

  /**
   * Wait mutations zero.
   */
  protected static void waitMutationsZero() {

    while (mutationsInProgress.get() != 0) {
      try {
        Thread.sleep(1);// Sleep 1 ms
      } catch (Exception e) {
      }
    }
  }

  /**
   * Append operation works only for fully qualified columns (with
   * versions).
   * 
   * @param tableDesc
   *          the table desc
   * @param append
   *          the append
   * @return the result
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */

  public Result preAppend(TableDescriptor tableDesc, Append append)
      throws IOException {
    if (isDisabled())
      return null;
    try {
      mutationsInProgress.incrementAndGet();
      TableName tableName = tableDesc.getTableName();
      byte[] row = append.getRow();
      // TODO optimize
      Set<byte[]> families = append.getFamilyCellMap().keySet();
      // Invalidate list of family keys
      invalidateKeys(tableName, row, families);
      return null;
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   * Invalidate keys
   * 
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param families
   *          the families
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void invalidateKeys(TableName tableName, byte[] row, Set<byte[]> families)
      throws IOException {
    for (byte[] family : families) {
      delete(tableName, row, family, null);
    }
  }

  /**
   *  pre check and delete.
   * 
   * @param tableDesc
   *          the table desc
   * @param row
   *          the row
   * @param family
   *          the family
   * @param qualifier
   *          the qualifier
   * @param delete
   *          the delete
   * @param result
   *          the result
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public boolean preCheckAndDelete(TableDescriptor tableDesc, byte[] row,
      byte[] family, byte[] qualifier, Delete delete, boolean result)
      throws IOException {

    doDeleteOperation(tableDesc, delete);
    return result;
  }

  /**
   * TODO: we ignore timestamps and delete everything delete
   * 'family:column' deletes all versions delete 'family' - deletes entire
   * family delete 'row' - deletes entire row from cache
   * 
   * We ignore time range and timestamps when we do delete from cache.
   * 
   * @param tableDesc
   *          the table desc
   * @param delete
   *          the delete
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void doDeleteOperation(TableDescriptor tableDesc, Delete delete)
      throws IOException {

    if (isDisabled())
      return;

    try {
      mutationsInProgress.incrementAndGet();
      TableName tableName = tableDesc.getTableName();
      byte[] row = delete.getRow();

      Set<byte[]> families = delete.getFamilyCellMap().keySet();
      if (families.size() == 0) {
        // we delete entire ROW
        families = getFamiliesForTable(tableDesc);
      }
      // Invalidate list of family keys

      invalidateKeys(tableName, row, families);
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   *  Get set of family names.
   * 
   * @param desc
   *          the desc
   * @return the families for table
   */
  private Set<byte[]> getFamiliesForTable(TableDescriptor desc) {
    Set<byte[]> families = new HashSet<byte[]>();
    ColumnFamilyDescriptor[] fams = desc.getColumnFamilies();
    for (ColumnFamilyDescriptor cdesc : fams) {
      families.add(cdesc.getName());
    }
    return families;
  }

  /**
   * Generic delete operation for Row, Family, Column. It does not
   * report parent
   * 
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param family
   *          the family
   * @param column
   *          the column
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void delete(TableName tableName, byte[] row, byte[] family, byte[] column)
      throws IOException {
    ByteBuffer buf = getLocalBuffer(); 

    prepareKeyForGet(buf, tableName.getName(), row, 0, row.length, family, column);
    buf.flip();
    //long ptr = UnsafeAccess.address(buf);
    int size = buf.getInt();
    byte[] data = buf.array();
    rowCache.delete(data, 4, size);
  }

  /**
   * 
   * Post checkAndPut.
   * 
   * @param tableDesc
   *          the table desc
   * @param row
   *          the row
   * @param family
   *          the family
   * @param qualifier
   *          the qualifier
   * @param put
   *          the put
   * @param result
   *          the result
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */

  public boolean preCheckAndPut(TableDescriptor tableDesc, byte[] row,
      byte[] family, byte[] qualifier, Put put, boolean result)
      throws IOException {

    // Do put if result of check is true
    doPutOperation(tableDesc, put);
    return result;
  }

 
  /**
   * CHECKED 2 Do put operation.
   * 
   * @param tableDesc
   *          the table desc
   * @param put
   *          the put
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void doPutOperation(TableDescriptor tableDesc, Put put)
      throws IOException {

    // LOG.error("PrePUT executed \n ");
    // /*DEBUG*/dumpPut(put);

    if (isDisabled())
      return;

    try {
      mutationsInProgress.incrementAndGet();
      TableName tableName = tableDesc.getTableName();
      byte[] row = put.getRow();

      Set<byte[]> families = put.getFamilyCellMap().keySet();

      // Invalidate list of family keys

      invalidateKeys(tableName, row, families);
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   * Post get operation: 1. We update data in cache
   * 
   * @param tableDesc
   *          the table desc
   * @param get
   *          the get
   * @param results
   *          the results
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */

  public void postGet(TableDescriptor tableDesc, Get get,
      List<Cell> results) throws IOException {

    try {
      // TODO with postGet and disabled

      if (isDisabled()) {
        return;
      }

      // Check if we bypass cache
      RequestContext ctxt = contextTLS.get();
      if (ctxt.isBypassCache()) {
        // bypass
        return;
      }
      if(isTrace()) {
        LOG.info("[postGet] "+ get);
      }

      // 1. Make sure we sorted kv's out
      // TODO do we sorting?
      // FIXME: confirm that we always get results sorted
      // Collections.sort(results, KeyValue.COMPARATOR);
      // 2. Next iterate results by columnFamily
      List<Cell> bundle = new ArrayList<Cell>();
      byte[] row = get.getRow();
      for (int index = 0; index < results.size();) {
        index = processFamilyForPostGet(index, results, row, tableDesc, bundle);
        //LOG.info("postGet: index=" + index + " results.size=" + results.size() + " bundle.size=" + bundle.size());
        bundle.clear();
      }

    } finally {
      filterResults(results, tableDesc);
      resetRequestContext();
    }
  }

  /**
   * FIXME - optimize  Filter results in postGet.
   * 
   * @param results
   *          the results
   * @param tableDesc
   *          the table desc
 * @throws IOException 
   */
  private void filterResults(List<Cell> results, TableDescriptor tableDesc) throws IOException {
    // results are sorted
    if (results.size() == 0)
      return;

    int index = 0;
    byte[] family = getFamily(results.get(0));
    byte[] column = getColumn(results.get(0));
    // FIXME - optimize TTL
    int ttl = tableDesc.getColumnFamily(family).getTimeToLive();

    int count = 0;

    while (index < results.size()) {

      Cell kv = results.get(index);
      byte[] fam = getFamily(kv);
      byte[] col = getColumn(kv);//kv.getQualifierArray();
      if (Bytes.equals(family, fam) == false) {
        family = fam;
        ttl = tableDesc.getColumnFamily(family).getTimeToLive();
        count = 0;
      }
      if (Bytes.equals(column, col) == false) {
        column = col;
        count = 0;
      }
      count++;
      if (doFilter(kv, count) == false) {
        // check TTL
        if (kv.getTimestamp() < System.currentTimeMillis() - ((long) ttl)
            * 1000) {
          results.remove(index);
          continue;
        }
      } else {
        results.remove(index);
        continue;
      }
      index++;
    }

  }

  /**
   * Processing family in postGet. All K-V's are sorted and all K-V's
   * from the same family are arranged together.
   * 
   * @param index
   *          the index
   * @param results
   *          the results
   * @param row
   *          the row
   * @param tableDesc
   *          the table desc
   * @param bundle
   *          the bundle
   * @return the int
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private int processFamilyForPostGet(int index, List<Cell> results,
      byte[] row, TableDescriptor tableDesc, List<Cell> bundle)
      throws IOException {

    byte[] family = getFamily(results.get(index));
    return processFamilyColumnForAdd(index, results, tableDesc, row, family,
        bundle);
  }

  /**
   * This method actually inserts/updates KEY = 'table:rowkey:family'
   * with all versions in a given bundle. It updates family KEY =
   * 'table:rowkey:family' as well to keep track of all cached columns
   * 
   * @param index
   *          the index
   * @param results
   *          the results
   * @param tableDesc
   *          the table desc
   * @param row
   *          the row
   * @param family
   *          the family
   * @param bundle
   *          the bundle
   * @return the int
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private int processFamilyColumnForAdd(int index, List<Cell> results,
      TableDescriptor tableDesc, byte[] row, byte[] family,
      List<Cell> bundle) throws IOException {

    TableName tableName = tableDesc.getTableName();

    // 1. get column
    while (index < results.size()) {
      byte[] fam = getFamily(results.get(index));

      if (Bytes.equals(fam, family) != true) {
        // We finished family
        break;
      }

      byte[] col = getColumn(results.get(index));
      // scan results until we get other family, column
      for (; index < results.size(); index++) {
        Cell kv = results.get(index);
        int familyOffset = kv.getFamilyOffset();
        int familyLength = kv.getFamilyLength();
        int columnOffset = kv.getQualifierOffset();
        int columnLength = kv.getQualifierLength();

        if (Bytes.equals(col, 0, col.length, kv.getQualifierArray(), columnOffset,
            columnLength)
            && Bytes.equals(family, 0, family.length, kv.getFamilyArray(),
                familyOffset, familyLength)) {
          bundle.add(kv);
        } else {
          break;
        }
      }
    }
    // We do caching ONLY if ROWCACHE is set for 'table' or 'cf'
    if (isRowCacheEnabledForFamily(tableDesc, family)) {
      // Do only if it ROWCACHE is set for the family
      upsertFamilyColumns(tableName, row, family, bundle);
    }

    return index;
  }

  /**
   * TODO - optimize filters Do filter.
   * 
   * @param kv
   *          the kv
   * @param count
   *          the count
   * @return true, if successful
 * @throws IOException 
   */
  private boolean doFilter(Cell kv, int count) throws IOException {
    // 1. Check timeRange
    RequestContext context = contextTLS.get();
    TimeRange timeRange = context.getTimeRange();

    int maxVersions = context.getMaxVersions();
    Filter filter = context.getFilter();

    // We skip families and columns before we call filter
    // if(doFamilyMapFilter(kv, context) == true){
    // return true;
    // }
    byte[] family = new byte[kv.getFamilyLength()];
    byte[] column = new byte[kv.getQualifierLength()];
    System.arraycopy(kv.getFamilyArray(), kv.getFamilyOffset(), family, 0, family.length);
    System.arraycopy(kv.getQualifierArray(), kv.getQualifierOffset(), column, 0, column.length);
    if (shouldSkipColumn(family, column)) {
      return true;
    }
    if (timeRange != null) {
      if (timeRange.compare(kv.getTimestamp()) != 0) {
        return true;
      }
    }
    // 2. Check maxVersions
    if (count > maxVersions)
      return true;

    // 3. Check filter
    if (filter != null) {
      ReturnCode code = filter.filterCell(kv);
      if (code == ReturnCode.INCLUDE) {
        return false;
      } else if (code == ReturnCode.INCLUDE_AND_NEXT_COL) {
        // TODO we do not interrupt iteration. The filter
        // implementation MUST be tolerant
        return false;
      }
    } else {
      return false;
    }
    // Meaning : filter != null and filter op result is not success.
    return true;
  }

  /**
   * 
   * Sets the family column. Format:
   * 
   * // List of columns: // 4 bytes - total columns // Column: // 4 bytes -
   * qualifier length // qualifier // 4 bytes - total versions // list of
   * versions: // { 8 bytes - timestamp // 4 bytes - value length // value // }
   * 
   * @param tableName
   *          the table name
   * @param family
   *          the family
   * @param bundle
   *          the bundle
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void upsertFamilyColumns(TableName tableName, byte[] row, byte[] family,
      List<Cell> bundle) throws IOException {
    if (bundle.size() == 0) return;
    // Get first
    ByteBuffer buf = getLocalBuffer();

    try {
      prepareKeyForPut(buf, tableName.toBytes(), row, 0, row.length, family, null);
      // buffer position is at beginning of a value block
      int valueSize = Bytes.SIZEOF_INT;
      int numColumns = 0;
      int startPosition = buf.position();
      // Skip numColumns
      skip(buf, Bytes.SIZEOF_INT);
      while (bundle.size() > 0) {
        valueSize += addColumn(buf, bundle);
        numColumns++;
      }
      buf.putInt(startPosition, numColumns);
      // Update value len
      buf.putInt(4, valueSize);
      // Now we have K-V pair in a buffer - put it into cache
    } catch (BufferOverflowException e) {
      LOG.error("[row-cache] Ignore put op. The row:family is too large and exceeds the limit "
          + ioBufferSize + " bytes.");
      buf.clear();
      return;
    }
    doPut(buf);
  }

  /**
   * Add column.
   * 
   * @param buf
   *          the buf
   * @param bundle
   *          the bundle
   * @return the int
   */
  private int addColumn(ByteBuffer buf, List<Cell> bundle) {
    if (bundle.size() == 0)
      return 0;

    byte[] column = getColumn(bundle.get(0));
    byte[] col = column;
    buf.putInt(col.length);
    buf.put(col);
    int startPosition = buf.position();
    int totalVersions = 0;
    // size = col (4) + num versions (4) + col length
    int size = 2 * Bytes.SIZEOF_INT + col.length;
    // Skip total versions
    skip(buf, Bytes.SIZEOF_INT);

    while (Bytes.equals(column, col)) {
      size += addKeyValue(buf, bundle.get(0));
      totalVersions++;
      bundle.remove(0);
      if (bundle.size() == 0)
        break;
      col = getColumn(bundle.get(0));
    }
    // Put total versions
    buf.putInt(startPosition, totalVersions);
    return size;
  }


  /**
   * Do put.
   * 
   * @param kv
   *          the kv
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private final void doPut(ByteBuffer kv) throws IOException {
    //long ptr = UnsafeAccess.address(kv);
    kv.flip();
    int keySize = kv.getInt();//UnsafeAccess.toInt(ptr);
    int valueSize = kv.getInt();//.toInt(ptr + 4);
    byte[] buf = kv.array();
    boolean result = rowCache.put(buf, 8, keySize, buf, 8 + keySize, valueSize, 0);   
  }

  /**
   * Adds the key value (KeyValue) to a buffer for Put/Append.
   * 
   * @param buf
   *          the buf
   * @param kv
   *          the kv
   * @return the int
   */
  private int addKeyValue(ByteBuffer buf, Cell kv) {

    // Format:
    // 8 bytes - ts
    // 4 bytes - value length
    // value blob
    int valLen = kv.getValueLength();
    int size = 12 + valLen;
    buf.putLong(kv.getTimestamp());
    buf.putInt(valLen);
    buf.put(kv.getValueArray(), kv.getValueOffset(), valLen);
    return size;
  }

  /**
   * This involves an array copy.
   * 
   * @param kv
   *          the kv
   * @return - column name
   */
  private byte[] getColumn(Cell kv) {
    int off = kv.getQualifierOffset();
    int len = kv.getQualifierLength();
    byte[] buf = new byte[len];
    System.arraycopy(kv.getQualifierArray(), off, buf, 0, len);
    return buf;
  }

  /**
   * This involves an array copy.
   * 
   * @param kv
   *          the kv
   * @return the family
   */
  private final byte[] getFamily(Cell kv) {
    int off = kv.getFamilyOffset();
    int len = kv.getFamilyLength();
    byte[] buf = new byte[len];
    System.arraycopy(kv.getFamilyArray(), off, buf, 0, len);
    return buf;
  }

  /**
   * Post increment.
   * 
   * @param tableDesc
   *          the table desc
   * @param increment
   *          the increment
   * @param result
   *          the result
   * @return the result
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public Result preIncrement(TableDescriptor tableDesc, Increment increment,
      Result result) throws IOException {

    if (isTrace()) {
      LOG.info("[row-cache] preIncrement: " + increment + " disabled="
          + isDisabled());
    }
    if (isDisabled()) {
      return null;
    }
    try {
      mutationsInProgress.incrementAndGet();
      // if(result == null || result.isEmpty()) return result;
      TableName tableName = tableDesc.getTableName();
      byte[] row = increment.getRow();
      Set<byte[]> families = increment.getFamilyCellMap().keySet();
      // Invalidate list of family keys
      invalidateKeys(tableName, row, families);
      return result;
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   *  Post increment column value.
   * 
   * @param tableDesc
   *          the table desc
   * @param row
   *          the row
   * @param family
   *          the family
   * @param qualifier
   *          the qualifier
   * @return the long
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public long preIncrementColumnValue(TableDescriptor tableDesc, byte[] row,
      byte[] family, byte[] qualifier) throws IOException {

    if (isDisabled())
      return 0;
    try {
      mutationsInProgress.incrementAndGet();

      TableName tableName = tableDesc.getTableName();
      // Invalidate family - column
      delete(tableName, row, family, null);
      return 0;
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   * Post delete.
   * 
   * @param tableDesc
   *          the table desc
   * @param delete
   *          the delete
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void preDelete(TableDescriptor tableDesc, Delete delete)
      throws IOException {
    // if(RowCache.isDisabled()) return ;
    doDeleteOperation(tableDesc, delete);
  }

  /**
   * TODO : optimize Pre exists call.
   * 
   * @param tableDesc
   *          the table desc
   * @param get
   *          the get
   * @param exists
   *          the exists
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */

  public boolean preExists(TableDescriptor tableDesc, Get get, boolean exists)
      throws IOException {

    if (isDisabled())
      return exists;

    List<Cell> results = new ArrayList<Cell>();
    try {
      preGet(tableDesc, get, results);
      boolean result = results.size() > 0 ? true : false;
      return result;
    } finally {
      resetRequestContext();
    }
  }

  /**
   * 
   * Skip byte buffer
   * 
   * @param buf
   *          the buf
   * @param skip
   *          the skip
   */
  private final void skip(ByteBuffer buf, int skip) {
    buf.position(buf.position() + skip);
  }

  /**
   * Reads content of table:row:family.
   * 
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param columnFamily
   *          the column family
   * @return the list
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private List<Cell> readFamily(byte[] tableName, byte[] row,
      byte[] columnFamily) throws IOException {
    List<Cell> result = new ArrayList<Cell>();
    ByteBuffer buf = getLocalBuffer();
//    long bufptr = UnsafeAccess.address(buf);
//    
//    /*DEBUG*/
//    if (bufptr == 0) {
//      LOG.error("bufptr=0");
//    }
    prepareKeyForGet(buf, tableName, row, 0, row.length, columnFamily, null);
    byte[] bbuf = buf.array();

    int keySize = UnsafeAccess.toInt(bbuf, 0);
 
    //buf.position(keySize + 4);
    int off = 4 + keySize;
    //int avail = buf.capacity() - buf.position();
    int avail = bbuf.length - off;
    long len = rowCache.get(bbuf, 4, keySize, true, bbuf, off);
    if (len > avail) {
      //TODO 
      throw new IOException("buffer overflow, avail=" + avail + " required="+ len);
    }
    if (len < 0) {
      // NOT FOUND Return empty list
      return result;
    }

    // Now we have in buffer:
    // 4 bytes - total length
    // 8 bytes - address
    // List of columns:
    // 4 bytes - total columns
    // Column:
    // 4 bytes - qualifier length
    // qualifier
    // 4 bytes - total versions
    // list of versions:
    // { 8 bytes - timestamp
    // 4 bytes - value length
    // value
    // }
    //bufptr += 4 + keySize;
    int totalColumns = UnsafeAccess.toInt(bbuf, off);
    int i = 0;

    off += 4;
    while (i++ < totalColumns) {
      off = readColumn(bbuf, off, row, columnFamily, result);
    }

    return result;
  }


  /**
   * We skip column which is not a part of a request.
   * 
   * @param buf
   *          the buf
   * @return the long
   */
//  private long skipColumn(long buf) {
//
//    int csize = UnsafeAccess.toInt(buf);
//    buf += 4 + csize;
//
//    int totalVersions = UnsafeAccess.toInt(buf);
//    buf += 4;
//    int i = 0;
//
//    while (i++ < totalVersions) {
//      buf += 8;
//      int valueSize = UnsafeAccess.toInt(buf);
//      buf += 4 + valueSize;
//    }
//    return buf;
//  }

  
  /**
   * We skip column which is not a part of a request.
   * 
   * @param buf
   *          the buf
   * @return the long
   */
  private int skipColumn(byte[] buf, int off) {

    int csize = UnsafeAccess.toInt(buf, off);
    off += 4 + csize;

    int totalVersions = UnsafeAccess.toInt(buf, off);
    off += 4;
    int i = 0;

    while (i++ < totalVersions) {
      off += 8;
      int valueSize = UnsafeAccess.toInt(buf, off);
      off += 4 + valueSize;
    }
    return off;
  }

  /**
   * 
   * Read column
   * 
   * @param buf
   *          the buf
   * @param row
   *          the row
   * @param family
   *          the family
   * @param result
   *          the result
   * @return the list
 * @throws IOException 
   */
//  private long readColumn(long bufptr, byte[] row, byte[] family,
//      List<Cell> result) throws IOException {
//
//    // Column format
//    // Column:
//    // 4 bytes - qualifier length
//    // qualifier
//    // 4 bytes - total versions
//    // list of versions:
//    // { 8 bytes - timestamp
//    // 4 bytes - value length
//    // value
//    // }
//
//    int csize = UnsafeAccess.toInt(bufptr);
//    byte[] column = new byte[csize];
//
//    UnsafeAccess.copy(bufptr + 4, column, 0, csize);
//    
//    bufptr += 4 + csize;
//    // boolean skip =
//    if (shouldSkipColumn(family, column)) {
//      bufptr -= 4 + csize;
//      bufptr = skipColumn(bufptr);
//      return bufptr;
//    }
//
//    int totalVersions = UnsafeAccess.toInt(bufptr);
//    bufptr += 4;
//    int i = 0;
//    RequestContext context = contextTLS.get();
//    TimeRange timeRange = context.getTimeRange();
//
//    int maxVersions = context.getMaxVersions();
//    Filter filter = context.getFilter();
//
//    while (i++ < totalVersions) {
//      KeyValue kv = null;
//      // Read ts
//      long ts = UnsafeAccess.toLong(bufptr);
//      bufptr += 8;
//      if (timeRange != null) {
//        if (timeRange.compare(ts) != 0) {
//          bufptr = skipKeyValue(bufptr);
//          continue;
//        }
//      }
//      // 2. Check maxVersions
//      if (i > maxVersions) {
//        bufptr = skipKeyValue(bufptr);
//        continue;
//      }
//
//      // 3. Read value
//
//      int size = UnsafeAccess.toInt(bufptr);
//      byte[] value = new byte[size];
//      UnsafeAccess.copy(bufptr + 4, value, 0, size);
//      bufptr += 4 + size;
//
//      kv = new KeyValue(row, family, column, ts, value);
//
//      if (filter != null) {
//        ReturnCode code = filter.filterCell(kv);
//        if (code == ReturnCode.INCLUDE) {
//          result.add(kv);
//        } else if (code == ReturnCode.INCLUDE_AND_NEXT_COL) {
//          // TODO we do not interrupt iteration. The filter
//          // implementation MUST be tolerant
//          result.add(kv);
//        }
//      } else {
//        result.add(kv);
//      }
//    }
//    return bufptr;
//  }

  /**
   * Read column
   * @param buf the buf
   * @param off offset in the buffer
   * @param row the row
   * @param family the family
   * @param result the result
   * @return new offset (or total read?)
   * @throws IOException
   */
  private int readColumn(byte[] buf, int off,  byte[] row, byte[] family,
      List<Cell> result) throws IOException {

    // Column format
    // Column:
    // 4 bytes - qualifier length
    // qualifier
    // 4 bytes - total versions
    // list of versions:
    // { 8 bytes - timestamp
    // 4 bytes - value length
    // value
    // }

    int csize = UnsafeAccess.toInt(buf, off);
    byte[] column = new byte[csize];
    off += 4;
    UnsafeAccess.copy(buf, off, column, 0, csize);
    
    off += csize;
    // boolean skip =
    if (shouldSkipColumn(family, column)) {
      off -= 4 + csize;
      off = skipColumn(buf, off);
      return off;
    }

    int totalVersions = UnsafeAccess.toInt(buf, off);
    off += 4;
    int i = 0;
    RequestContext context = contextTLS.get();
    TimeRange timeRange = context.getTimeRange();

    int maxVersions = context.getMaxVersions();
    Filter filter = context.getFilter();

    while (i++ < totalVersions) {
      KeyValue kv = null;
      // Read ts
      long ts = UnsafeAccess.toLong(buf, off);
      off += 8;
      if (timeRange != null) {
        if (timeRange.compare(ts) != 0) {
          off = skipKeyValue(buf, off);
          continue;
        }
      }
      // 2. Check maxVersions
      if (i > maxVersions) {
        off = skipKeyValue(buf, off);
        continue;
      }

      // 3. Read value

      int size = UnsafeAccess.toInt(buf, off);
      byte[] value = new byte[size];
      off += 4;
      UnsafeAccess.copy(buf, off, value, 0, size);
      off += size;

      kv = new KeyValue(row, family, column, ts, value);

      if (filter != null) {
        ReturnCode code = filter.filterCell(kv);
        if (code == ReturnCode.INCLUDE) {
          result.add(kv);
        } else if (code == ReturnCode.INCLUDE_AND_NEXT_COL) {
          // TODO we do not interrupt iteration. The filter
          // implementation MUST be tolerant
          result.add(kv);
        }
      } else {
        result.add(kv);
      }
    }
    return off;
  }

  
//  /**
//   * Skip key value.
//   * 
//   * @param bufptr
//   *          the bufptr
//   * @param unsafe
//   *          the unsafe
//   * @return the long
//   */
//  private final long skipKeyValue(long bufptr) {
//    return bufptr + 4 + UnsafeAccess.toInt(bufptr);
//  }

  /**
   * Skip key value.
   * 
   * @param bufptr
   *          the bufptr
   * @param unsafe
   *          the unsafe
   * @return the long
   */
  private final int skipKeyValue(byte[] buf, int off) {
    return off + 4 + UnsafeAccess.toInt(buf, off);
  }
  
  /**
   * FIXME - optimize.
   * 
   * @param family
   *          the family
   * @param column
   *          the column
   * @return true, if successful
   */
  private boolean shouldSkipColumn(byte[] family, byte[] column) {
    RequestContext context = contextTLS.get();
    Map<byte[], NavigableSet<byte[]>> map = context.getFamilyMap();
    NavigableSet<byte[]> cols = map.get(family);
    if (cols == null || cols.size() == 0)
      return false;
    return cols.contains(column) == false;
  }

  /**
   * Pre get.
   * 
   * @param desc
   *          the desc
   * @param get
   *          the get
   * @param results
   *          the results
   * @return true if bypass HBase, false otherwise
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public boolean preGet(TableDescriptor desc, Get get, List<Cell> results)
      throws IOException {

    if (isDisabled())
      return false;
    if (isTrace()){
      LOG.info("[row-cache][trace][preGet]: " + get);
    }
    updateRequestContext(get);

    RequestContext ctxt = contextTLS.get();
    if (isTrace()) {
      Map<byte[], NavigableSet<byte[]>> map = ctxt.getFamilyMap();
      for (byte[] f : map.keySet()) {
        NavigableSet<byte[]> set = map.get(f);
        LOG.info("[row-cache] " + new String(f) + " has " + (set != null? map.get(f).size(): null)
            + " columns");
      }
    }
    if (ctxt.isBypassCache()) {
      // bypass
      return false;
    }

    byte[] tableName = desc.getTableName().toBytes();

    Set<byte[]> set = get.familySet();

    if (get.hasFamilies() == false) {
      set = getFamiliesForTable(desc);
      addFamilies(get, set);
    }

    List<byte[]> toDelete = new ArrayList<byte[]>();

    for (byte[] columnFamily : set) {
      if (isRowCacheEnabledForFamily(desc, columnFamily)) {
        if (processFamilyPreGet(desc, get, tableName, columnFamily, results)) {
          toDelete.add(columnFamily);
        }
      }

    }

    // Delete families later to avoid concurrent modification exception
    deleteFrom(get, toDelete);

    if (isTrace()){
      LOG.info("[row-cache][trace][preGet] found " + results.size());
    }
    // DEBUG ON
    fromCache = results.size();
    // DEBUG OFF
    // Now check if we need bypass() HBase?
    // 1. When map.size == 0 (Get is empty - then we bypass HBase)
    if (get.getFamilyMap().size() == 0) {
      // Reset request context
      if(isTrace()){
        LOG.error("PreGet bypass HBase");
      }
      resetRequestContext();
      return true;
    }
    if (isTrace())
      LOG.info("[row-cache][trace][preGet]: send to HBase " + get);
    // Finish preGet
    return false;
  }

  /**
   * Delete from.
   * 
   * @param get
   *          the get
   * @param toDelete
   *          the to delete
   */
  private void deleteFrom(Get get, List<byte[]> toDelete) {
    Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
    for (byte[] f : toDelete) {
      map.remove(f);
    }
  }

  /**
   * Process family pre get.
   * 
   * @param desc
   *          the desc
   * @param get
   *          the get
   * @param tableName
   *          the table name
   * @param columnFamily
   *          the column family
   * @param results
   *          the results
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private boolean processFamilyPreGet(TableDescriptor desc, Get get,
      byte[] tableName, byte[] columnFamily, List<Cell> results)
      throws IOException {

    Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
    byte[] row = get.getRow();

    if (desc.hasColumnFamily(columnFamily) == false) {
      // TODO, do we have to remove columnFamily?
      // If family does not exists - remove family from Get request
      map.remove(columnFamily);
      return true;
    }

    // FIXME - This is not safe
    byte[] key = Bytes.add(tableName, columnFamily);
    Integer ttl = null;
    synchronized (familyTTLMap) {
      ttl = familyTTLMap.get(key);
      if (ttl == null || ttl == 0) {
        ColumnFamilyDescriptor cdesc = desc.getColumnFamily(columnFamily);
        ttl = cdesc.getTimeToLive();
        familyTTLMap.put(key, ttl);
      }
    }

    boolean foundFamily = false;
    List<Cell> res = readFamily(tableName, row, columnFamily);
    foundFamily = res.size() > 0;
    res = filterTTL(res, ttl);
    results.addAll(res);
    return foundFamily;
  }

  /**
   * Filter TTL
   * 
   * @param list
   *          the list
   * @param ttl
   *          the ttl
   * @return the list
   */
  private List<Cell> filterTTL(List<Cell> list, int ttl) {

    long oldestTimestamp = System.currentTimeMillis() - ((long) ttl) * 1000;
    for (int i = 0; i < list.size(); i++) {
      Cell kv = list.get(i);
      if (kv.getTimestamp() < oldestTimestamp) {
        list.remove(i);
        i--;
      }
    }
    return list;
  }

  /**
   * Add families
   * 
   * @param get
   *          the get
   * @param families
   *          the families
   */

  private void addFamilies(Get get, Set<byte[]> families) {
    for (byte[] fam : families) {
      get.addFamily(fam);
    }
  }

  /**
   * Resets all filter conditions in a current Get operation to a
   * default and max values This will get us ALL cell versions from HBase to
   * keep in memory Make sure that you are aware of consequences.
   * 
   * @param get
   *          - Get operation
   */
  private void updateRequestContext(Get get) {

    RequestContext context = contextTLS.get();
    boolean bypassCache = Hints.bypassCache(get, true);
    context.setBypassCache(bypassCache);

    context.setFamilyMap(get.getFamilyMap());

    context.setFilter(get.getFilter());
    get.setFilter(null);
    context.setTimeRange(get.getTimeRange());
    try {
      get.setTimeRange(0L, Long.MAX_VALUE);
      context.setMaxVersions(get.getMaxVersions());
      get.readVersions(Integer.MAX_VALUE);
    } catch (IOException e) {
    }

  }

  /**
   * After preGet if successful or postGet. We need to make it public
   * API for testing only
   */
  public void resetRequestContext() {

    RequestContext context = contextTLS.get();

    context.setFilter(null);
    context.setMaxVersions(Integer.MAX_VALUE);
    context.setTimeRange(null);
    context.setBypassCache(false);
    context.setFamilyMap(null);

  }

  /**
   * Prepare key for Get op.
   * 
   * @param buf
   *          the buf
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param offset
   *          the offset
   * @param size
   *          the size
   * @param columnFamily
   *          the column family
   * @param column
   *          the column
   */
  private void prepareKeyForGet(ByteBuffer buf, byte[] tableName, byte[] row,
      int offset, int size, byte[] columnFamily, byte[] column) {

    buf.clear();
    int totalSize = 2 + tableName.length + // table
        2 + size + // row
        ((columnFamily != null) ? (2 + columnFamily.length) : 0) + // family
        ((column != null) ? (4 + column.length) : 0); // column
    buf.putInt(totalSize);
    // 4 bytes to keep key length;
    buf.putShort((short) tableName.length);
    buf.put(tableName);
    buf.putShort((short) size);
    buf.put(row, offset, size);
    if (columnFamily != null) {
      buf.putShort((short) columnFamily.length);
      buf.put(columnFamily);
    }
    if (column != null) {
      buf.putInt(column.length);
      buf.put(column);
    }
    // prepare for read
    // buf.flip();

  }

  /**
   * Prepare key for get.
   * 
   * @param buf
   *          the buf
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param offset
   *          the offset
   * @param size
   *          the size
   * @param columnFamily
   *          the column family
   * @param column
   *          the column
   */
//  private void prepareKeyForGet(long buf, byte[] tableName, byte[] row,
//      int offset, int size, byte[] columnFamily, byte[] column) {
//
//    int totalSize = 2 + tableName.length + // table
//        2 + size + // row
//        ((columnFamily != null) ? (2 + columnFamily.length) : 0) + // family
//        ((column != null) ? (4 + column.length) : 0); // column
//    UnsafeAccess.putInt(buf, totalSize);
//    buf += 4;
//    // 4 bytes to keep key length;
//    UnsafeAccess.putShort(buf, (short) tableName.length);
//    buf += 2;
//    UnsafeAccess.copy(tableName, 0, buf, tableName.length);
//    buf += tableName.length;
//    UnsafeAccess.putShort(buf, (short) size);
//    buf += 2;
//    UnsafeAccess.copy(row, offset, buf, size);
//    buf += size;
//
//    if (columnFamily != null) {
//      UnsafeAccess.putShort(buf, (short) columnFamily.length);
//      buf += 2;
//      UnsafeAccess.copy(columnFamily, 0, buf, columnFamily.length);
//      buf += columnFamily.length;
//    }
//    if (column != null) {
//      UnsafeAccess.putShort(buf, (short) column.length);
//      buf += 2;
//      UnsafeAccess.copy(column, 0, buf, column.length);
//      buf += column.length;
//    }
//    // prepare for read
//    // buf.flip();
//
//  }

  /**
   * Prepare key for Get op.
   * 
   * @param buf
   *          the buf
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param offset
   *          the offset
   * @param size
   *          the size
   * @param columnFamily
   *          the column family
   * @param column
   *          the column
   */
  private void prepareKeyForPut(ByteBuffer buf, byte[] tableName, byte[] row,
      int offset, int size, byte[] columnFamily, byte[] column) {

    buf.clear();
    int totalSize = 2 + tableName.length + // table
        2 + size + // row
        ((columnFamily != null) ? (2 + columnFamily.length) : 0) + // family
        ((column != null) ? (4 + column.length) : 0); // column
    buf.putInt(totalSize);
    // 4 bytes to keep key length;
    // skip 4 bytes for Value length
    buf.position(8);
    buf.putShort((short) tableName.length);
    buf.put(tableName);
    buf.putShort((short) size);
    buf.put(row, offset, size);
    if (columnFamily != null) {
      buf.putShort((short) columnFamily.length);
      buf.put(columnFamily);
    }
    if (column != null) {
      buf.putInt(column.length);
      buf.put(column);
    }
    // prepare for read
    // buf.flip();
  }

 
  /**
   * Post put - do put operation.
   * 
   * @param tableDesc
   *          the table desc
   * @param put
   *          the put
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void prePut(TableDescriptor tableDesc, Put put) throws IOException {
    doPutOperation(tableDesc, put);
  }

  /**
   * Checks if is row cache enabled for family.
   * 
   * @param tableDesc
   *          the table desc
   * @param family
   *          the family
   * @return true, if is row cache enabled for family
   */
  private final boolean isRowCacheEnabledForFamily(TableDescriptor tableDesc,
      byte[] family) {
    ColumnFamilyDescriptor colDesc = tableDesc.getColumnFamily(family);
    // Its possible if request CF which does not exists
    if (colDesc == null)
      return false;

    byte[] value = colDesc.getValue(RConstants.ROWCACHE);
    if (value != null) {
      return Bytes.equals(value, RConstants.TRUE);
    }
    // else check tableDesc
    value = tableDesc.getValue(RConstants.ROWCACHE);
    if (value != null) {
      return Bytes.equals(value, RConstants.TRUE);
    }
    return false;
  }

  /**
   * Checks if is row cache enabled for family.
   * 
   * @param tableDesc
   *          the table desc
   * @param colDesc
   *          the col desc
   * @return true, if is row cache enabled for family
   */
  @SuppressWarnings("unused")
  private final boolean isRowCacheEnabledForFamily(TableDescriptor tableDesc,
      ColumnFamilyDescriptor colDesc) {

    byte[] value = colDesc.getValue(RConstants.ROWCACHE);
    if (value != null) {
      return Bytes.equals(value, RConstants.TRUE);
    }
    // else check tableDesc
    value = tableDesc.getValue(RConstants.ROWCACHE);
    if (value != null) {
      return Bytes.equals(value, RConstants.TRUE);
    }
    return false;
  }

  /**
   * Checks if is row cache enabled for families.
   * 
   * @param tableDesc
   *          the table desc
   * @param families
   *          the families
   * @return true, if is row cache enabled for families
   */
  private final boolean isRowCacheEnabledForFamilies(
      TableDescriptor tableDesc, List<byte[]> families) {
    for (byte[] family : families) {
      if (isRowCacheEnabledForFamily(tableDesc, family)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Access to underlying off-heap cache.
   * 
   * @return the cache
   */

  public Cache getCache() {
    return rowCache;
  }

  /**
   * Size.
   * 
   * @return the long
   */
  public long size() {
    return rowCache.activeSize();
  }

  /** DEBUG interface. */

  int fromCache;

  /**
   * Gets the from cache.
   * 
   * @return the from cache
   */
  public int getFromCache() {
    return fromCache;
  }
}
