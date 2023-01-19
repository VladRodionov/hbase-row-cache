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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.carrot.cache.Cache;
import com.carrot.cache.Scavenger;
import com.carrot.cache.controllers.AQBasedAdmissionController;
import com.carrot.cache.io.Segment;
import com.carrot.cache.io.SegmentScanner;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Utils;
import com.carrot.hbase.cache.utils.IOUtils;
//import com.carrot.sidecar.file.SidecarLocalFileSystem;

/**
 * HBase + RowCache multi-threaded performance test
 */
public class HBasePerfTest {

  static enum WorkloadType {
    UNIFORM(0.0),
    ZIPFIAN(0.9);
    
    double zipfAlpha;
    
    WorkloadType(double zipfAlpha){
      this.zipfAlpha = zipfAlpha;
    }
    
    public double getZipfAlpha() {
      return zipfAlpha;
    }
    
    public static WorkloadType defaultType() {
      return WorkloadType.UNIFORM;
    }
  }
  
  /** The Constant THREADS. */
  private final static String THREADS = "-t";

  /** The Constant MAXMEMORY. */
  private final static String MAXMEMORY = "-mm";

  /** The Constant WRITE_RATIO. */
  private final static String WRITE_RATIO = "-writes"; // write ops %%

  /** The Constant DURATION. */
  private final static String DURATION = "-duration";

  /** The seq number. */
  private static AtomicLong seqNumber = new AtomicLong(0);

  /** Data size. */
  private static int M = 10000;
  /** The Constant LOG. */
  private final static Logger LOG = Logger.getLogger(PerfTest.class);

  /** The test time. */
  private static long testTime = 3000000;// 3000 secs

  /** The write ratio. */
  private static float writeRatio = 0.1f; // 10% puts - 90% gets

  /** Statistics tread interval in ms */
  private static long statsInterval = 5000;

  /** File cache limit in bytes */
  private static long fileCacheSizeLimit = 50 * (1L << 30); // 50G by default

  /** File cache segment size */
  private static long fileCacheSegmentSize = 64 * (1 << 20); // 64MB
  
  /** Cache items limit - used for all other caches. */
  private static long fileCacheItemsLimit = 25000000; // 25 M by default

  /**  Enable admission controller for file cache */
  private static boolean acForFileCache = false;
  
  /** Admission controller ratio for file cache */
  private static double acRatioForFileCache = 0.2;
  
  /* Eviction policy for file cache*/
  private static EvictionPolicy fileEvictionPolicy = EvictionPolicy.SLRU;
  
  /* Recycling selector for file cache*/
  private static RecyclingSelector fileRecyclingSelector = RecyclingSelector.LRC;
  
  /** Offheap cache limit in bytes */
  private static long offheapCacheSizeLimit = 10 * (1L << 30); // 10G by default

  /** Offheap cache segment size */
  private static long offheapCacheSegmentSize = 4 * (1 << 20); // 4MB
  
  /** Offheap cache items limit - used for all other caches. */
  private static long offheapCacheItemsLimit = 5000000; // 5 M by default
  
  /** Enable admission controller for offheap cache*/
  private static boolean acForOffheapCache = false;
  
  /** Admission controller ratio for offheap cache */
  private static double acRatioForOffheapCache = 0.5;
  
  /** Hybrid cache section */
  /** Victim cache */
  private static boolean victim_promoteOnHit = true;
  
  /** Victim cache */
  private static double victim_promoteThreshold = 0.9;
  
  /** Main cache- when this is true, victim promote on hit must be true as well */
  private static boolean hybridCacheInverseMode = true;
  
  /* Eviction policy for offheap cache*/
  private static EvictionPolicy offheapEvictionPolicy = EvictionPolicy.SLRU;
  
  /* Recycling selector for offheap cache*/
  private static RecyclingSelector offheapRecyclingSelector = RecyclingSelector.LRC;
  
  private static long cacheItemsLimit;
  
  /** Number of client threads. */
  private static int clientThreads = 1; // by default

  /** Number of PUT operations. */
  private static AtomicLong PUTS = new AtomicLong(0);

  /** Number of GET operations. */
  private static AtomicLong GETS = new AtomicLong(0);

  /** The native cache. */
  static Cache nativeCache;

  /** The table a. */
  protected static byte[] TABLE_A = "TABLE_A".getBytes();

  /* Families */
  /** The families. */
  protected static byte[][] FAMILIES =
      new byte[][] { "fam_a".getBytes(), "fam_b".getBytes(), "fam_c".getBytes() };

  /* Columns */
  /** The columns. */
  protected static byte[][] COLUMNS =
      { "col_a".getBytes(), "col_b".getBytes(), "col_c".getBytes() };


  /** The cache. */
  static RowCache cache;

  /* All CF are cacheable */
  /** The table a. */
  static TableDescriptor tableA;

  /** The max versions. */
  static int maxVersions = 10;

  static int scavNumberThreads = 1;
  
  static String ssrow = "row-xxx-xxx-xxx";
  /** The s row. */
  static byte[] ROW = ssrow.getBytes();

  static Path dataDir;

  static CacheType cacheType = CacheType.OFFHEAP;
  
  static WorkloadType workloadType = WorkloadType.ZIPFIAN;
  
  static ZipfDistribution dist ;
  
  /** The util. */
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();  
  
  /** The cp class name. */
  private static String CP_CLASS_NAME = RowCacheCoprocessor.class.getName();
  
  /** The cluster. */
  static MiniHBaseCluster cluster;
  
  /** The _table c. */
  static HTable _tableA;
  
  /* HBase cluster connection */
  static Connection conn;
  
  /* HBase Admin interface*/
  static Admin admin;

  /**
   * The main method.
   * @param args the arguments
   * @throws Exception the exception
   */
  @SuppressWarnings("deprecation")
  public final static void main(String[] args) throws Exception {
    
    parseArgs(args);
    init();
    printTestParameters();
    String[] keyPrefix = new String[clientThreads];
    for (int i = 0; i < clientThreads; i++) {
      keyPrefix[i] = "Thread[" + i + "]";
    }
    long t1 = System.currentTimeMillis();
    ExecuteThread[] threads = startTest(keyPrefix, clientThreads);
    StatsCollector collector = new StatsCollector(statsInterval, threads);
    LOG.error("Test started");
    collector.start();
    waitToFinish(threads);
    collector.stop();
    Scavenger.waitForFinish();
    // Dump some stats
    long t2 = System.currentTimeMillis();
    LOG.error("Total time=" + (t2 - t1) + " ms");
    LOG.error("Estimated RPS=" + ((double) (PUTS.get() + GETS.get()) * 1000) / (t2 - t1));
    
    dumpStats();
    
    IOUtils.deleteRecursively(dataDir.toFile());
  }
  
  private static void printTestParameters() {
    LOG.error("Test parameters:"
    + "\n            Workload=" + workloadType 
    + "\n               Cache=" + cacheType 
    + "\n          Cache size=" + nativeCache.getMaximumCacheSize()
    + "\n   Cache items limit=" + cacheItemsLimit 
    + "\n      Client threads=" + clientThreads
    + "\n   Scavenger threads=" + scavNumberThreads 
    + "\n  Offheap cache size=" + offheapCacheSizeLimit 
    + "\n     File cache size=" + fileCacheSizeLimit
    + "\nOffheap segment size=" + offheapCacheSegmentSize 
    + "\n   File segment size=" + fileCacheSegmentSize
    + "\n       Test duration=" + testTime / 1000 
    + "\n    Offheap eviction=" + offheapEvictionPolicy
    + "\n       File eviction=" + fileEvictionPolicy 
    + "\n    Offheap recycler=" + offheapRecyclingSelector
    + "\n       File recycler=" + fileRecyclingSelector
    + "\n         AC for File=" + acForFileCache
    + "\n       AC File ratio=" + acRatioForFileCache
    + "\n      AC for Offheap=" + acForOffheapCache
    + "\n    AC Offheap ratio=" + acRatioForOffheapCache
    + "\n  Victim promote hit=" + victim_promoteOnHit
    + "\n      Victim thrshld=" + victim_promoteThreshold 
    + "\n Hybrid inverse mode=" + hybridCacheInverseMode);
  }
  
  private static void dumpStats() throws IOException {
    Segment[] segments = nativeCache.getSegmentsSorted();
    for (Segment seg: segments) {
      if (seg == null) {
        LOG.error("null");
        continue;
      }
      if (seg.isSealed() == false) {
        LOG.error("Segment id=" + seg.getId() + " is not sealed, offheap=" + seg.isOffheap());
        seg.seal();
      }
      if (seg.isRecycling()) {
        LOG.error("Segment id=" + seg.getId() + " is recycling");
      }
      dumpSegments(seg);
    }
  }

  private static void dumpSegments(Segment seg) throws IOException {
    
    if (nativeCache.getCacheType() == Cache.Type.DISK && seg.isOffheap()) {
      return;
    }
    SegmentScanner sc = nativeCache.getSegmentScanner(seg);

    int total = seg.getTotalItems();
    int alive = seg.getAliveItems();
    int maxId = Integer.MIN_VALUE;
    int minId = Integer.MAX_VALUE;
    boolean isDirect = sc.isDirect();
    byte[] buffer = !isDirect? new byte[4096]: null;
    while (sc.hasNext()) {
      long keyPtr = sc.keyAddress();
      int keySize = sc.keyLength();
      String key = null;
      if (!isDirect) {
        keySize = sc.getKey(buffer, 0);
        key = new String(buffer, 0, keySize);
      } else {
        key = new String(Utils.toBytes(keyPtr, keySize));
      }
      key = key.substring(21, 31);
      int id = Integer.parseInt(key);
      if (id > maxId) {
        maxId = id;
      } else if (id < minId) {
        minId = id;
      }
      sc.next();
    }
    
    LOG.error("Segment id=" + seg.getId() + " creation time=" + seg.getInfo().getCreationTime() + 
      " total=" + total + " active =" + alive + " minId=" + minId + " maxId=" + maxId + " datsSize=" 
        + seg.getSegmentDataSize());

  }

  /**
   * Initializes the cache.
   * @throws Exception 
   */
  private static void init() throws Exception {

    long start = System.currentTimeMillis();
    LOG.error("Generating " + M + " rows took: " + (System.currentTimeMillis() - start) + " ms");
    LOG.error("Allocated JVM heap: "
        + (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory()));
    
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, CP_CLASS_NAME);
    conf.set("hbase.zookeeper.useMulti", "false");
    
    // Cache configuration
    dataDir = Files.createTempDirectory("temp");
    LOG.error("Data directory: " + dataDir);
    // Cache configuration
    
    conf.set(CarrotConfig.CACHE_ROOT_DIR_PATH_KEY, dataDir.toString());
    
    switch (cacheType) {
      case OFFHEAP:
        // Set cache type to 'offheap'
        conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, CacheType.OFFHEAP.getType());
        initOffheapConfiguration(conf);
        cacheItemsLimit = offheapCacheItemsLimit;
        break;
      case FILE:
        // Set cache type to 'file'
        conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, CacheType.FILE.getType());
        initFileConfiguration(conf);
        cacheItemsLimit = fileCacheItemsLimit;
        break;
      case HYBRID:
        // Set cache type to 'hybrid'
        conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, CacheType.HYBRID.getType());
        initOffheapConfiguration(conf);
        initFileConfiguration(conf);
        
        cacheItemsLimit = offheapCacheItemsLimit + fileCacheItemsLimit;
        break;
    }
    
    initForZipfianAndHybridConfiguration(conf);
    //conf.set("fs.file.impl", SidecarLocalFileSystem.class.getName());
    // Enable snapshot
    UTIL.startMiniCluster(1);
    cluster = UTIL.getMiniHBaseCluster();

    createTables();
    createHBaseTables();
    
    while(RowCache.instance == null) {
      try {
        Thread.sleep(1000);
        LOG.error("WAIT 1s for row cache to come up");

      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    cache = RowCache.instance;
    //cache.setTrace(true);
    nativeCache = cache.getCache();
  }
  
  protected static void createHBaseTables() throws IOException {    
    Configuration cfg = cluster.getConf();
    conn = ConnectionFactory.createConnection(cfg);
    admin = conn.getAdmin();
    
    if( admin.tableExists(tableA.getTableName()) == false){
      admin.createTable(tableA);
      LOG.error("Created table "+tableA);
    }
    _tableA = (HTable) conn.getTable(TableName.valueOf(TABLE_A));
  }
  
  private static void initForZipfianAndHybridConfiguration(Configuration conf) {
 
    if (workloadType == WorkloadType.ZIPFIAN) {
      cacheItemsLimit = 2 * cacheItemsLimit;
      dist = new ZipfDistribution((int) cacheItemsLimit, workloadType.getZipfAlpha());
    }
    if (cacheType == CacheType.HYBRID) {
      // For uniform distribution we do nothing
      // for zipfian we have several options: use Admission controllers, use cache inverse mode
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.CACHE_VICTIM_PROMOTION_ON_HIT_KEY),
        Boolean.toString(victim_promoteOnHit));
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.CACHE_VICTIM_PROMOTION_THRESHOLD_KEY),
        Double.toString(victim_promoteThreshold));
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP, CarrotConfig.CACHE_HYBRID_INVERSE_MODE_KEY),
        Boolean.toString(hybridCacheInverseMode));
    }
  }
  
  
  private static void initFileConfiguration(Configuration conf) {
   
    EvictionPolicy evictionPolicy;
    RecyclingSelector recyclingSelector;
    if (workloadType == WorkloadType.UNIFORM) {
      evictionPolicy = EvictionPolicy.FIFO;
      recyclingSelector = RecyclingSelector.LRC;
    } else {
      evictionPolicy = fileEvictionPolicy;
      recyclingSelector = fileRecyclingSelector;
    }
    
    // set cache size to 1GB
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.CACHE_MAXIMUM_SIZE_KEY),
      Long.toString(fileCacheSizeLimit));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.CACHE_EVICTION_POLICY_IMPL_KEY),
      evictionPolicy.getClassName());
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.CACHE_RECYCLING_SELECTOR_IMPL_KEY),
      recyclingSelector.getClassName());
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.SCAVENGER_START_RUN_RATIO_KEY), 
      Double.toString(0.99));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.SCAVENGER_STOP_RUN_RATIO_KEY), 
      Double.toString(0.95));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.SCAVENGER_NUMBER_THREADS_KEY),
      Integer.toString(scavNumberThreads));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.SCAVENGER_DUMP_ENTRY_BELOW_START_KEY), 
      Double.toString(1.0));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.CACHE_SEGMENT_SIZE_KEY), 
      Long.toString(fileCacheSegmentSize));
    if (acForFileCache && workloadType == WorkloadType.ZIPFIAN) {
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.CACHE_ADMISSION_CONTROLLER_IMPL_KEY),
        AQBasedAdmissionController.class.getName());
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CarrotConfig.ADMISSION_QUEUE_START_SIZE_RATIO_KEY),
        Double.toString(acRatioForFileCache));
    }
  }
  
  private static void initOffheapConfiguration(Configuration conf) {

    EvictionPolicy evictionPolicy;
    RecyclingSelector recyclingSelector;
    
    if (workloadType == WorkloadType.UNIFORM) {
      evictionPolicy = EvictionPolicy.FIFO;
      recyclingSelector = RecyclingSelector.LRC;
    } else {
      evictionPolicy = offheapEvictionPolicy;
      recyclingSelector = offheapRecyclingSelector;
    }
    
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP, CarrotConfig.CACHE_MAXIMUM_SIZE_KEY),
      Long.toString(offheapCacheSizeLimit));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP, CarrotConfig.CACHE_EVICTION_POLICY_IMPL_KEY),
      evictionPolicy.getClassName());
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP,CarrotConfig.CACHE_RECYCLING_SELECTOR_IMPL_KEY),
      recyclingSelector.getClassName());
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP,CarrotConfig.SCAVENGER_START_RUN_RATIO_KEY), 
      Double.toString(0.99));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP,CarrotConfig.SCAVENGER_STOP_RUN_RATIO_KEY), 
      Double.toString(0.95));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP,CarrotConfig.SCAVENGER_NUMBER_THREADS_KEY),
      Integer.toString(scavNumberThreads));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP,CarrotConfig.SCAVENGER_DUMP_ENTRY_BELOW_START_KEY), 
      Double.toString(1.0));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP, CarrotConfig.CACHE_SEGMENT_SIZE_KEY), 
      Long.toString(offheapCacheSegmentSize));
    if (acForOffheapCache && workloadType == WorkloadType.ZIPFIAN) {
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP, CarrotConfig.CACHE_ADMISSION_CONTROLLER_IMPL_KEY),
        AQBasedAdmissionController.class.getName());
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.OFFHEAP, CarrotConfig.ADMISSION_QUEUE_START_SIZE_RATIO_KEY),
        Double.toString(acRatioForOffheapCache));
    }
  }
  
  
  /**
   * Parses command-line arguments.
   * @param args command-line argument list
   */
  private static void parseArgs(String[] args) {
    int i = 0;
    while (i < args.length) {
      if (args[i].equals(THREADS)) {
        clientThreads = Integer.parseInt(args[++i]);
      } else if (args[i].equals(WRITE_RATIO)) {
        writeRatio = Float.parseFloat(args[++i]);
      } else if (args[i].equals(DURATION)) {
        testTime = Long.parseLong(args[++i]) * 1000;
      } else if (args[i].equals(MAXMEMORY)) {
        fileCacheSizeLimit = Long.parseLong(args[++i]);
      }
      i++;
    }
  }

  /**
   * Wait for all threads to finish.
   * @param threads the threads
   */
  static void waitToFinish(Thread[] threads) {
    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  /**
   * Start test.
   * @param keyPrefix the key prefix
   * @param number the number
   * @param opNumber the op number
   * @return the execute thread[]
   */
  static ExecuteThread[] startTest(String[] keyPrefix, int number) {
    ExecuteThread[] threadArray = new ExecuteThread[number];
    for (int i = 0; i < number; i++) {
      threadArray[i] = new ExecuteThread(keyPrefix[i]);
      threadArray[i].start();
    }
    return threadArray;
  }

  /**
   * The Class ExecuteThread.
   */
  static class ExecuteThread extends Thread {

    /** The m thread index. */
    int mThreadIndex;

    /** The m total threads. */
    int mTotalThreads;

    /** Statistics section. */
    private double avgTime;

    /** The max time. */
    private double maxTime;

    /** The median time. */
    private double medianTime;

    /** The time99. */
    private double time99;

    /** The time999. */
    private double time999;

    /** The time9999. */
    private double time9999;

    /** The total time. */
    private long totalTime; // in nanoseconds

    /** The total requests. */
    private long totalRequests;

    /** The Constant NN. */
    final private static int NN = 100000;

    /** The Constant MM. */
    final private static int MM = 20;

    /** The request times. */
    private long[] requestTimes = new long[NN + MM];

    /** The copy array. */
    private long[] copyArray = new long[NN + MM];

    /** The MI n_ time. */
    final long MIN_TIME = 5000; // 5 microsec

    /** The counter. */
    private int counter;

    /** The icounter. */
    private int icounter;

    /** The tt. */
    private long tt;

    /** The stat time. */
    private long statTime;

    /** The start time. */
    @SuppressWarnings("unused")
    private long startTime = System.nanoTime();

    /** The max item number. */
    long maxItemNumber = 0;

    /** The random number generator */
    Random r;

    /** The m prefix. */
    String mPrefix;
    
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    

    /**
     * Checks if is read request.
     * @return true, if is read request
     */
    private final boolean isReadRequest() {
      double d = rnd.nextDouble();
      return d > writeRatio;
    }

    /**
     * Calculate statistics
     */
    private void calculateStats() {
      // avgTime

      double sum = 0.d;
      double max = Double.MIN_VALUE;
      for (int i = 0; i < requestTimes.length; i++) {
        sum += ((double) requestTimes[i]) / 1000;
      }
      // avgTime
      avgTime = (avgTime * (totalRequests - requestTimes.length) + sum) / totalRequests;
      // sort
      Arrays.sort(requestTimes);
      max = ((double) requestTimes[requestTimes.length - 1]) / 1000;
      // maxTime
      if (max > maxTime) maxTime = max;
      double median = ((double) (requestTimes[requestTimes.length - (counter) / 2])) / 1000;// microsecs

      if (medianTime == 0.d) {
        medianTime = median;
      } else {
        medianTime =
            (medianTime * (totalRequests - (counter)) + median * (counter)) / totalRequests;
      }

      double t99 = ((double) requestTimes[requestTimes.length - 1000]) / 1000;
      if (time99 == 0.d) {
        time99 = t99;
      } else {
        time99 = (time99 * (totalRequests - (counter)) + t99 * (counter)) / totalRequests;
      }
      double t999 = ((double) requestTimes[requestTimes.length - 100]) / 1000;
      if (time999 == 0.d) {
        time999 = t999;
      } else {
        time999 = (time999 * (totalRequests - (counter)) + t999 * (counter)) / totalRequests;
      }

      double t9999 = ((double) requestTimes[requestTimes.length - 10]) / 1000;
      if (time9999 == 0.d) {
        time9999 = t9999;
      } else {
        time9999 = (time9999 * (totalRequests - counter) + t9999 * counter) / totalRequests;
      }

      counter = 0;
      System.arraycopy(copyArray, 0, requestTimes, 0, requestTimes.length);
    }

    /**
     * Gets the counter.
     * @return the counter
     */
    public int getCounter() {
      return counter;
    }

    /**
     * in microseconds.
     * @return the avg time
     */
    public double getAvgTime() {
      return avgTime;
    }

    /**
     * in microseconds.
     * @return the max time
     */
    public double getMaxTime() {
      return maxTime;
    }

    /**
     * Gets the requests per sec.
     * @return the requests per sec
     */
    public double getRequestsPerSec() {
      if (totalTime > 0) {
        double secs = ((double) totalTime) / 1000000000;
        return totalRequests / secs;
      } else {
        return 0;
      }
    }

    /**
     * Gets the total requests.
     * @return the total requests
     */
    public long getTotalRequests() {
      return totalRequests;
    }

    /**
     * 50% of requests have latency < medianTime. In microseconds
     * @return the median time
     */
    public double getMedianTime() {
      return medianTime;
    }

    /**
     * 99% of requests have latency < time99. In microseconds
     * @return the time99
     */
    public double getTime99() {
      return time99;
    }

    /**
     * 99.9% of requests have latency < time999. In microseconds
     * @return the time999
     */
    public double getTime999() {
      return time999;
    }

    /**
     * 99.99% of requests have latency < time9999. In microseconds
     * @return the time9999
     */
    public double getTime9999() {
      return time9999;
    }

    /**
     * Instantiates a new execute thread.
     * @param keyPrefix the key prefix
     * @param n the n
     */
    public ExecuteThread(String keyPrefix) {
      super(keyPrefix);
      this.mPrefix = keyPrefix;
      r = new Random(Arrays.hashCode(keyPrefix.getBytes()));
    }

    public void run() {
      try {
        testPerf(getName());
      } catch (Exception e) {
        LOG.error(e);
      }
    }

    /**
     * Test performance
     * @param key the key
     */
    private void testPerf(String key) {
      LOG.error("RowCache Performance test. Cache size =" + nativeCache.size() + ": "
          + Thread.currentThread().getName());

      try {
        int c = 0;
        // JIT warm up
        while (c++ < 1000) {
          innerLoop();
        }

        totalTime = 0;
        totalRequests = 0;
        tt = System.nanoTime();
        icounter = 0;
        counter = 0;
        statTime = 0;
        long t1 = System.currentTimeMillis();
        long stopTime = t1 + testTime;
        while (System.currentTimeMillis() < stopTime ) {
          innerLoop();
        }
        LOG.error(getName() + ": Finished.");
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error(e);
        System.exit(-1);
      }
    }

    /**
     * Next index
     * @param max the maximum value
     * @return the index
     */
    private final int nextInt(final int max) {
      return rnd.nextInt(max);
    }

    /**
     * Unsafe if cacheSize >.
     * @return the next get index
     */
    private final long getNextGetIndex() {
      long cacheSize = (cacheItemsLimit > 0) ? cacheItemsLimit : nativeCache.size();
      long maxItemNumber = seqNumber.get();
      
      if (workloadType == WorkloadType.UNIFORM) {
        if (maxItemNumber > cacheSize) {
          return maxItemNumber - ((nextInt((int) cacheSize)));
        } else {
          return maxItemNumber > 0 ? Math.abs(nextInt((int) maxItemNumber)) : 0;
        }
      } else {
        int n = dist.sample();
        if (cacheSize < maxItemNumber) {
          return maxItemNumber - n;
        } else {
          return maxItemNumber - Math.round(n * (double) maxItemNumber / cacheSize);
        }
      }
    }

    /** To speed get request - pre- construct. */
    List<byte[]> families = Arrays.asList(new byte[][] { FAMILIES[0], FAMILIES[1], FAMILIES[2] });

    /** The columns. */
    List<byte[]> columns = Arrays.asList(new byte[][] { COLUMNS[0], COLUMNS[1], COLUMNS[2] });

    /** The map. */
    Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(families, columns);

    /**
     * Inner loop.
     */
    private final void innerLoop() {

      long tt1 = System.nanoTime();
      boolean isReadRequest = isReadRequest();// f > sWriteRatio;

      if (isReadRequest) {
        try {
          long l = getNextGetIndex();
          byte[] row = getRow(l);
          @SuppressWarnings("unused")
          List<Cell> results = getFromCache(row, map);
        } catch (Exception e) {
          LOG.error("get native call.", e);
          System.exit(-1);
        }
      } else {
        try {
          long nextSeqNumber = seqNumber.incrementAndGet();
          cacheRow(nextSeqNumber);
        } catch (Exception e) {
          e.printStackTrace();
          LOG.error("put call.", e);
          System.exit(-1);
        }
      }
      icounter++;
      totalRequests++;
      long tt2 = System.nanoTime();
      if (tt2 - tt1 > MIN_TIME) {
        // process all previous
        long lastt = tt2 - tt1;
        long rt = (icounter > 1) ? (tt1 - tt - statTime) / (icounter - 1) : 0;
        for (int i = 0; i < icounter - 1; i++) {
          requestTimes[counter++] = rt;
        }
        requestTimes[counter++] = lastt;
        totalTime += (tt2 - tt) - statTime;
        tt = tt2;
        icounter = 0;
        statTime = 0;
      } else if (tt2 - tt > MIN_TIME) {
        long rt = (tt2 - tt - statTime) / icounter;
        for (int i = 0; i < icounter; i++) {
          requestTimes[counter++] = rt;
        }
        totalTime += tt2 - tt - statTime;
        tt = tt2;
        icounter = 0;
        statTime = 0;
      } else {
        // continue
      }
      if (counter >= NN/* requestTimes.length */) {
        long ttt1 = System.nanoTime();
        calculateStats();
        long ttt2 = System.nanoTime();
        statTime = ttt2 - ttt1;
      }
    }
  }

  /**
   * Class StatsCollector.
   */
  static class StatsCollector extends Thread {

    /** The m threads. */
    ExecuteThread[] mThreads;

    /** The m interval. */
    long mInterval;

    /** The m start time. */
    long mStartTime;

    /**
     * Instantiates a new stats collector.
     * @param interval the interval
     * @param sources the sources
     */
    public StatsCollector(long interval, ExecuteThread[] sources) {
      super("StatsCollector");
      setDaemon(true);
      this.mThreads = sources;
      this.mInterval = interval;
      this.mStartTime = System.currentTimeMillis();
    }

    public void run() {
      
      if (cacheType == CacheType.HYBRID) {
        Scavenger.Stats stats1 = Scavenger.getStatisticsForCache(CacheType.OFFHEAP.getCacheName());
        Scavenger.Stats stats2 = Scavenger.getStatisticsForCache(CacheType.FILE.getCacheName());

      } else {
        Scavenger.Stats stats = Scavenger.getStatisticsForCache(cacheType.getCacheName());
      }
      while (true) {
        try {
          Thread.sleep(mInterval);
        } catch (Exception e) {

        }
        double rps = 0.d;
        double max = 0.d;
        double avg = 0.d;
        double median = 0.d;
        double t99 = 0.d;
        double t999 = 0.d;
        double t9999 = 0.d;
        long totalRequests = 0;
        for (ExecuteThread et : mThreads) {
          rps += et.getRequestsPerSec();
          totalRequests += et.getTotalRequests();
          double m = et.getMaxTime();
          if (m > max) max = m;
          avg += et.getAvgTime();
          median += et.getMedianTime();
          t99 += et.getTime99();
          t999 += et.getTime999();
          t9999 += et.getTime9999();
        }

        avg /= mThreads.length;
        median /= mThreads.length;
        t99 /= mThreads.length;
        t999 /= mThreads.length;
        t9999 /= mThreads.length;
        rps = totalRequests * 1000 / (System.currentTimeMillis() - mStartTime);
        LOG.error(
              "\n            RPS=" + rps 
            + "\n            MAX=" + max 
            + "\n            AVG=" + avg 
            + "\n         MEDIAN=" + median
            + "\n            99%=" + t99 
            + "\n          99.9%=" + t999 
            + "\n         99.99%=" + t9999 
            + "\n    TOTAL ITEMS=" + getTotalItems() 
            + "\n   ACTIVE ITEMS=" + getTotalActiveItems() 
            + "\n           GETS=" + getTotalRequests() 
            + "\n           HITS=" + getTotalHits()
            + "\n ALLOCATED SIZE=" + getMemAllocated() 
            + "\n      USED SIZE=" + getRawSize()
            + "\n   TOTAL WRITES=" + getTotalWrites()
            + "\nREJECTED WRITES=" + getTotalRejectedWrites());
//            + "\n   SCAV SCANNED=" + stats.getTotalItemsScanned()
//            + "\n     SCAV FREED=" + stats.getTotalItemsFreed() 
//            + "\n   SCAV DELETED=" + stats.getTotalItemsDeleted()
//            + "\n   SCAV EXPIRED=" + stats.getTotalItemsExpired() 
//            + "\n SCAV NOT FOUND=" + stats.getTotalItemsNotFound());
        Scavenger.printStats();
            
      }
    }
  }

  public static String getTotalWrites() {
    return Long.toString(nativeCache.getTotalWrites());
  }
  
  public static String getTotalRejectedWrites() {
    return Long.toString(nativeCache.getTotalRejectedWrites());
  }
  
  /**
   * Gets the total items.
   * @return the total items
   */
  public static String getTotalItems() {
    long size = nativeCache.size();
    Cache victim = nativeCache.getVictimCache();
    if (victim != null) {
      size += victim.size();
    }
    return Long.toString(size);
  }

  /**
   * Gets the total active items.
   * @return the total items
   */
  public static String getTotalActiveItems() {
    long size = nativeCache.activeSize();
    Cache victim = nativeCache.getVictimCache();
    if (victim != null) {
      size += victim.activeSize();
    }
    return Long.toString(size);
  }

  /**
   * Gets the memory allocated.
   * @return the memory allocated
   */
  public static String getMemAllocated() {
    long size = nativeCache.getStorageAllocated();
    Cache victim = nativeCache.getVictimCache();
    if (victim != null) {
      size += victim.getStorageAllocated();
    }
    return Long.toString(size);
  }

  /**
   * Gets the raw size.
   * @return the raw size
   */
  public static String getRawSize() {
    long size = nativeCache.getStorageUsed();
    Cache victim = nativeCache.getVictimCache();
    if (victim != null) {
      size += victim.getStorageUsed();
    }
    return Long.toString(size);  
  }

  /**
   * Gets the total requests.
   * @return the total requests
   */
  public static String getTotalRequests() {
    return Long.toString(nativeCache.getTotalGets());
  }

  /**
   * Gets the total hits.
   * @return the total hits
   */
  public static String getTotalHits() {
    long hits = nativeCache.getTotalHits();
    Cache victim = nativeCache.getVictimCache();
    if (victim != null) {
      hits += victim.getTotalHits();
    }
    return Long.toString(hits);
  }

  /**
   * Cache row.
   * @param table the table
   * @param rowNum the row num
   * @param seqNumber the seq number
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected static void cacheRow(long seqNumber)
      throws IOException {
    List<Cell> cells = generateRowData((int)seqNumber);
    Put put = createPut(cells);
    _tableA.put(put);
  }

  /**
   * Gets the from cache.
   * @param table the table
   * @param row the row
   * @param map the map
   * @return the from cache
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected static List<Cell> getFromCache(byte[] row,
      Map<byte[], NavigableSet<byte[]>> map) throws IOException {
    Get get = createGet(row, map, null, null);
    get.readVersions(maxVersions);
    return _tableA.get(get).listCells();
  }

  /**
   * Construct family map.
   * @param families the families
   * @param columns the columns
   * @return the map
   */
  protected static Map<byte[], NavigableSet<byte[]>> constructFamilyMap(List<byte[]> families,
      List<byte[]> columns) {
    Map<byte[], NavigableSet<byte[]>> map =
        new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
    if (families == null) return map;
    NavigableSet<byte[]> colSet = getColumnSet(columns);
    for (byte[] f : families) {
      map.put(f, colSet);
    }
    return map;
  }

  /**
   * Gets the row.
   * @param i the index
   * @return the row
   */
  static byte[] getRow(long i) {
    return ("rowxxxxxxx" + format(i, 10)).getBytes();
  }
  
  private static String format(long l, int n) {
    String s = Long.toString(l);
    // n > s.length
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < n; i++) {
      if (i < n - s.length()) {
        sb.append("0");
      } else {
        sb.append(s);
        break;
      }
    }
    return sb.toString();
  }
  
  /**
   * Gets the value.
   * @param i the index
   * @return the value
   */
  static byte[] getValue(int i) {
    return ("value" + i).getBytes();
  }

  /**
   * Generate row data.
   * @param i the index
   * @return the list
   */
  static List<Cell> generateRowData(int i) {
    byte[] row = getRow(i);
    byte[] value = getValue(i);
    long startTime = System.currentTimeMillis();
    ArrayList<Cell> list = new ArrayList<Cell>();
    int count = 0;
    int VERSIONS = maxVersions;
    for (byte[] f : FAMILIES) {
      for (byte[] c : COLUMNS) {
        count = 0;
        for (; count < VERSIONS; count++) {
          KeyValue kv = new KeyValue(row, f, c, startTime + (count), value);
          list.add(kv);
        }
      }
    }
    Collections.sort(list, CellComparator.getInstance());
    return list;
  }


  /**
   * Creates the tables.
   */
  protected static void createTables() {

    ColumnFamilyDescriptor famA = ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[0])
        .setValue(RConstants.ROWCACHE, "true".getBytes()).build();

    ColumnFamilyDescriptor famB = ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[1])
        .setValue(RConstants.ROWCACHE, "true".getBytes()).build();

    ColumnFamilyDescriptor famC = ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[2])
        .setValue(RConstants.ROWCACHE, "true".getBytes()).build();

    tableA = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_A)).setColumnFamily(famA)
        .setColumnFamily(famB).setColumnFamily(famC).build();
  }

  /**
   * Creates the get.
   * @param row the row
   * @param familyMap the family map
   * @param tr the tr
   * @param f the f
   * @return the gets the
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected static Get createGet(byte[] row, Map<byte[], NavigableSet<byte[]>> familyMap,
      TimeRange tr, Filter f) throws IOException {
    Get get = new Get(row);
    if (tr != null) {
      get.setTimeRange(tr.getMin(), tr.getMax());
    }
    if (f != null) get.setFilter(f);
    if (familyMap != null) {
      for (byte[] fam : familyMap.keySet()) {
        NavigableSet<byte[]> cols = familyMap.get(fam);
        if (cols == null || cols.size() == 0) {
          get.addFamily(fam);
        } else {
          for (byte[] col : cols) {
            get.addColumn(fam, col);
          }
        }
      }
    }
    return get;
  }

  /**
   * Creates the put.
   * @param values the values
   * @return the put
   * @throws IOException
   */
  protected static Put createPut(List<Cell> values) throws IOException {
    Put put = new Put(TestUtils.getRow(values.get(0)));
    for (Cell kv : values) {
      put.add(kv);
    }
    return put;
  }

  /**
   * Creates the increment.
   * @param row the row
   * @param familyMap the family map
   * @param tr the tr
   * @param value the value
   * @return the increment
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected static Increment createIncrement(byte[] row,
      Map<byte[], NavigableSet<byte[]>> familyMap, TimeRange tr, long value) throws IOException {
    Increment incr = new Increment(row);
    if (tr != null) {
      incr.setTimeRange(tr.getMin(), tr.getMax());
    }
    if (familyMap != null) {
      for (byte[] fam : familyMap.keySet()) {
        NavigableSet<byte[]> cols = familyMap.get(fam);
        for (byte[] col : cols) {
          incr.addColumn(fam, col, value);
        }
      }
    }
    return incr;
  }

  /**
   * Creates the append.
   * @param row the row
   * @param families the families
   * @param columns the columns
   * @param value the value
   * @return the append
   */
  protected static Append createAppend(byte[] row, List<byte[]> families, List<byte[]> columns,
      byte[] value) {
    Append op = new Append(row);
    for (byte[] f : families) {
      for (byte[] c : columns) {
        op.addColumn(f, c, value);
      }
    }
    return op;
  }

  /**
   * Creates the delete.
   * @param values the values
   * @return the delete
   */
  protected static Delete createDelete(List<KeyValue> values) {
    Delete del = new Delete(TestUtils.getRow(values.get(0)));
    for (KeyValue kv : values) {
      del.addColumns(TestUtils.getFamily(kv), TestUtils.getQualifier(kv));
    }
    return del;
  }

  /**
   * Creates the delete.
   * @param row the row
   * @return the delete
   */
  protected static Delete createDelete(byte[] row) {
    Delete del = new Delete(row);
    return del;
  }

  /**
   * Creates the delete.
   * @param row the row
   * @param families the families
   * @return the delete
   */
  protected static Delete createDelete(byte[] row, List<byte[]> families) {
    Delete del = new Delete(row);
    for (byte[] f : families) {
      del.addFamily(f);
    }
    return del;
  }

  /**
   * Equals.
   * @param list1 the list1
   * @param list2 the list2
   * @return true, if successful
   */
  protected static boolean equals(List<Cell> list1, List<Cell> list2) {
    if (list1.size() != list2.size()) return false;
    Collections.sort(list1, CellComparator.getInstance());
    Collections.sort(list2, CellComparator.getInstance());
    for (int i = 0; i < list1.size(); i++) {
      if (list1.get(i).equals(list2.get(i)) == false) return false;
    }
    return true;
  }

  /**
   * Sub list.
   * @param list the list
   * @param family the family
   * @return the list
   */
  protected static List<Cell> subList(List<Cell> list, byte[] family) {
    List<Cell> result = new ArrayList<Cell>();
    for (Cell kv : list) {
      if (Bytes.equals(family, TestUtils.getFamily(kv))) {
        result.add(kv);
      }
    }
    return result;
  }

  /**
   * Sub list.
   * @param list the list
   * @param family the family
   * @param cols the cols
   * @return the list
   */
  protected static List<Cell> subList(List<Cell> list, byte[] family, List<byte[]> cols) {
    List<Cell> result = new ArrayList<Cell>();
    for (Cell kv : list) {
      if (Bytes.equals(family, TestUtils.getFamily(kv))) {
        byte[] col = TestUtils.getQualifier(kv);
        for (byte[] c : cols) {
          if (Bytes.equals(col, c)) {
            result.add(kv);
            break;
          }
        }
      }
    }
    return result;
  }

  /**
   * Sub list.
   * @param list the list
   * @param families the families
   * @param cols the cols
   * @return the list
   */
  protected static List<Cell> subList(List<Cell> list, List<byte[]> families, List<byte[]> cols) {
    List<Cell> result = new ArrayList<Cell>();
    for (Cell kv : list) {
      for (byte[] family : families) {
        if (Bytes.equals(family, TestUtils.getFamily(kv))) {
          byte[] col = TestUtils.getQualifier(kv);
          for (byte[] c : cols) {
            if (Bytes.equals(col, c)) {
              result.add(kv);
              break;
            }
          }
        }
      }
    }
    return result;
  }

  /**
   * Sub list.
   * @param list the list
   * @param families the families
   * @param cols the cols
   * @param max the max
   * @return the list
   */
  protected static List<Cell> subList(List<Cell> list, List<byte[]> families, List<byte[]> cols,
      int max) {
    List<Cell> result = new ArrayList<Cell>();
    for (Cell kv : list) {
      for (byte[] family : families) {
        if (Bytes.equals(family, TestUtils.getFamily(kv))) {
          byte[] col = TestUtils.getQualifier(kv);
          for (byte[] c : cols) {
            if (Bytes.equals(col, c)) {
              result.add(kv);
              break;
            }
          }
        }
      }
    }
    int current = 0;
    byte[] f = TestUtils.getFamily(result.get(0));
    byte[] c = TestUtils.getQualifier(result.get(0));
    List<Cell> ret = new ArrayList<Cell>();
    for (Cell kv : result) {
      byte[] fam = TestUtils.getFamily(kv);
      byte[] col = TestUtils.getQualifier(kv);
      if (Bytes.equals(f, fam)) {
        if (Bytes.equals(c, col)) {
          if (current < max) {
            ret.add(kv);
          }
          current++;
        } else {
          c = col;
          current = 1;
          ret.add(kv);
        }
      } else {
        f = fam;
        c = col;
        current = 1;
        ret.add(kv);
      }
    }
    return ret;
  }

  /**
   * Gets the column set.
   * @param cols the cols
   * @return the column set
   */
  protected static NavigableSet<byte[]> getColumnSet(List<byte[]> cols) {
    if (cols == null) {
      return null;
    }
    TreeSet<byte[]> set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (byte[] c : cols) {
      set.add(c);
    }
    return set;
  }

  /**
   * Dump.
   * @param list the list
   */
  protected static void dump(List<Cell> list) {
    for (Cell kv : list) {
      dump(kv);
    }
  }

  /**
   * Dump key-value cell
   * @param kv the kv
   */
  protected static void dump(Cell kv) {
    LOG.error(
      "row=" + new String(TestUtils.getRow(kv)) + " family=" + new String(TestUtils.getFamily(kv))
          + " column=" + new String(TestUtils.getQualifier(kv)) + " ts=" + kv.getTimestamp());
  }

  /**
   * Patch row.
   * @param kv the kv
   * @param patch the patch
   */
  protected static void patchRow(KeyValue kv, byte[] patch) {
    int off = kv.getRowOffset();
    System.arraycopy(patch, 0, kv.getBuffer(), off, patch.length);
  }

  /**
   * Patch row.
   * @param row the row
   * @param off the off
   * @param seqNumber the seq number
   */
  protected static void patchRow(byte[] row, int off, long seqNumber) {
    row[off] = (byte) ((seqNumber >>> 56) & 0xff);
    row[off + 1] = (byte) ((seqNumber >>> 48) & 0xff);
    row[off + 2] = (byte) ((seqNumber >>> 40) & 0xff);
    row[off + 3] = (byte) ((seqNumber >>> 32) & 0xff);
    row[off + 4] = (byte) ((seqNumber >>> 24) & 0xff);
    row[off + 5] = (byte) ((seqNumber >>> 16) & 0xff);
    row[off + 6] = (byte) ((seqNumber >>> 8) & 0xff);
    row[off + 7] = (byte) ((seqNumber) & 0xff);
  }
}
