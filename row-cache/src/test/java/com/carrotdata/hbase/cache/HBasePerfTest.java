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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
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

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.Scavenger;
import com.carrotdata.cache.controllers.AQBasedAdmissionController;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.Utils;
import com.carrotdata.hbase.cache.utils.IOUtils;
import com.carrotdata.sidecar.DataCacheMode;
import com.carrotdata.sidecar.SidecarCachingFileSystem;
import com.carrotdata.sidecar.SidecarConfig;
import com.carrotdata.sidecar.SidecarDataCacheType;
import com.carrotdata.sidecar.WriteCacheMode;
import com.carrotdata.sidecar.fs.hdfs.SidecarDistributedFileSystem;
import com.carrotdata.sidecar.fs.s3a.SidecarS3AFileSystem;
import com.carrotdata.sidecar.hints.HBaseCachingHintDetector;

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
  private static long testTime = 3 * 3600000;// 3 hours

  /** The write ratio. */
  private static float writeRatio = 0.05f; // 10% puts - 90% gets

  /** Statistics tread interval in ms */
  private static long statsInterval = 5000;

  /**
   * Row-Cache SECTION
   */
  
  /** File cache limit in bytes */
  private static long rcFileCacheSizeLimit = 50L * (1L << 30); // 25G by default

  /** File cache segment size */
  private static long rcFileCacheSegmentSize = 64 * (1 << 20); // 64MB
  
  /** Cache items limit - used for all other caches. */
  private static long rcFileCacheItemsLimit = 25000000; // 25 M by default

  /**  Enable admission controller for file cache */
  private static boolean rcACForFileCache = false;
  
  /** Admission controller ratio for file cache */
  private static double rcACRatioForFileCache = 0.2;
  
  /* Eviction policy for file cache*/
  private static EvictionPolicy rcFileEvictionPolicy = EvictionPolicy.SLRU;
  
  /* Recycling selector for file cache*/
  private static RecyclingSelector rcFileRecyclingSelector = RecyclingSelector.MinAlive;
  
  /** Offheap cache limit in bytes */
  private static long rcOffheapCacheSizeLimit = 10 * (1L << 30); // 10G by default

  /** Offheap cache segment size */
  private static long rcOffheapCacheSegmentSize = 4 * (1 << 20); // 4MB
  
  /** Offheap cache items limit - used for all other caches. */
  private static long rcOffheapCacheItemsLimit = 5000000; // 5 M by default
  
  /** Enable admission controller for offheap cache*/
  private static boolean rcACForOffheapCache = false;
  
  /** Admission controller ratio for offheap cache */
  private static double rcACRatioForOffheapCache = 0.5;
  
  /** Hybrid cache section */
  /** Victim cache */
  private static boolean rc_victim_promoteOnHit = true;
  
  /** Victim cache */
  private static double rc_victim_promoteThreshold = 0.9;
  
  /** Main cache- when this is true, victim promote on hit must be true as well */
  private static boolean rcHybridCacheInverseMode = true;
  
  /* Eviction policy for offheap cache*/
  private static EvictionPolicy rcOffheapEvictionPolicy = EvictionPolicy.SLRU;
  
  /* Recycling selector for offheap cache*/
  private static RecyclingSelector rcOffheapRecyclingSelector = RecyclingSelector.MinAlive;
  
  /* Number of GC threads for both: offheap and file caches */
  private static int rcScavNumberThreads = 1;
  
  /* Cache type */
  private static CacheType rcCacheType = CacheType.MEMORY;

  /** END of Row-Cache SECTION */
  
  private static long cacheItemsLimit;
  
  /** Number of client threads. */
  private static int clientThreads = 4; // by default

  /** Number of PUT operations. */
  private static AtomicLong PUTS = new AtomicLong(0);

  /** Number of GET operations. */
  private static AtomicLong GETS = new AtomicLong(0);

  /** The native cache. */
  static Cache nativeCache;

  /** The table name */
  protected static byte[] TABLE_A = "TABLE_A".getBytes();

  /* Families */
  protected static byte[][] FAMILIES =
      new byte[][] { "fam_a".getBytes(), "fam_b".getBytes(), "fam_c".getBytes() };

  /* Columns */
  protected static byte[][] COLUMNS =
      { "col_a".getBytes(), "col_b".getBytes(), "col_c".getBytes() };


  /** The Mr. RowCache */
  static RowCache cache;

  /* Use row cache */
  static boolean useRowCache = false;
  
  /** The table descriptor */
  static TableDescriptor tableA;

  static int blockSize = 64 * 1024;
  
  /** The max versions. */
  static int maxVersions = 20;
  
  static String ssrow = "row-xxx-xxx-xxx";
  /** The s row. */
  static byte[] ROW = ssrow.getBytes();

  static Path dataDir;
  
  static WorkloadType workloadType = WorkloadType.ZIPFIAN;
  
  /* SIDECAR configuration SECTION */
  
  static SidecarDataCacheType scCacheType = SidecarDataCacheType.FILE;
  
  static long scWriteCacheMaxSize = 5L * (1 << 30);
  
  static long scFileCacheSize = 15L * (1L << 30);
  
  static int scFileDataSegmentSize = 64 * (1 << 20);
  
  static long scOffheapCacheSize = 5L * (1L << 30);
  
  static int scOffheapDataSegmentSize = 4 * (1 << 20);
  
  static int scPageSize = 1 << 16; // 4Kb
  
  static int scIOBufferSize = 2 * scPageSize;
  
  static boolean scACFileEnabled = false;
  
  static double scACStartRatio = 0.2;
  
  static long scMetaCacheSize = 1 << 30;
  
  static int scMetaCacheSegmentSize = 4 * (1 << 20);
  
  static int scScavThreads = 1;
    
  static URI scWriteCacheDirectoryURI;
  
  static ZipfDistribution dist ;
  
  static long toLoad = 2000000;// 2M KV
  
  /** The util. */
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();  
  
  /** The cp class name. */
  private static String CP_CLASS_NAME = RowCacheCoprocessor.class.getName();
  
  /** The cluster. */
  static MiniHBaseCluster cluster;
  
  static Path testDirPath;
  
  /** The _table c. */
  static HTable _tableA;
  
  /* HBase cluster connection */
  static Connection conn;
  
  /* HBase Admin interface*/
  static Admin admin;
  
  static boolean hbaseBlockCacheEnabled = false;
  
  static enum TestType{
    LOAD_AND_READ,
    LOAD_THEN_READ
  }

  static TestType testType = TestType.LOAD_AND_READ;
  
  /**
   * Minio server access
   */
  protected static String S3_ENDPOINT = "http://localhost:9000";
  protected static String S3_BUCKET = "s3a://hbase";
  protected static String ACCESS_KEY = "admin";
  protected static String SECRET_KEY = "password";
  protected static boolean s3run = false;
  /**
   * The main method.
   * @param args the arguments
   * @throws Exception the exception
   */
  public final static void main(String[] args) throws Exception {

    parseArgs(args);
    init();
    printTestParameters();

    long t1 = System.currentTimeMillis();
    ExecuteThread[] threads = startTest(false);
    StatsCollector collector = new StatsCollector(statsInterval, threads);
    LOG.error("Test started");
    collector.start();
    // Phase 1: load data
    waitToFinish(threads);
    // TODO: stop collector gracefully
    collector.interrupt();
    try {
      collector.join();
    } catch (InterruptedException e) {
    }
    if (testType == TestType.LOAD_THEN_READ) {
      // Compact all
      LOG.error("Major compaction started ");
      long start = System.currentTimeMillis();
      UTIL.getMiniHBaseCluster().flushcache();
      UTIL.compact(true);
      LOG.error(
        "Major compaction finished in " + (System.currentTimeMillis() - start) / 1000 + " sec");
      // PHASE 2: read
      threads = startTest(true);
      collector = new StatsCollector(statsInterval, threads);
      LOG.error("Phase READ started");
      collector.start();
      waitToFinish(threads);
      // TODO: stop collector gracefully
      collector.interrupt();
      try {
        collector.join();
      } catch (InterruptedException e) {
      }
    }

    Scavenger.waitForFinish();
    // Dump some stats
    long t2 = System.currentTimeMillis();
    LOG.error("Total time=" + (t2 - t1) + " ms");
    LOG.error("Estimated RPS=" + ((double) (PUTS.get() + GETS.get()) * 1000) / (t2 - t1));

    cluster.shutdown();
    if (nativeCache != null) {
      nativeCache.printStats();
    }
    SidecarCachingFileSystem.getDataCache().printStats();
    SidecarCachingFileSystem.getMetaCache().printStats();
    IOUtils.deleteRecursively(dataDir.toFile());
    IOUtils.deleteRecursively(new File(scWriteCacheDirectoryURI.getPath()));
  }

  private static void printTestParameters() {
    LOG.error("Test parameters:"
    + "\n            Workload=" + workloadType 
    + "\n               Cache=" + rcCacheType 
    + "\n          Cache size=" + (nativeCache != null?nativeCache.getMaximumCacheSize(): 0)
    + "\n   Cache items limit=" + cacheItemsLimit 
    + "\n      Client threads=" + clientThreads
    + "\n   Scavenger threads=" + rcScavNumberThreads 
    + "\n  Offheap cache size=" + rcOffheapCacheSizeLimit 
    + "\n     File cache size=" + rcFileCacheSizeLimit
    + "\nOffheap segment size=" + rcOffheapCacheSegmentSize 
    + "\n   File segment size=" + rcFileCacheSegmentSize
    + "\n       Test duration=" + testTime / 1000 
    + "\n    Offheap eviction=" + rcOffheapEvictionPolicy
    + "\n       File eviction=" + rcFileEvictionPolicy 
    + "\n    Offheap recycler=" + rcOffheapRecyclingSelector
    + "\n       File recycler=" + rcFileRecyclingSelector
    + "\n         AC for File=" + rcACForFileCache
    + "\n       AC File ratio=" + rcACRatioForFileCache
    + "\n      AC for Offheap=" + rcACForOffheapCache
    + "\n    AC Offheap ratio=" + rcACRatioForOffheapCache
    + "\n  Victim promote hit=" + rc_victim_promoteOnHit
    + "\n      Victim thrshld=" + rc_victim_promoteThreshold 
    + "\n Hybrid inverse mode=" + rcHybridCacheInverseMode);
  }
  

  /**
   * Initializes the cache.
   * @throws Exception 
   */
  private static void init() throws Exception {
    blockSize = 4096;
    scPageSize = 2048;
    scIOBufferSize = 512 * scPageSize; // 1MB
    //useRowCache = false;
    //hbaseBlockCacheEnabled = false;
    
    long start = System.currentTimeMillis();
    LOG.error("Generating " + M + " rows took: " + (System.currentTimeMillis() - start) + " ms");
    LOG.error("Allocated JVM heap: "
        + (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory()));
    
    Configuration conf = UTIL.getConfiguration();
    if (useRowCache) {
      String coprocessors = conf.get(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY);
      if (coprocessors != null && coprocessors.length() > 1) {
        coprocessors += "," + CP_CLASS_NAME;
      } else {
        coprocessors = CP_CLASS_NAME;
      }
      conf.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, coprocessors);
    }
    conf.set("hbase.zookeeper.useMulti", "false");
    
    conf.setInt("hbase.hstore.flusher.count", 4);
    conf.setInt("hbase.hstore.blockingStoreFiles", 100);
    conf.setInt("hbase.hregion.memstore.flush.size", 256 * (1 << 20));
    conf.set("hbase.regionserver.global.memstore.size", "0.5");
    conf.set("hfile.block.cache.size", "0.2");
    //conf.set("hbase.wal.provider", "filesystem");
    conf.set("hbase.store.file-tracker.impl", "FILE");
    if (s3run) {
      configureS3(conf);
    }
    // Cache configuration
    dataDir = Files.createTempDirectory("temp");
    LOG.error("Data directory: " + dataDir);
    // Cache configuration
    
    conf.set(CacheConfig.CACHE_DATA_DIR_PATHS_KEY, dataDir.toString());
    
    if (useRowCache) {
      switch (rcCacheType) {
        case MEMORY:
          // Set cache type to 'offheap'
          conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, CacheType.MEMORY.getType());
          initOffheapConfiguration(conf);
          cacheItemsLimit = rcOffheapCacheItemsLimit;
          break;
        case FILE:
          // Set cache type to 'file'
          conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, CacheType.FILE.getType());
          initFileConfiguration(conf);
          cacheItemsLimit = rcFileCacheItemsLimit;
          break;
        case HYBRID:
          // Set cache type to 'hybrid'
          conf.set(RowCacheConfig.ROWCACHE_TYPE_KEY, CacheType.HYBRID.getType());
          initOffheapConfiguration(conf);
          initFileConfiguration(conf);

          cacheItemsLimit = rcOffheapCacheItemsLimit + rcFileCacheItemsLimit;
          break;
      }
    } else {
      cacheItemsLimit = rcFileCacheItemsLimit;
    }
    
    initForZipfianAndHybridConfiguration(conf);
    configureSidecar(conf);
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numRegionServers(3).numDataNodes(3).createWALDir(true).build();
    UTIL.startMiniCluster(option);
    
    cluster = UTIL.getMiniHBaseCluster();
    testDirPath = Path.of(UTIL.getDataTestDir().toString());
    
    LOG.error(testDirPath);
    createTables();
    createHBaseTables();
     
    while(useRowCache && RowCache.instance == null) {
      try {
        Thread.sleep(1000);
        LOG.error("WAIT 1s for row cache to come up");

      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    if (useRowCache) {
      cache = RowCache.instance;
      nativeCache = cache.getCache();
    }
  }
  
  private static void configureS3(Configuration configuration) {
    configuration.set("fs.s3a.impl", SidecarS3AFileSystem.class.getName());
    
    configuration.set("fs.s3a.access.key", ACCESS_KEY);
    configuration.set("fs.s3a.secret.key", SECRET_KEY);
    configuration.setBoolean("fs.s3a.path.style.access", true);
    configuration.set("fs.s3a.block.size", "512M");
    configuration.setBoolean("fs.s3a.committer.magic.enabled", false);
    configuration.set("fs.s3a.committer.name", "directory");
    configuration.setBoolean("fs.s3a.committer.staging.abort.pending.uploads", true);
    configuration.set("fs.s3a.committer.staging.conflict-mode","append");
    configuration.setBoolean("fs.s3a.committer.staging.unique-filenames", true);
    configuration.setInt("fs.s3a.connection.establish.timeout", 50000);
    configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
    configuration.setInt("fs.s3a.connection.timeout", 200000);
    configuration.set("fs.s3a.endpoint", S3_ENDPOINT);

    configuration.setInt("fs.s3a.committer.threads", 64);// Number of threads writing to MinIO
    configuration.setInt("fs.s3a.connection.maximum", 8192);// Maximum number of concurrent conns
    configuration.setInt("fs.s3a.fast.upload.active.blocks", 2048);// Number of parallel uploads
    configuration.set("fs.s3a.fast.upload.buffer", "disk");//Use drive as the buffer for uploads
    configuration.setBoolean("fs.s3a.fast.upload", true);//Turn on fast upload mode
    configuration.setInt("fs.s3a.max.total.tasks", 2048);// Maximum number of parallel tasks
    configuration.set("fs.s3a.multipart.size", "64M");//  Size of each multipart chunk
    configuration.set("fs.s3a.multipart.threshold", "64M");//Size before using multipart uploads
    configuration.setInt("fs.s3a.socket.recv.buffer", 65536);// Read socket buffer hint
    configuration.setInt("fs.s3a.socket.send.buffer", 65536);// Write socket buffer hint
    configuration.setInt("fs.s3a.threads.max", 256);//  Maximum number of threads for S3A
    configuration.set("hbase.rootdir", S3_BUCKET + "/data");
    configuration.set("fs.s3a.aws.credentials.provider", SimpleAWSCredentialsProvider.class.getName());    
    // S3Guard
    //configuration.set("fs.s3a.metadatastore.impl", LocalMetadataStore.class.getName());
  }
  
  private static void configureSidecar (Configuration conf) throws IOException {
    
    scWriteCacheDirectoryURI = Files.createTempDirectory("write").toUri();
    
    if (!s3run) {
      conf.set("fs.hdfs.impl", SidecarDistributedFileSystem.class.getName());
      // Do not use cached instance - default
      conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    }

    // Disable
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_MODE_KEY, WriteCacheMode.DISABLED.getMode());
    conf.setLong(SidecarConfig.SIDECAR_WRITE_CACHE_SIZE_KEY, scWriteCacheMaxSize);
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_URI_KEY, scWriteCacheDirectoryURI.toString());
    conf.setBoolean(SidecarConfig.SIDECAR_TEST_MODE_KEY, true);
    conf.setBoolean(SidecarConfig.SIDECAR_JMX_METRICS_ENABLED_KEY, true);
    conf.setBoolean(SidecarConfig.SIDECAR_INSTALL_SHUTDOWN_HOOK_KEY, true);
    // Set global cache directory
    // Files are immutable after creation
    conf.setBoolean(SidecarConfig.SIDECAR_REMOTE_FILES_MUTABLE_KEY, true);
    conf.setBoolean(SidecarConfig.SIDECAR_SCAN_DETECTOR_ENABLED_KEY, true);
    // Set scan pages 
    conf.setInt(SidecarConfig.SIDECAR_SCAN_DETECTOR_THRESHOLD_PAGES_KEY, 100);
    // Exclude list
    conf.set(SidecarConfig.SIDECAR_WRITE_CACHE_EXCLUDE_LIST_KEY, 
      ".*/oldWALs/.*,.*/archive/.*,.*/corrupt/.*,.*/staging/.*");
    conf.set(SidecarConfig.SIDECAR_CACHING_HINT_DETECTOR_IMPL_KEY, HBaseCachingHintDetector.class.getName());
    
    CacheConfig carrotCacheConfig = CacheConfig.getInstance();
    // Set meta cache 
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.META_CACHE_NAME, scMetaCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.META_CACHE_NAME, scMetaCacheSegmentSize);
    switch(scCacheType) {
      case  MEMORY: 
        conf = updateConfigurationOffheap(conf); break;
      case FILE: 
        conf = updateConfigurationFile(conf); break;
      case DISABLED:
        conf = updateConfigurationDisabled(conf);
      default:
    }
  }
  
  private static Configuration updateConfigurationDisabled(Configuration conf) {
    SidecarConfig cacheConfig = SidecarConfig.getInstance();
    cacheConfig
      .setDataCacheType(scCacheType)
      .setDataPageSize(scPageSize)
      .setIOBufferSize(scIOBufferSize);
    
    return getHdfsConfiguration(conf, cacheConfig, CacheConfig.getInstance());
  }
  
  private static Configuration updateConfigurationFile(Configuration conf) {
    SidecarConfig cacheConfig = SidecarConfig.getInstance();
    cacheConfig
      .setDataPageSize(scPageSize)
      .setIOBufferSize(scIOBufferSize)
      .setDataCacheType(SidecarDataCacheType.FILE)
      .setJMXMetricsEnabled(true);
    
    cacheConfig.setDataCacheMode(DataCacheMode.NOT_IN_WRITE_CACHE);
    cacheConfig.setCacheableFileSizeThreshold(100 * (1 << 20)); // 100 MB
    
    CacheConfig carrotCacheConfig = CacheConfig.getInstance();
    
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_FILE_NAME, scFileCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_FILE_NAME, scFileDataSegmentSize);
    carrotCacheConfig.setCacheEvictionPolicy(SidecarConfig.DATA_CACHE_FILE_NAME, SLRUEvictionPolicy.class.getName());
    carrotCacheConfig.setRecyclingSelector(SidecarConfig.DATA_CACHE_FILE_NAME, MinAliveRecyclingSelector.class.getName());
    carrotCacheConfig.setSLRUInsertionPoint(SidecarConfig.DATA_CACHE_FILE_NAME, 6);
    if (scACFileEnabled) {
      carrotCacheConfig.setAdmissionController(SidecarConfig.DATA_CACHE_FILE_NAME, AQBasedAdmissionController.class.getName());
      carrotCacheConfig.setAdmissionQueueStartSizeRatio(SidecarConfig.DATA_CACHE_FILE_NAME, scACStartRatio);
    }
    
    if (scScavThreads > 1) {
      carrotCacheConfig.setScavengerNumberOfThreads(SidecarConfig.DATA_CACHE_FILE_NAME, scScavThreads);      
    }
    
    carrotCacheConfig.setVictimCachePromotionOnHit(SidecarConfig.DATA_CACHE_FILE_NAME, rc_victim_promoteOnHit);
    carrotCacheConfig.setVictimPromotionThreshold(SidecarConfig.DATA_CACHE_FILE_NAME, rc_victim_promoteThreshold);
    
    return getHdfsConfiguration(conf, cacheConfig, carrotCacheConfig);
  }

  private static Configuration updateConfigurationOffheap(Configuration conf) {
    SidecarConfig cacheConfig = SidecarConfig.getInstance();
    cacheConfig
      .setDataPageSize(scPageSize)
      .setIOBufferSize(scIOBufferSize)
      .setDataCacheType(SidecarDataCacheType.MEMORY)
      .setJMXMetricsEnabled(true);
    
    CacheConfig carrotCacheConfig = CacheConfig.getInstance();
    
    carrotCacheConfig.setCacheMaximumSize(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, scOffheapCacheSize);
    carrotCacheConfig.setCacheSegmentSize(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, scOffheapDataSegmentSize);
    carrotCacheConfig.setCacheEvictionPolicy(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, SLRUEvictionPolicy.class.getName());
    carrotCacheConfig.setRecyclingSelector(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, MinAliveRecyclingSelector.class.getName());
    carrotCacheConfig.setSLRUInsertionPoint(SidecarConfig.DATA_CACHE_FILE_NAME, 6);
    
    if (scScavThreads > 1) {
      carrotCacheConfig.setScavengerNumberOfThreads(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, scScavThreads);      
    }
    
    //carrotCacheConfig.setVictimCachePromotionOnHit(SidecarConfig.DATA_CACHE_FILE_NAME, victim_promoteOnHit);
    //carrotCacheConfig.setVictimPromotionThreshold(SidecarConfig.DATA_CACHE_FILE_NAME, victim_promoteThreshold);
    
    return getHdfsConfiguration(conf, cacheConfig, carrotCacheConfig);
  }

  
  public static Configuration getHdfsConfiguration(Configuration configuration, 
      SidecarConfig sidecarConfig, CacheConfig carrotCacheConfig)
  {
      for(Entry<Object, Object> e: sidecarConfig.entrySet()) {
        configuration.set((String) e.getKey(), (String) e.getValue());
      }
      Properties p = carrotCacheConfig.getProperties();
      for(Entry<Object, Object> e: p.entrySet()) {
        configuration.set((String) e.getKey(), (String) e.getValue());
      }
      return configuration;
  }
  
  protected static void createHBaseTables() throws IOException {    
    Configuration cfg = cluster.getConf();
    conn = ConnectionFactory.createConnection(cfg);
    admin = conn.getAdmin();
    
    if( admin.tableExists(tableA.getTableName()) == false){
      admin.createTable(tableA, new byte[] {0,0,0,0}, 
        new byte[] { (byte) 255, (byte) 255, (byte) 255, (byte) 255}, 10);
      LOG.error("Created table "+tableA);
    }
    _tableA = (HTable) conn.getTable(TableName.valueOf(TABLE_A));
    
  }
  
  private static void initForZipfianAndHybridConfiguration(Configuration conf) {
 
    if (workloadType == WorkloadType.ZIPFIAN) {
      cacheItemsLimit = 2 * cacheItemsLimit;
      dist = new ZipfDistribution((int) cacheItemsLimit, workloadType.getZipfAlpha());
    }
    if (rcCacheType == CacheType.HYBRID) {
      // For uniform distribution we do nothing
      // for zipfian we have several options: use Admission controllers, use cache inverse mode
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.CACHE_VICTIM_PROMOTION_ON_HIT_KEY),
        Boolean.toString(rc_victim_promoteOnHit));
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.CACHE_VICTIM_PROMOTION_THRESHOLD_KEY),
        Double.toString(rc_victim_promoteThreshold));
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY, CacheConfig.CACHE_HYBRID_INVERSE_MODE_KEY),
        Boolean.toString(rcHybridCacheInverseMode));
    }
  }
  
  
  private static void initFileConfiguration(Configuration conf) {
   
    EvictionPolicy evictionPolicy;
    RecyclingSelector recyclingSelector;
    if (workloadType == WorkloadType.UNIFORM) {
      evictionPolicy = EvictionPolicy.FIFO;
      recyclingSelector = RecyclingSelector.LRC;
    } else {
      evictionPolicy = rcFileEvictionPolicy;
      recyclingSelector = rcFileRecyclingSelector;
    }
    
    // set cache size to 1GB
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.CACHE_MAXIMUM_SIZE_KEY),
      Long.toString(rcFileCacheSizeLimit));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.CACHE_EVICTION_POLICY_IMPL_KEY),
      evictionPolicy.getClassName());
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.CACHE_RECYCLING_SELECTOR_IMPL_KEY),
      recyclingSelector.getClassName());
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.SCAVENGER_START_RUN_RATIO_KEY), 
      Double.toString(0.99));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.SCAVENGER_STOP_RUN_RATIO_KEY), 
      Double.toString(0.95));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.SCAVENGER_NUMBER_THREADS_KEY),
      Integer.toString(rcScavNumberThreads));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.SCAVENGER_DUMP_ENTRY_BELOW_MIN_KEY), 
      Double.toString(1.0));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.CACHE_SEGMENT_SIZE_KEY), 
      Long.toString(rcFileCacheSegmentSize));
    if (rcACForFileCache && workloadType == WorkloadType.ZIPFIAN) {
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.CACHE_ADMISSION_CONTROLLER_IMPL_KEY),
        AQBasedAdmissionController.class.getName());
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.FILE, CacheConfig.ADMISSION_QUEUE_START_SIZE_RATIO_KEY),
        Double.toString(rcACRatioForFileCache));
    }
  }
  
  private static void initOffheapConfiguration(Configuration conf) {

    EvictionPolicy evictionPolicy;
    RecyclingSelector recyclingSelector;
    
    if (workloadType == WorkloadType.UNIFORM) {
      evictionPolicy = EvictionPolicy.FIFO;
      recyclingSelector = RecyclingSelector.LRC;
    } else {
      evictionPolicy = rcOffheapEvictionPolicy;
      recyclingSelector = rcOffheapRecyclingSelector;
    }
    
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY, CacheConfig.CACHE_MAXIMUM_SIZE_KEY),
      Long.toString(rcOffheapCacheSizeLimit));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY, CacheConfig.CACHE_EVICTION_POLICY_IMPL_KEY),
      evictionPolicy.getClassName());
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY,CacheConfig.CACHE_RECYCLING_SELECTOR_IMPL_KEY),
      recyclingSelector.getClassName());
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY,CacheConfig.SCAVENGER_START_RUN_RATIO_KEY), 
      Double.toString(0.99));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY,CacheConfig.SCAVENGER_STOP_RUN_RATIO_KEY), 
      Double.toString(0.95));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY,CacheConfig.SCAVENGER_NUMBER_THREADS_KEY),
      Integer.toString(rcScavNumberThreads));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY,CacheConfig.SCAVENGER_DUMP_ENTRY_BELOW_MIN_KEY), 
      Double.toString(1.0));
    conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY, CacheConfig.CACHE_SEGMENT_SIZE_KEY), 
      Long.toString(rcOffheapCacheSegmentSize));
    if (rcACForOffheapCache && workloadType == WorkloadType.ZIPFIAN) {
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY, CacheConfig.CACHE_ADMISSION_CONTROLLER_IMPL_KEY),
        AQBasedAdmissionController.class.getName());
      conf.set(RowCacheConfig.toCarrotPropertyName(CacheType.MEMORY, CacheConfig.ADMISSION_QUEUE_START_SIZE_RATIO_KEY),
        Double.toString(rcACRatioForOffheapCache));
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
        rcFileCacheSizeLimit = Long.parseLong(args[++i]);
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
   * Start test
   * @param readPhase - read only phase
   * @return the execute thread[]
   */
  static ExecuteThread[] startTest(boolean readPhase) {
    String[] keyPrefix = new String[clientThreads];
    for (int i = 0; i < clientThreads; i++) {
      keyPrefix[i] = "Thread[" + i + "]";
    }
    ExecuteThread[] threadArray = new ExecuteThread[clientThreads];
    for (int i = 0; i < clientThreads; i++) {
      threadArray[i] = new ExecuteThread(keyPrefix[i]);
      if (readPhase) {
        threadArray[i].setReadPhase();
      }
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
    
    boolean loadPhase = true;
    
    /**
     * Checks if is read request.
     * @return true, if is read request
     */
    private final boolean isReadRequest() {
      if (testType == TestType.LOAD_THEN_READ) {
        if (seqNumber.get() < toLoad) {
          return false;
        } else {
          return true;
        }
      } else {
        if (seqNumber.get() < 1000) {
          return false;
        }
        double d = rnd.nextDouble();
        return d > writeRatio;
      }
    }

    public void setReadPhase() {
      loadPhase = false;
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
        e.printStackTrace();
        LOG.error("", e);
      }
    }

    /**
     * Test performance
     * @param key the key
     */
    private void testPerf(String key) {
      LOG.error("RowCache Performance test. Cache size =" + (nativeCache != null? nativeCache.size(): 0) + ": "
          + Thread.currentThread().getName());

      try {
 
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
          if (testType == TestType.LOAD_THEN_READ && loadPhase && seqNumber.get() >= toLoad) {
            break;
          }
        }
        LOG.error(getName() + ": Finished.");
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("", e);
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
      long cacheSize =  cacheItemsLimit;
      long maxItemNumber = seqNumber.get();
      maxItemNumber -= clientThreads + 1;
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
      boolean isReadRequest = isReadRequest();
//      if (loadPhase && isReadRequest) {
//        return;
//      }
      if (isReadRequest) {
        try {
          long l = getNextGetIndex();
          if (l < 0) l = 0;
          if (l >= seqNumber.get() - clientThreads - 1) {
            l = seqNumber.get() - clientThreads - 1;
          }
          byte[] row = getRow(l);
          List<Cell> results = null;
          
          while((results = getFromCache(row, map)) == null) ;
          
          if (maxVersions * FAMILIES.length * COLUMNS.length !=  results.size()){  
            LOG.info(String.format("Expected %d but got %d, index=%d", 
              maxVersions * FAMILIES.length * COLUMNS.length, results.size(), l ));
            byte[] arr = results.get(0).getRowArray();
            int off = results.get(0).getRowOffset();
            int len = results.get(0).getRowLength();
            if (Utils.compareTo(row, 0, row.length, arr, off, len) != 0) {
              LOG.error("\n\nWRONG ROW RECIEVED\n\n");
            }
            System.exit(-1);
          }  else {
            byte[] arr = results.get(0).getRowArray();
            int off = results.get(0).getRowOffset();
            int len = results.get(0).getRowLength();
            if (Utils.compareTo(row, 0, row.length, arr, off, len) != 0) {
              LOG.error("\n\nWRONG ROW RECIEVED\n\n");
              System.exit(-1);
            }
          }
          GETS.incrementAndGet();
        } catch (Exception e) {
          LOG.error("get native call.", e);
          System.exit(-1);
        }
      } else {
        try {
          long nextSeqNumber = seqNumber.getAndIncrement();
          cacheRow(nextSeqNumber);
          PUTS.incrementAndGet();
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

    ExecuteThread[] mThreads;
    long mInterval;
    long mStartTime;
    /**
     * Instantiates a new stats collector.
     * @param interval the interval
     * @param sources the sources
     */
    public StatsCollector(long interval, ExecuteThread[] sources) {
      super("StatsCollector");
      this.mThreads = sources;
      this.mInterval = interval;
      this.mStartTime = System.currentTimeMillis();
    }

    public void run() {
      
      if (rcCacheType == CacheType.HYBRID) {
        Scavenger.getStatisticsForCache(CacheType.MEMORY.getCacheName());
        Scavenger.getStatisticsForCache(CacheType.FILE.getCacheName());
      } else {
        Scavenger.getStatisticsForCache(rcCacheType.getCacheName());
      }
      while (true) {
        if (Thread.interrupted()) {
          return;
        }
        try {
          Thread.sleep(mInterval);
        } catch (InterruptedException e) {
          return;
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
            + "\n           GETS=" + GETS.get()//getTotalRequests() 
            + "\n           HITS=" + getTotalHits()
            + "\n           PUTS=" + PUTS.get()
            + "\n ALLOCATED SIZE=" + getMemAllocated() 
            + "\n      USED SIZE=" + getRawSize()
            + "\n   TOTAL WRITES=" + getTotalWrites());
            //+ "\n  TEST DIR SIZE=" + TestUtils.format(TestUtils.getDirectorySize(testDirPath)));
        if (scCacheType != SidecarDataCacheType.DISABLED) {
          Scavenger.printStats();
          SidecarCachingFileSystem.getDataCache().printStats();
          SidecarCachingFileSystem.getMetaCache().printStats();  
        }
      }
    }
  }

  public static String getTotalWrites() {
    return nativeCache != null? Long.toString(nativeCache.getTotalWrites()): "0";
  }
  
  public static String getTotalRejectedWrites() {
    return nativeCache != null? Long.toString(nativeCache.getTotalRejectedWrites()): "0";
  }
  
  /**
   * Gets the total items.
   * @return the total items
   */
  public static String getTotalItems() {
    if (nativeCache == null) {
      return "0";
    }
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
    if (nativeCache == null) {
      return "0";
    }
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
    if (nativeCache == null) {
      return "0";
    }
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
    if (nativeCache == null) {
      return "0";
    }
    long size = nativeCache.getStorageUsedActual();
    Cache victim = nativeCache.getVictimCache();
    if (victim != null) {
      size += victim.getStorageUsedActual();
    }
    return Long.toString(size);  
  }

  /**
   * Gets the total requests.
   * @return the total requests
   */
  public static String getTotalRequests() {
    if (nativeCache == null) {
      return "0";
    }
    return Long.toString(nativeCache.getTotalGets());
  }

  /**
   * Gets the total hits.
   * @return the total hits
   */
  public static String getTotalHits() {
    if (nativeCache == null) {
      return "0";
    }
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
    Random r = new Random(i);
    byte[] row = new byte[16];
    r.nextBytes(row);
    return row;
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
        .setBlocksize(blockSize)
        .setBlockCacheEnabled(hbaseBlockCacheEnabled)
        .setMaxVersions(maxVersions)
        .setValue(RConstants.ROWCACHE, "true".getBytes()).build();

    ColumnFamilyDescriptor famB = ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[1])
        .setBlocksize(blockSize)
        .setMaxVersions(maxVersions)
        .setBlockCacheEnabled(hbaseBlockCacheEnabled)
        .setValue(RConstants.ROWCACHE, "true".getBytes()).build();

    ColumnFamilyDescriptor famC = ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[2])
        .setBlocksize(blockSize)
        .setMaxVersions(maxVersions)
        .setBlockCacheEnabled(hbaseBlockCacheEnabled)
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
