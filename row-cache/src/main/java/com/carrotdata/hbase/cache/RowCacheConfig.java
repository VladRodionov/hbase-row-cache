/**
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
package com.carrotdata.hbase.cache;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import com.carrotdata.cache.util.CacheConfig;


public class RowCacheConfig extends Properties {
  
  private static final long serialVersionUID = 1L;
  
  static final Logger LOG = LoggerFactory.getLogger(RowCacheConfig.class);  

  public final static String CACHE_MEMORY_NAME = "rowcache-memory";
  
  public final static String CACHE_FILE_NAME = "rowcache-file";
  
  public final static String ROWCACHE_JMX_METRICS_ENABLED_KEY ="rowcache.jmx.metrics.enabled";
  
  public final static String ROWCACHE_JMX_METRICS_DOMAIN_NAME_KEY ="rowcache.jmx.metrics.domain.name";
  
  public final static String ROWCACHE_PERSISTENT_KEY = "rowcache.persistent";
  
  public final static String ROWCACHE_IO_BUFFER_SIZE_KEY = "rowcache.io.buffer.size";
  
  public final static String ROWCACHE_TYPE_KEY = "rowcache.type";// 'offheap', 'file', 'hybrid'
        
  public final static String DEFAULT_ROWCACHE_JMX_METRICS_ENABLED = "true";
  
  public final static String DEFAULT_ROWCACHE_JMX_METRICS_DOMAIN_NAME = "HBase-RowCache";
  
  public final static String DEFAULT_ROWCACHE_PERSISTENT = "true";
  
  
  public final static String DEFAULT_ROWCACHE_IO_BUFFER_SIZE = Integer.toString(1<<20); // 1MB
  
  public final static long DEFAULT_ROWCACHE_OFFHEAP_SIZE = 1L << 30; // 1GB
  
  public final static long DEFAULT_ROWCACHE_FILE_SIZE = 100 * (1L << 30); // 100GB
  
  public final static long DEFAULT_ROWCACHE_OFFHEAP_SEGMENT_SIZE = 1 << 22; // 4MB
  
  public final static long DEFAULT_ROWCACHE_FILE_SEGMENT_SIZE = (1 << 26); // 64MB
  
  public final static CacheType DEFAULT_ROWCACHE_TYPE = CacheType.MEMORY;
  
  public final static EvictionPolicy DEFAULT_OFFHEAP_EVICTION_POLICY = EvictionPolicy.SLRU;
  
  public final static EvictionPolicy DEFAULT_FILE_EVICTION_POLICY = EvictionPolicy.SLRU;
  
  public final static RecyclingSelector DEFAULT_OFFHEAP_RECYCLING_SELECTOR = RecyclingSelector.LRC;
  
  public final static RecyclingSelector DEFAULT_FILE_RECYCLING_SELECTOR = RecyclingSelector.LRC;
  
  private static RowCacheConfig instance;
  
  private static CacheConfig carrotConfig;
  
  private RowCacheConfig() {
  }
  
  public synchronized static RowCacheConfig fromHadoopConfiguration(Configuration conf) {
    
    RowCacheConfig config = getInstance();
    Iterator<Map.Entry<String, String>> it = conf.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      String name = entry.getKey();
      if (isRowcachePropertyName(name)) {
        LOG.info("RowCache "+ name + " value=" + entry.getValue());

        config.setProperty(name, entry.getValue());
      } else if (CacheConfig.isCarrotPropertyName(name)) {
        LOG.info("Carrot "+ name + " value=" + entry.getValue());
        carrotConfig.setProperty(name, entry.getValue());
      }
    }
    return instance;
  }
  
  public synchronized static RowCacheConfig getInstance() {
    if (instance == null) {
      instance = new RowCacheConfig();
      carrotConfig = CacheConfig.getInstance();
    }
    return instance;
  }
  
  private static boolean isRowcachePropertyName(String name) {
    return name.startsWith("rowcache.");
  }

  private static boolean isCarrotPropertyName(String name) {
    return name.startsWith("cc.");
  }
  
  public static String toCarrotPropertyName(CacheType type, String name) {
    switch(type) {
      case MEMORY:
        return CACHE_MEMORY_NAME + "." + name;
      case FILE:
        return CACHE_FILE_NAME + "." + name;
      case HYBRID:
      default:
        return null;
    }
  }
  
  /**
   * Is JMX metrics enabled
   * @return true or false
   */
  public boolean isJMXMetricsEnabled() {
    String value = getProperty(ROWCACHE_JMX_METRICS_ENABLED_KEY, DEFAULT_ROWCACHE_JMX_METRICS_ENABLED);
    return Boolean.parseBoolean(value);
  }
  
  /**
   * Get JMX domain name
   * @return jmx domain name
   */
  public String getJMXDomainName() {
    String v = getProperty(ROWCACHE_JMX_METRICS_DOMAIN_NAME_KEY, 
      DEFAULT_ROWCACHE_JMX_METRICS_DOMAIN_NAME);
    try {
      v += "-" + InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
    }
    return v;
  }
  
  /**
   * Is cache persistent
   * @return true or false
   */
  public boolean isCachePersistent() {
    String value = getProperty(ROWCACHE_PERSISTENT_KEY, DEFAULT_ROWCACHE_PERSISTENT);
    return Boolean.parseBoolean(value);
  }
 
  /**
   * Get I/O buffer size in bytes
   * @return I/O buffer size in bytes
   */
  public int getIOBufferSize() {
    String value = getProperty(ROWCACHE_IO_BUFFER_SIZE_KEY, DEFAULT_ROWCACHE_IO_BUFFER_SIZE);
    return Integer.parseInt(value);
  }
  
  /**
   * Get row-cache type (offheap or file)
   * @return cache type
   */
  public CacheType getCacheType() {
    String typeName = getProperty(ROWCACHE_TYPE_KEY);
    if (typeName == null) {
      return DEFAULT_ROWCACHE_TYPE;
    }
    return CacheType.valueOf(typeName.toUpperCase());
  }
  
  /**
   * Sets row-cache type
   * @param type cache type
   * @return configuration instance
   */
  public RowCacheConfig setCacheType(CacheType type) {
    setProperty(ROWCACHE_TYPE_KEY, type.name());
    return this;
  }
  
  /**
   * Get cache maximum size for a given type
   * @param type cache type
   * @return cache maximum size
   */
  public long getCacheMaxSize(CacheType type) {
    long size;
    switch(type) {
      case MEMORY:
        size = carrotConfig.getCacheMaximumSize(CACHE_MEMORY_NAME);
        if (size == 0) {
          size = DEFAULT_ROWCACHE_OFFHEAP_SIZE;
        }
        break;
      case FILE:
        size = carrotConfig.getCacheMaximumSize(CACHE_FILE_NAME);
        if (size == 0) {
          size = DEFAULT_ROWCACHE_FILE_SIZE;
        }
        break;
      case HYBRID:
      default:
        return 0;
    }
    return size;
  }
  
  /**
   * Sets cache maximum size
   * @param type cache type
   * @param maxSize maximum size
   * @return configuration instance
   */
  public RowCacheConfig setCacheMaxSize(CacheType type, long maxSize) {
    String cacheName = type.getCacheName();
    if (cacheName != null) {
      carrotConfig.setCacheMaximumSize(cacheName, maxSize);
    }
    return this;
  }
  
  /**
   * Get cache segment size
   * @param type cache type
   * @return segment size
   */
  public long getCacheSegmentSize(CacheType type) {
    long size;
    switch(type) {
      case MEMORY:
        size = carrotConfig.getCacheSegmentSize(CACHE_MEMORY_NAME);
        if (size == 0) {
          size = DEFAULT_ROWCACHE_OFFHEAP_SEGMENT_SIZE;
        }
        break;
      case FILE:
        size = carrotConfig.getCacheSegmentSize(CACHE_FILE_NAME);
        if (size == 0) {
          size = DEFAULT_ROWCACHE_FILE_SEGMENT_SIZE;
        }
        break;
      case HYBRID:
      default:
        return 0;
    }
    return size;
  }
  
  /**
   * Sets cache segment size
   * @param type cache type
   * @param size segment size
   * @return configuration instance
   */
  public RowCacheConfig setCacheSegmentSize(CacheType type, long size) {
    String cacheName = type.getCacheName();
    if (cacheName != null) {
      carrotConfig.setCacheSegmentSize(cacheName, size);
    }
    return this;
  }
  /**
   * Set eviction policy for cache
   * @param type cache type
   * @param policy eviction policy 
   * @return configuration instance
   */
  public RowCacheConfig setEvictionPolicy(CacheType type, EvictionPolicy policy) {
    String cacheName = type.getCacheName();
    if (cacheName != null) {
      carrotConfig.setCacheEvictionPolicy(cacheName, policy.getClassName());
    }
    return this;
  }
  
  /**
   * Set recycling selector
   * @param type cache type
   * @param selector selector's 
   * @return configuration instance
   */
  public RowCacheConfig setRecyclingSelector(CacheType type, RecyclingSelector selector) {
    String cacheName = type.getCacheName();
    if (cacheName != null) {
      carrotConfig.setRecyclingSelector(cacheName, selector.getClassName());
    }
    return this;
  }
  
  public RowCacheConfig setCarrotProperty(CacheType type, String propertyName, String value) {
    String carrotPropertyName = toCarrotPropertyName(type, propertyName);
    carrotConfig.setProperty(carrotPropertyName, value);
    return this;
  }
  
}
