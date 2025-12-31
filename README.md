# HBase Row Cache
HBase Row cache, powered by [Carrot Cache](https://github.com/carrotdata/carrot-cache)  and updated to HBase 2.x. This cache is impelemented as HBase co-processor and can be easily installed into any HBase cluster.
[Apache HBase](https://hbase.apache.org) is an open-source, distributed, versioned, column-oriented store modeled after Google' Bigtable: A Distributed Storage System for Structured Data by Chang et al [link](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf). Just as Bigtable leverages the distributed data storage provided by the Google File System, HBase provides Bigtable-like capabilities on top of [Apache Hadoop](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf). The official HBase still lacks one BigTable feature, namely - Row Cache, which caches frequently requested key-value pairs. This project adds this functionality to Apache HBase. 
You can read this Wiki page to get an idea why it is important, especially in disagregated storage environments: [HBase: Why Block Cahce Alone Is No Longer Enough In The Cloud](https://cloud.google.com/blog/products/databases/exploring-bigtable-read-throughput-performance-gains)

## Where is the BigTable's Row Cache?

* Row Cache in Goggle's BigTable is responsible for caching hot rows data. 
* It improves read performance when read operation is much smaller than block size (blocks are cached in a block cache) or involves multiple blocks to a fetch a single row
* This feature is still missing in HBase (as of 2.6.1).  
* It's very hard to implement in Java as since it puts extreme pressure on GC (when done on heap).
* GC pauses grow beyond acceptable levels in live enterprise environments due to heap fragmentation. 
* Unpredictability of GC interruption hurts as well.
* Read more about Google row cache in this [blog post](https://cloud.google.com/blog/products/databases/exploring-bigtable-read-throughput-performance-gains).

## Row Cache

* Fast multi-level (L1/L2/L3) Row Cache implementation for HBase (2.x).
* Its much more efficient than block cache when average read operation size is much less than block size because it does not pollute block cache with data that is not needed. 
* Supports in-memory, disk-only and hybrid modes.
* Cache size: 100's of GBs to TBs of RAM/Disk with low predictable query latency.
* Can be used with the following eviction policies (LRU, S-LRU, FIFO). S-LRU is default.
* Pure 100% - compatible Java. The only native code are Zstd compression codecs (multiple platforms are supported out of the box). It can works on any Java platform with compression disabled.
* Sub-millisecond latencies (on network),  zero GC.
* Implemented as RegionObserver co-processor.
* Easy installation and configuration. 

## Row Cache (Details)

* It caches data on read (read through cache). 
* Cache key is table:rowkey:family (table name + row key + column family)
* If row has multiple CFs (column families) - they will be cached separately.
* It caches the whole CF data (all column qualifiers + all versions) even if only part of it was requested.
* Works best when size of CF is < block size. With default block size of 64KB we are talking about kilobytes - not megabytes.
* Cached data is invalidated on every mutation for a particular rowkey:family. Every time, the column family is updated/deleted for a given row key, the corresponding entry gets evicted from the cache. 
* Make sure that your access is read mostly. 
* The ROW-CACHE can be enabled/disabled per table and per table:family (column family).
* The setting is visible in HBase shell and HBase UI. There is new ROW_CACHE attribute on a table and table:column.  
  The table:column settings of ROW_CACHE overwrites table's setting. 
* One can enable both: row cache and block cache on a table, but usually it either first or second needs to be enabled.
* Its very convenient for testing: run test with ROW_CACHE = false, then using provided utility - enable ROW_CACHE and re-run test. 
* Tables which have random (small) read mostly access pattern will benefit most from ROW-CACHE (make sure disable block cache).
* Tables, which are accessed in a more predictable sequential way, must have block cache enabled instead. 

## Memory usage and compression

* ROW-CACHE core caching library [Carrot Cache](https://www.github.com/carrotdata/carrot-cache) is between 2x and 6x more memory efficient than its direct competitors (Caffeine, EHCache)
  when compressioin is enabled. 
* It means that you will be able to keep 2-6x more data in a same amount of RAM (compared to a other caches with client-side compression enabled). Clear benefits. 
* Compression works fine even for small objects (10s - 100s of bytes). Please refer to this blog post for more information about how compression is implemented in `Carrot Cache`:
  [Memory Matters: Benchmarking Caching Servers with Membench](https://medium.com/carrotdata/memory-matters-benchmarking-caching-servers-with-membench-e6e3037aa201) (`Memcarrot` is powered by `Carrot Cache` as well)
* Our compression is not a simple server-side compression - it is a bit more trickier :)

## Performance and Scalability (Some old numbers, new ones will be coming soon)

* GET (small rows < 100 bytes): 175K operations per sec per one Region Server (from cache).
* MULTI-GET (small rows < 100 bytes): > 1M records per second (network limited) per one Region Server.
* LATENCY:  99% < 1ms (for GETs) with 100K ops on a small (4 nodes) cluster.
* Vertical scalability: tested up to 240GB (the maximum available in Amazon EC2).
* Horizontal scalability: limited by HBase scalability. 


## Limitations

* Caches the whole rowkey:family data even if only subset is requested (not a big deal when rows are small).
* Not suitable for large rows (100s of KB and above).
* Invalidates cache entry on each mutation operation, which involves this entry (rowkey:family). 
  Each time rowkey:family is updated, the corresponding cache entry is deleted to maintain full consistency.
* Make sure that your access is read mostly. 
* Current version supports Hadoop 2.x only (it can be rebuild with 3.x dependency, but I have not tried it yet). 

## Known issues

* No special handling for security/access control yet.
* Bulk loading does not invalidate cache
* Region reassignment handling. Scenario: Region R1 is reassined to a server B and then back to its original server A.
  In this scenario row cache in server A may containg stale data for the region R1.

## Things TODO

* Sparse rows support (cache only required data - not a whole row).
* Update (not delete) cached row on mutation operation.
* Better eviction policy, which takes into account size of a row.
* Smart limit of cached row size (prevent unrestricted rows growth). 

## Dependencies

* [Carrot Cache](https://www.github.com/carrotdata/carrot-cache). This is new open source caching library under Apache 2.0 license. Cache implementation can be made pluggable.

## How to build

Clone repo and checkout the latest release (tag). Run `mvn clean install -DskipTests` for a `hbase-row-cache`. After build is complete you will get the binaries under `dist/target` directory.

## Installation

Note: Install software on HBase Master host first.

1. Shutdown HBase cluster
2. Extract `rowcache-x.y.z.tar.gz` tar file.
3. Run `install.sh` script. (Note: HBASE_HOME MUST be defined)
   
The installation script will:

* copies all needed library jars into `$HBASE_HOME/lib`,
* copies shell scripts (rcadmin.sh and synccluster.sh) into `$HBASE_HOME/bin` directory.
* copies Row Cache configuration template file into `$HBASE_HOME/conf` directory.
* will update `$HBASE_HOME/bin/hbase` shell script to include specific row cache jar files
* (the copy of previous version of hbase script will be created).


4. Modify `$HBASE_HOME/conf/hbase-site.xml` to include RowCache specific configuration parameters
   (see ./conf/hbase-site.xml.template for the list of parameters and CONFIGURATION manual)
5. Sync `$HBASE_HOME/` across HBase cluster using provided `synccluster.sh`.
6. Start HBase cluster.


## Administration

Row cache can be enabled/disabled per table:column_family

+ To view row cache configuration for a particular table:

```
$HBASE_HOME/bin/rcadmin.sh status TABLE
```

+ To enable row cache for a table

```
$HBASE_HOME/bin/rcadmin.sh enable TABLE
```

+ To enable row cache for a table:cf

```
$HBASE_HOME/bin/rcadmin.sh enable TABLE FAMILY
```

+ To disable row cache for a table

```
$HBASE_HOME/bin/rcadmin.sh disable TABLE
```

+ To disable row cache for a table:cf

```
$HBASE_HOME/bin/rcadmin.sh disable TABLE FAMILY  
```

## Configuration

### 1. Introduction

All RowCache - specific configuration options must be defined in hbase-site.xml file. After installation is complete
you must update hbase-site.xml and sync these files on all HBase cluster's nodes. HBase RowCache is backed by Carrot Cache - high performance,
memory efficient, hybrid (Memory/SSD) cache. Plese refer to https://github.com/carrotdata/carrot-cache for more information about Carrot Cache.

### 2. RowCache - specific configuration options

There are only few of RowCache - specific configuration options and majority of configuration options come from a Carrot Cache.

#### 2.1 Defines RowCache coprocessor main class

```xml
<property>
<name>hbase.coprocessor.user.region.classes</name>        
        <value>com.carrotdata.hbase.cache.RowCacheCoprocessor</value>
</property>   
```

#### 2.2 Is this cache persistent or not.

Persistence in this context means that cache is stored on disk upon region server shutdown
and is loaded from disk (see 'offheap.blockcache.storage.dir') upon server's start up. 
Default is false. (Optional)

```xml
<property>
        <name>rowcache.persistent</name>
        <value>false</value>
</property>    
```

#### 2.3 Cache type

MEMORY, FILE or HYBRID (MEMORY + FILE). Default: MEMORY

```xml
<property>
        <name>rowcache.type</name>
        <value>MEMORY</value>
</property>
```

#### 2.3 Enable/Disable JMX metrics

Default: true

```xml
<property>
        <name>rowcache.jmx.metrics.enabled</name>
        <value>true</value>
</property>
```

#### 2.4 JMX domain name

Default: "HBase-RowCache"

```xml
<property>
        <name>rowcache.jmx.metrics.domain.name</name>
        <value>some-name</value>
</property>
```

### 3. Carrot Cache configuration

Important: Carrot Cache configuration option names MUST be prefixed with "cc."
In this example we set size of a MEMORY cache (MEMORY cache name is "rowcache-memory")

```xml
<property>
        <name>cc.rowcache-memory.storage.size.max</name>
        <value>10000000000</value>
</property>
```

This one set FILE cache size (FILE cache name is "rowcache-file")

```xml
<property>
        <name>cc.rowcache-file.storage.size.max</name>
        <value>10000000000</value>
</property>
```

For the full list of the Carrot Cache configuration options please refer to the Carrot Cache [Documentation](https://github.com/carrotdata/carrot-cache/wiki/Overview)
