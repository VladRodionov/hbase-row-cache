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
package com.carrotdata.hbase.cache.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;

import com.carrotdata.hbase.cache.RConstants;


@SuppressWarnings("deprecation")
public class UserTableLoader {
  /** The Constant LOG. */
  static final Logger LOG = LoggerFactory.getLogger(UserTableLoader.class);
  private static byte[] TABLE = "usertable".getBytes();
  private static byte[] FAMILY = "cf".getBytes();
  private static byte[] COLUMN = "col".getBytes();

  private static String THREADS = "-t";
  private static String RECORDS = "-rec";
  private static String START_RECORD = "-startRec";
  private static String BATCH = "-b";
  private static String REUSE_CONFIG = "-rc";
  private static String VALUE_SIZE = "-vs";
  private static String BLOCK_SIZE = "-bs";
  private static String ROW_CACHE_DISABLED = "-rowCacheDisabled";

  private static int threads = 8;
  private static long records = 100000;
  private static long startRecordNumber = 0;
  private static int batchSize = 1;
  private static int valueSize = 10;
  private static int blockSize = 64 * 1024;

  private static AtomicLong startTime = new AtomicLong(0);
  private static AtomicLong endTime = new AtomicLong(0);
  private static AtomicLong failed = new AtomicLong(0);
  private static AtomicLong completed = new AtomicLong(0);
  private static AtomicLong totalSize = new AtomicLong(0);
  private static long lastId;
  private static Timer timer;
  private static boolean reuseConfig = false;
  private static boolean rowCacheEnabled = true;

  static class Stats extends TimerTask {

    @Override
    public void run() {
      long start = startTime.get();

      if (start > 0) {
        long current = System.currentTimeMillis();
        LOG.info(((double) (completed.get() - startRecordNumber) * 1000) / (current - start)
            + " RPS. failed=" + failed.get() + " Avg size=" + (totalSize.get() / completed.get()));
      }

    }

  }

  static class Worker extends Thread {
    Configuration cfg;
    int id;
    Random r;

    public Worker(int id) {
      super("worker-no-reuse#" + id);
      this.id = id;
      this.cfg = HBaseConfiguration.create();
      r = new Random(id);

    }

    public Worker(int id, Configuration cfg) {
      super("worker-reuse#" + id);
      this.id = id;
      this.cfg = cfg;
      r = new Random(id);
    }

    public void run() {
      LOG.info(Thread.currentThread().getName() + " starting ...");
      TableName tableName = TableName.valueOf(TABLE);
      try (Connection connection = ConnectionFactory.createConnection(cfg);
          BufferedMutator mutator = connection.getBufferedMutator(tableName);) {
        // TODO: setAutoFlush(false) - HOW?
        LOG.info(Thread.currentThread().getName() + " acquired HTable instance and started.");
        startTime.compareAndSet(0, System.currentTimeMillis());
        long counter = 0;
        while (counter < lastId) {
          counter = completed.getAndAdd(batchSize);
          if (counter > lastId) {
            break;
          }
          List<Put> puts = createBatch(counter);
          mutator.mutate(puts);
          // completed.addAndGet(batchSize);
        }
        mutator.flush();
        LOG.info(Thread.currentThread().getName() + " finished");
        endTime.set(System.currentTimeMillis());
      } catch (IOException e) {
        LOG.error(Thread.currentThread().getName(), e);
      }

    }

    private List<Put> createBatch(long counter) {
      List<Put> puts = new ArrayList<Put>();

      for (int i = 0; i < batchSize; i++) {
        byte[] key = ("user" + hash(counter + i)).getBytes();
        byte[] value = new byte[valueSize];
        r.nextBytes(value);
        Put p = new Put(key);
        p.addColumn(FAMILY, COLUMN, value);
        puts.add(p);
      }
      return puts;
    }

    private long hash(long l) {
      return Utils.hash(l);
    }
  }

  private static void recreateTable(Configuration cfg) throws IOException {

    TableName tableName = TableName.valueOf(TABLE);
    try (Connection connection = ConnectionFactory.createConnection(cfg);
        Admin admin = connection.getAdmin();) {

      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);

      }
      // Create table

      HColumnDescriptor col = new HColumnDescriptor(FAMILY);
      col.setBlockCacheEnabled(true);
      col.setMaxVersions(1);
      col.setBloomFilterType(BloomType.ROWCOL);
      col.setCompressionType(Algorithm.GZ);
      col.setBlocksize(blockSize);
      // enable row cache
      if (rowCacheEnabled) {
        col.setValue(RConstants.ROWCACHE, "true".getBytes());
      } else {
        col.setValue(RConstants.ROWCACHE, "false".getBytes());
      }

      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      tableDesc.addFamily(col);
      // tableDesc.setDeferredLogFlush(true);
      tableDesc.setMaxFileSize(10000000000L);

      byte[][] splits = new byte[][] { "user05".getBytes(), "user1".getBytes(), "user15".getBytes(),
          "user2".getBytes(), "user25".getBytes(), "user3".getBytes(), "user35".getBytes(),
          "user4".getBytes(), "user45".getBytes(), "user5".getBytes(), "user55".getBytes(),
          "user6".getBytes(), "user65".getBytes(), "user7".getBytes(), "user75".getBytes(),
          "user8".getBytes(), "user85".getBytes(), "user9".getBytes(), "user95".getBytes() };

      admin.createTable(tableDesc, splits);
    }
    LOG.info("Created table: usertable");
  }

  public static void main(String[] args) throws IOException {
    parseArgs(args);

    Configuration cfg = HBaseConfiguration.create();

    if (startRecordNumber == 0) {
      recreateTable(cfg);
    }

    // set current id to startRecordNumber
    completed.set(startRecordNumber);

    lastId = startRecordNumber + records;

    Worker[] workers = new Worker[threads];
    for (int i = 0; i < threads; i++) {
      workers[i] = (reuseConfig == false) ? new Worker(i) : new Worker(i, cfg);
      workers[i].start();
    }

    // Start stats
    timer = new Timer();
    timer.schedule(new Stats(), 5000, 5000);
    // Join all workers
    for (int i = 0; i < threads; i++) {
      try {
        workers[i].join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        // e.printStackTrace();
      }
    }

    LOG.info("Finished: " + ((double) records * 1000) / (endTime.get() - startTime.get()) + " RPS");
    System.exit(-1);
  }

  private static void parseArgs(String[] args) {
    try {

      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (arg.equals(THREADS)) {
          threads = Integer.parseInt(args[++i]);
        } else if (arg.equals(RECORDS)) {
          records = Long.parseLong(args[++i]);
        } else if (arg.equals(BATCH)) {
          batchSize = Integer.parseInt(args[++i]);
        } else if (arg.equals(REUSE_CONFIG)) {
          reuseConfig = true;
        } else if (arg.equals(VALUE_SIZE)) {
          valueSize = Integer.parseInt(args[++i]);
        } else if (arg.equals(BLOCK_SIZE)) {
          blockSize = Integer.parseInt(args[++i]);
        } else if (arg.equals(START_RECORD)) {
          startRecordNumber = Long.parseLong(args[++i]);
        } else if (arg.equals(ROW_CACHE_DISABLED)) {
          rowCacheEnabled = false;
        } else {
          LOG.error("Unrecognized argument: " + arg);
          System.exit(-1);
        }
      }

    } catch (Exception e) {
      LOG.error("Wrong input arguments", e);

      System.exit(-1);
    }

    LOG.info("Threads      =" + threads);
    LOG.info("Records      =" + records);
    LOG.info("Start record =" + startRecordNumber);
    LOG.info("Block size   =" + blockSize);
    LOG.info("Batch size   =" + batchSize);
    LOG.info("Reuse config =" + reuseConfig);
    LOG.info("Value size   =" + valueSize);
    LOG.info("RowCache     =" + rowCacheEnabled);

  }
}
