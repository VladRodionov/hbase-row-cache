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

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;

// TODO: Auto-generated Javadoc
/**
 * The Class ScanCacheCoprocessor.
 * 1. Coprocessor instance is one per table region
 * 2. New instance is created every time the region gets enabled.
 * 3. Call sequence: 
 *     1. new
 *     2. start
 *     3. preOpen 
 *     4. postOpen
 *     
 *     On disable:
 *     1. preClose
 *     2. postClose
 *     3. stop
 * 
 *  Alter Table
 *  
 *  Altering table may:
 *  1. Delete existing CF
 *  2. Modify existing CF
 *  3. Adding new CF
 *  
 *  How does this affect RowCache?
 *  - Deleting existing CF - NO if we HAVE CHECK on preGet that CF exists and cacheable
 *  - Modifying existing CF - TTL and ROWCACHE are of interest. on preOpen we update TTL map, on preGet we check
 *                            if CF exists and is row-cacheable - NO
 *  - adding new CF    - NO at all
 *  
 *   Plan:  
 *   1. on preOpen update only TTL map, 
 *   2. remove preClose, do not do any cache delete operations.
 *   3. keep preBulkLoad. On bulk load table which fully or partially row-cacheable we DELETE all cache
 *   
 *                            
 * 
 */
public class RowCacheCoprocessor implements RegionCoprocessor, RegionObserver {


  /** The Constant LOG. */
    static final Log LOG = LogFactory.getLog(RowCacheCoprocessor.class);      
  /** The scan cache. */
  RowCache rowCache;
  
  public RowCacheCoprocessor()
  {
    LOG.info("[row-cache] new instance.");
  }
  
  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> c,
      boolean abortRequested) throws IOException {
       
    LOG.info("[row-cache][preClose] " + c.getEnvironment().getRegion().getRegionInfo().toString());
    // We disabled preClose --?????
    //rowCache.preClose(c.getEnvironment().getRegion().getTableDesc(), abortRequested);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext)
   */
  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e)
      throws IOException {
    LOG.info("[row-cache][preOpen] " + e.getEnvironment().getRegion().getRegionInfo().toString());
    rowCache.preOpen(e.getEnvironment().getRegion().getTableDescriptor());
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#postBulkLoadHFile(org.apache.hadoop.hbase.coprocessor.ObserverContext, java.util.List, boolean)
   */
  @Override
  public void preBulkLoadHFile(
      ObserverContext<RegionCoprocessorEnvironment> ctx,
      List<Pair<byte[], String>> familyPaths)
      throws IOException {

      rowCache.preBulkLoadHFile(ctx.getEnvironment().getRegion().getTableDescriptor(), familyPaths);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preAppend(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Append)
   */
  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> e,
      Append append) throws IOException {

      return rowCache.preAppend(e.getEnvironment().getRegion().getTableDescriptor(), append);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preCheckAndDelete(org.apache.hadoop.hbase.coprocessor.ObserverContext, byte[], byte[], byte[], org.apache.hadoop.hbase.filter.CompareFilter.CompareOp, org.apache.hadoop.hbase.filter.WritableByteArrayComparable, org.apache.hadoop.hbase.client.Delete, boolean)
   */
//  @Override
//  public boolean preCheckAndDelete(
//      ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
//      byte[] family, byte[] qualifier, CompareOperator compareOp,
//      ByteArrayComparable comparator, Delete delete,
//      boolean result) throws IOException {
//      return rowCache.preCheckAndDelete(e.getEnvironment().getRegion().getTableDescriptor(), row, family, qualifier, delete, result);
//  }
//
//  /* (non-Javadoc)
//   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preCheckAndPut(org.apache.hadoop.hbase.coprocessor.ObserverContext, byte[], byte[], byte[], org.apache.hadoop.hbase.filter.CompareFilter.CompareOp, org.apache.hadoop.hbase.filter.WritableByteArrayComparable, org.apache.hadoop.hbase.client.Put, boolean)
//   */
//  @Override
//  public boolean preCheckAndPut(
//      ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
//      byte[] family, byte[] qualifier, CompareOperator compareOp,
//      ByteArrayComparable comparator, Put put, boolean result)
//      throws IOException {
//
//      return rowCache.preCheckAndPut(e.getEnvironment().getRegion().getTableDescriptor(), row, family, 
//        qualifier, put, result);
//  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preDelete(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Delete, org.apache.hadoop.hbase.regionserver.wal.WALEdit, boolean)
   */
  @Override
  public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
      Delete delete, WALEdit edit, Durability durability) throws IOException {

      rowCache.preDelete(e.getEnvironment().getRegion().getTableDescriptor(), delete);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preIncrement(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Increment)
   */
  @Override
  public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> e,
      Increment increment) throws IOException {

      return rowCache.preIncrement(e.getEnvironment().getRegion().getTableDescriptor(), increment, null);
  }


  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#prePut(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Put, org.apache.hadoop.hbase.regionserver.wal.WALEdit, boolean)
   */
  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
      Put put, WALEdit edit, Durability durability) throws IOException 
  {
    rowCache.prePut(e.getEnvironment().getRegion().getTableDescriptor(), put);
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#postGet(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Get, java.util.List)
   */
  @Override
  public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
      Get get, List<Cell> results) throws IOException 
  {
    rowCache.postGet(e.getEnvironment().getRegion().getTableDescriptor(), get, results); 
  }


  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preExists(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Get, boolean)
   */
  @Override
  public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> e,
      Get get, boolean exists) throws IOException 
  {

      boolean result = rowCache.preExists(e.getEnvironment().getRegion().getTableDescriptor(), get, exists);
      if( result == true) e.bypass();
      return result;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preGet(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Get, java.util.List)
   */
  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
      Get get, List<Cell> results) throws IOException {
    
    //*DEBUG*/ LOG.info("Coproc preGet obj=" + this + " thread=" + 
    //Thread.currentThread().getName() + " count=" + (count++)); 
    //Thread.dumpStack();
    boolean bypass = rowCache.preGet(e.getEnvironment().getRegion().getTableDescriptor(), get, results);
      if(bypass) e.bypass();
    
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#start(org.apache.hadoop.hbase.CoprocessorEnvironment)
   */
  @SuppressWarnings("rawtypes")
  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    
    LOG.info("[row-cache][start coprocessor]");
    rowCache = new RowCache();
    rowCache.start(e.getConfiguration());     
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#stop(org.apache.hadoop.hbase.CoprocessorEnvironment)
   */
  @SuppressWarnings("rawtypes")
  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {

      LOG.info("[row-cache][stop coprocessor]");
      rowCache.stop(e);  
  }
  
  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }
  
  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    int num = miniBatchOp.size();
    TableDescriptor tableDesc = c.getEnvironment().getRegion().getTableDescriptor();
    for (int i = 0; i < num; i++) {
      Mutation m = miniBatchOp.getOperation(i);
      if (m instanceof Put) {
        rowCache.prePut(tableDesc, (Put) m);
      } else if (m instanceof Delete) {
        rowCache.preDelete(tableDesc, (Delete) m);
      } else if (m instanceof Append) {
        rowCache.preAppend(tableDesc, (Append) m);
      } else if (m instanceof Increment) {
        rowCache.preIncrement(tableDesc, (Increment) m, null);
      }
    }
  }

  @Override
  public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Put put,
      boolean result) throws IOException {
    if (result) {
      rowCache.preCheckAndPut(c.getEnvironment().getRegion().getTableDescriptor(), 
        row, family, qualifier, put, result);
    }
    return result;
  }

  @Override
  public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
      Delete delete, boolean result) throws IOException {
    if (result) {
      rowCache.preCheckAndDelete(c.getEnvironment().getRegion().getTableDescriptor(), 
        row, family, qualifier, delete, result);
    }
    return result;  
  }

  @Override
  public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      Filter filter, Put put, boolean result) throws IOException {
    if (result) {
      rowCache.preCheckAndPut(c.getEnvironment().getRegion().getTableDescriptor(), 
        row, null, null, put, result);
    }  
    return result;
  }

  @Override
  public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      Filter filter, Delete delete, boolean result) throws IOException {
    if (result) {
      rowCache.preCheckAndDelete(c.getEnvironment().getRegion().getTableDescriptor(), 
        row, null, null, delete, result);
    }  
    return result;
  }

  @Override
  public CheckAndMutateResult postCheckAndMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      CheckAndMutate checkAndMutate, CheckAndMutateResult result) throws IOException {
    if (result.isSuccess()) {
      Row action = checkAndMutate.getAction();
      
      if (action instanceof Put) {
        postCheckAndPut(c, null, null, (Put) action, true);
      } else if (action instanceof Delete) {
        postCheckAndDelete(c, null, null, (Delete) action, true);
      } else if (action instanceof Append) {
        preAppend(c, (Append) action);
      } else if (action instanceof Increment) {
        preIncrement(c, (Increment) action);
      }
    }
    return result;
  }

  public RowCache getCache()
  {
    return rowCache;
  }

}
