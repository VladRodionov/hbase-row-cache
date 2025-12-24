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
package com.carrotdata.hbase.cache;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;

// TODO: Auto-generated Javadoc
/**
 * The Class RowCacheCoprocessor.
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
    static final Logger LOG = LoggerFactory.getLogger(RowCacheCoprocessor.class);      
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
    //rowCache.preClose(c.getEnvironment().getRegion().getTableDescriptor(), abortRequested);
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
  
  @Override
  public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
      throws IOException {
    // Check consistency, if strong - do not cache or return result from cache
    // Check Scan ID if not Set - set it org.apache.hadoop.hbase.client.Scan#ID_ATRIBUTE
    // Create a Key from a Scan object
    // 
  }

  @Override
  public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
      RegionScanner s) throws IOException {
    // Now we can return subclass which can work with cached data
    //LOG.info("postScannerOpen s=" + s);
    return s;
  }

  @Override
  public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s,
      List<Result> result, int limit, boolean hasNext) throws IOException {
    // RegionScannerImpl
    // we can use org.apache.hadoop.hbase.client.Scan#ID_ATRIBUTE
    //LOG.info("preScannerNext s=" + s);
    return hasNext;
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
      if(result == true) {
        e.bypass();
      }
      return result;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preGet(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Get, java.util.List)
   */
  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
      Get get, List<Cell> results) throws IOException {
    boolean bypass = rowCache.preGet(e.getEnvironment().getRegion().getTableDescriptor(), get, results);
    if(bypass) {
      e.bypass();
    }
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
  
  public RowCache getCache()
  {
    return rowCache;
  }

}
