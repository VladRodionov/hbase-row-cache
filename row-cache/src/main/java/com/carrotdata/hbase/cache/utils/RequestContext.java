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

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;


// TODO: Auto-generated Javadoc
/**
 * The Class RequestContext.
 */
public class RequestContext {
	
	/** The filter. */
	private Filter filter;
	
	/** The time range. */
	private TimeRange timeRange = null;
	
	/** The max versions. */
	private int maxVersions = Integer.MAX_VALUE;
	
	/** The exact cache. */
	private boolean exactCache  = false;
	
	/** The cache on write. */
	private boolean cacheOnWrite = false;	
	
	/** The bypass cache. */
	private boolean bypassCache = false;
	
	/** The family map. */
	private Map<byte[], NavigableSet<byte[]>> familyMap = 
		 new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
	
	/**
	 * Gets the filter.
	 *
	 * @return the filter
	 */
	public Filter getFilter() {
		return filter;
	}
	
	/**
	 * Sets the filter.
	 *
	 * @param filter the new filter
	 */
	public void setFilter(Filter filter) {
		this.filter = filter;
	}
	
	/**
	 * Gets the time range.
	 *
	 * @return the time range
	 */
	public TimeRange getTimeRange() {
		return timeRange;
	}
	
	/**
	 * Sets the time range.
	 *
	 * @param timeRange the new time range
	 */
	public void setTimeRange(TimeRange timeRange) {
		this.timeRange = timeRange;
	}
	
	/**
	 * Gets the max versions.
	 *
	 * @return the max versions
	 */
	public int getMaxVersions() {
		return maxVersions;
	}
	
	/**
	 * Sets the max versions.
	 *
	 * @param maxVersions the new max versions
	 */
	public void setMaxVersions(int maxVersions) {
		this.maxVersions = maxVersions;
	}
	
	/**
	 * Checks if is bypass cache.
	 *
	 * @return true, if is bypass cache
	 */
	public boolean isBypassCache() {
		return bypassCache;
	}
	
	/**
	 * Sets the bypass cache.
	 *
	 * @param bypassCache the new bypass cache
	 */
	public void setBypassCache(boolean bypassCache) {
		this.bypassCache = bypassCache;
	}
	
	/**
	 * Checks if is exact cache.
	 *
	 * @return true, if is exact cache
	 */
	public boolean isExactCache() {
		return exactCache;
	}
	
	/**
	 * Sets the exact cache.
	 *
	 * @param exactCache the new exact cache
	 */
	public void setExactCache(boolean exactCache) {
		this.exactCache = exactCache;
	}
	
	/**
	 * Checks if is cache on write.
	 *
	 * @return true, if is cache on write
	 */
	public boolean isCacheOnWrite() {
		return cacheOnWrite;
	}
	
	/**
	 * Sets the cache on write.
	 *
	 * @param cacheOnWrite the new cache on write
	 */
	public void setCacheOnWrite(boolean cacheOnWrite) {
		this.cacheOnWrite = cacheOnWrite;
	}

	/**
	 * Sets the family map.
	 *
	 * @param map the map
	 */
	public void setFamilyMap(Map<byte[], NavigableSet<byte[]>> map)
	{
		familyMap.clear();
		if(map == null) return;
		
		for(byte[] f: map.keySet()){
			NavigableSet<byte[]> cols = map.get(f);
			// Nullify current entry
			map.put(f, (NavigableSet<byte[]>)null);
			familyMap.put(f, cols);
		}
	}
	
	/**
	 * Gets the family map.
	 *
	 * @return the family map
	 */
	public Map<byte[], NavigableSet<byte[]>> getFamilyMap()
	{
		return familyMap;
	}
	
	
}
