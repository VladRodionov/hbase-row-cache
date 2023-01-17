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
package com.carrot.hbase.cache.utils;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

// TODO: Auto-generated Javadoc
/**
 * The Class Hints.
 */
public class Hints {

	/** The Constant CACHE_BYPASS. */
	public final static byte[] CACHE_BYPASS     = "cache$bypass".getBytes();
	
	/** The Constant CACHE_EXACT. */
	public final static byte[] CACHE_EXACT      = "cache$exact".getBytes();
	
	/** The Constant CACHE_ON_WRITE. */
	public final static byte[] CACHE_ON_WRITE   = "cache$on-write".getBytes();
	
	
	/**
	 * Bypass cache.
	 *
	 * @param get the get
	 * @return true, if successful
	 */
	public final static boolean bypassCache(Get get)
	{
		Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
		return map.containsKey(CACHE_BYPASS);
	}
	
	/**
	 * Bypass cache.
	 *
	 * @param get the get
	 * @param clean the clean
	 * @return true, if successful
	 */
	public final static boolean bypassCache(Get get, boolean clean)
	{
		Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
		boolean result = map.containsKey(CACHE_BYPASS);
		if(result && clean){
			map.remove(CACHE_BYPASS);
		}
		return result;
	}
	
	/**
	 * Exact cache.
	 *
	 * @param get the get
	 * @return true, if successful
	 */
	public final static boolean exactCache(Get get)
	{
		Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
		return map.containsKey(CACHE_EXACT);
	}
	
	/**
	 * Exact cache.
	 *
	 * @param get the get
	 * @param clean the clean
	 * @return true, if successful
	 */
	public final static boolean exactCache(Get get, boolean clean)
	{
		Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
		boolean result = map.containsKey(CACHE_EXACT);
		if(result && clean){
			map.remove(CACHE_EXACT);
		}
		return result;
	}
	/**
	 * Currently we do not support hints in Increment
	 * Only Put and Append.
	 *
	 * @param op the op
	 * @return true, if successful
	 */
	public final static boolean onWriteCache( Mutation op)
	{
		if( op instanceof Put){
			Put put = (Put) op;
		    Map<byte[], List<Cell>> map = put.getFamilyCellMap();
		    return map.containsKey(CACHE_ON_WRITE);
		} else if( op instanceof Append){
			Append append = (Append) op;
			Map<byte[], List<Cell>> map = append.getFamilyCellMap();
		    return map.containsKey(CACHE_ON_WRITE);
			
		}
		return false;
	}
	
	/**
	 * On write cache.
	 *
	 * @param op the op
	 * @param clean the clean
	 * @return true, if successful
	 */
	public final static boolean onWriteCache( Mutation op, boolean clean)
	{
		boolean result = false;
		Map<byte[], List<Cell>> map = null;
		if( op instanceof Put){
			Put put = (Put) op;
		    map = put.getFamilyCellMap();
		} else if( op instanceof Append){
			Append append = (Append) op;
			map = append.getFamilyCellMap();			
		}
	    if( map != null){
	    	result = map.containsKey(CACHE_ON_WRITE);
	    	if(result && clean){
	    		map.remove(CACHE_ON_WRITE);
	    	}
	    }	    
		return result;
	}
}
