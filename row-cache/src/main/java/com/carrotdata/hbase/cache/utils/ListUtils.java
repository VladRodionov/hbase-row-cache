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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class ListUtils.
 */
public class ListUtils {

	/**
	 * One list minus two list. Both lists MUST be sorted
	 *
	 * @param <T> the generic type
	 * @param one the one
	 * @param two the two
	 * @param comparator the comparator
	 * @param result the result
	 * @return the list
	 */
	public static <T> List<T> minus(List<T> one, List<T> two, Comparator<T> comparator, List<T> result)
	{
		for(int i=0; i < one.size(); i++){
			T obj = one.get(i);
			if(Collections.binarySearch(two, obj, comparator) >=0) {
				continue;
			}
			result.add(obj);
		}
		return result;
	}
	
	/**
	 * One list plus (union) two list. Both lists MUST be sorted
	 *
	 * @param <T> the generic type
	 * @param one the one
	 * @param two the two
	 * @param comparator the comparator
	 * @param result the result
	 * @return the list
	 */
	public static <T> List<T> plus(List<T> one, List<T> two, Comparator<T> comparator, List<T> result)
	{
		for(int i=0; i < one.size(); i++){
			T obj = one.get(i);
			int index = Collections.binarySearch(two, obj, comparator);
			if( index >= 0) {
				// Found. remove from second collection
				two.remove(index);
			}
			// Add object to result collection
			result.add(obj);
		}
		// Add rest of a second collection
		result.addAll(two);
		return result;
	}
}
