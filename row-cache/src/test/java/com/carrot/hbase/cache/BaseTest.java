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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;


// TODO: Auto-generated Javadoc
/**
 * The Class BaseTest.
 */
public class BaseTest {

 	/** The Constant LOG. */
	 static final Log LOG = LogFactory.getLog(BaseTest.class);	
	/** The table a. */
	protected static byte[] TABLE_A = "TABLE_A".getBytes();
	
	/** The table b. */
	protected static byte[] TABLE_B = "TABLE_B".getBytes();
	
	/** The table c. */
	protected static byte[] TABLE_C = "TABLE_C".getBytes();
	
	/* Families */
	/** The families. */
	protected static byte[][] FAMILIES = new byte[][]
	      {"fam_a".getBytes(), "fam_b".getBytes(), "fam_c".getBytes()};
	
	
	/* Columns */
	/** The columns. */
	protected  static byte[][] COLUMNS = 
	{"col_a".getBytes(), "col_b".getBytes(),  "col_c".getBytes()};
	
	/** The data. */
	static List<List<Cell>> data ;
	
	
	/** The versions. */
	static int VERSIONS = 10;
	
	/** The cache. */
	static RowCache cache;
	
	/* All CF are cacheable */
	/** The table a. */
	static TableDescriptor tableA;
	/* fam_c not cacheable */
	/** The table b. */
	static TableDescriptor tableB;
	
	/* Not row cacheable */
	/** The table c. */
	static TableDescriptor tableC;
	
	
	/**
	 * Cache row.
	 *
	 * @param table the table
	 * @param rowNum the row num
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected static void cacheRow(TableDescriptor table, int rowNum) throws IOException
	{
		List<Cell> list = data.get(rowNum);
		Get get = createGet(getRow(list.get(0)), null, null, null);
		get.readVersions(Integer.MAX_VALUE);
		cache.resetRequestContext();
		cache.postGet(table, get, list);				
	}
	
	protected static byte[] getRow(Cell cell) {
	  byte[] arr = cell.getRowArray();
	  int off = cell.getRowOffset();
	  int len = cell.getRowLength();
	  byte[] row = new byte[len];
	  System.arraycopy(arr, off, row, 0, len);
	  return row;
	}
	
  protected byte[] getFamily(Cell cell) {
    byte[] arr = cell.getFamilyArray();
    int off = cell.getFamilyOffset();
    int len = cell.getFamilyLength();
    byte[] row = new byte[len];
    System.arraycopy(arr, off, row, 0, len);
    return row;
  }
	
  protected byte[] getQualifier(Cell cell) {
    byte[] arr = cell.getQualifierArray();
    int off = cell.getQualifierOffset();
    int len = cell.getQualifierLength();
    byte[] row = new byte[len];
    System.arraycopy(arr, off, row, 0, len);
    return row;
  }
  
  protected byte[] getValue(Cell cell) {
    byte[] arr = cell.getValueArray();
    int off = cell.getValueOffset();
    int len = cell.getValueLength();
    byte[] row = new byte[len];
    System.arraycopy(arr, off, row, 0, len);
    return row;
  }
  
	/**
	 * Gets the from cache.
	 *
	 * @param table the table
	 * @param row the row
	 * @param families the families
	 * @param columns the columns
	 * @return the from cache
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected List<Cell> getFromCache(TableDescriptor table, byte[] row, List<byte[]> families, List<byte[]> columns) throws IOException
	{
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(families, columns);
		Get get = createGet(row, map, null, null);	
		get.readVersions(Integer.MAX_VALUE);
		List<Cell> results = new ArrayList<Cell>();
		cache.preGet(table, get, results);		
		return results;
		
	}

	/**
	 * Constract family map.
	 *
	 * @param families the families
	 * @param columns the columns
	 * @return the map
	 */
	protected Map<byte[], NavigableSet<byte[]>> constructFamilyMap(List<byte[]> families, List<byte[]> columns)
	{
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		NavigableSet<byte[]> colSet = getColumnSet(columns);
		for(byte[] f: families){
			map.put(f, colSet);
		}
		return map;
	}
	
	/**
	 * Gets the row.
	 *
	 * @param i the i
	 * @return the row
	 */
	static byte[] getRow (int i){
		return ("row"+i).getBytes();
	}
	
	/**
	 * Gets the value.
	 *
	 * @param i the i
	 * @return the value
	 */
	static byte[] getValue (int i){
		return ("value"+i).getBytes();
	}
	
	
	/**
	 * Generate row data.
	 *
	 * @param i the i
	 * @return the list
	 */
	static List<Cell> generateRowData(int i){
		byte[] row = getRow(i);
		byte[] value = getValue(i);
		long startTime = System.currentTimeMillis();
		ArrayList<Cell> list = new ArrayList<Cell>();
		int count = 0;
		for(byte[] f: FAMILIES){
			for(byte[] c: COLUMNS){
				count = 0;
				for(; count < VERSIONS; count++){
					KeyValue kv = new KeyValue(row, f, c, startTime + (count),  value);	
					list.add(kv);
				}
			}
		}
		
		Collections.sort(list, CellComparator.getInstance());
		
		return list;
	}
	
	
	/**
	 * Generate data.
	 *
	 * @param n the n
	 * @return the list
	 */
	static List<List<Cell>> generateData(int n)
	{
		List<List<Cell>> data = new ArrayList<List<Cell>>();
		for(int i=0; i < n; i++){
			data.add(generateRowData(i));
		}
		return data;
	}
	
	/**
	 * Creates the tables.
	 *
	 * @param versions the versions
	 */
	protected static void createTables(int versions) {
		
		ColumnFamilyDescriptor famA =  
		    ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[0])
		    .setValue(RConstants.ROWCACHE, "true".getBytes())
		    .setMaxVersions(versions).build();
	
	  ColumnFamilyDescriptor famB =  
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[1])
        .setValue(RConstants.ROWCACHE, "true".getBytes())
        .setMaxVersions(versions).build();
		
	  ColumnFamilyDescriptor famC =  
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[2])
        .setValue(RConstants.ROWCACHE, "true".getBytes())
        .setMaxVersions(versions).build();
	  
		tableA = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_A))
		          .setColumnFamily(famA).setColumnFamily(famB).setColumnFamily(famC).build();

    ColumnFamilyDescriptor famA1 =  
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[0])
        .setValue(RConstants.ROWCACHE, "true".getBytes())
        .setMaxVersions(versions).build();
  
    ColumnFamilyDescriptor famB1 =  
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[1])
        .setValue(RConstants.ROWCACHE, "true".getBytes())
        .setMaxVersions(versions).build();
    
    ColumnFamilyDescriptor famC1 =  
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[2])
        .setMaxVersions(versions).build();
    
    tableB = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_B))
              .setColumnFamily(famA1).setColumnFamily(famB1).setColumnFamily(famC1).build();
		
    ColumnFamilyDescriptor famA2 =  
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[0])
        .setMaxVersions(versions).build();
  
    ColumnFamilyDescriptor famB2 =  
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[1])
        .setMaxVersions(versions).build();
    
    ColumnFamilyDescriptor famC2 =  
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[2])
        .setMaxVersions(versions).build();
    
    
    tableC = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_C))
              .setColumnFamily(famA2).setColumnFamily(famB2).setColumnFamily(famC2).build();
    
		
	}
	
 	
 	/**
	 * Creates the get.
	 *
	 * @param row the row
	 * @param familyMap the family map
	 * @param tr the tr
	 * @param f the f
	 * @return the gets the
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected static Get createGet(byte[] row, Map<byte[], NavigableSet<byte[]>> familyMap, TimeRange tr, Filter f ) throws IOException
	{
		Get get = new Get(row);
		if(tr != null){
			get.setTimeRange(tr.getMin(), tr.getMax());
		}
		if(f != null) get.setFilter(f);
		
		if(familyMap != null){
			for(byte[] fam: familyMap.keySet())
			{
				NavigableSet<byte[]> cols = familyMap.get(fam);
				if( cols == null || cols.size() == 0){
					get.addFamily(fam);
				} else{
					for(byte[] col: cols)
					{
						get.addColumn(fam, col);
					}
				}
			}
		}
		return get;
	}
	
	/**
	 * Creates the put.
	 *
	 * @param values the values
	 * @return the put
	 */
	protected Put createPut(List<Cell> values)
	{
		Cell c = values.get(0);
	  Put put = new Put(c.getRowArray(), c.getRowOffset(), c.getRowLength());
		for(Cell kv: values)
		{
			try {
        put.add(kv);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
		}
		return put;
	}
	
	
	/**
	 * Creates the increment.
	 *
	 * @param row the row
	 * @param familyMap the family map
	 * @param tr the tr
	 * @param value the value
	 * @return the increment
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected Increment createIncrement(byte[] row, Map<byte[], NavigableSet<byte[]>> familyMap, TimeRange tr, long value) 
	throws IOException
	{
		Increment incr = new Increment(row);
		if(tr != null){
			incr.setTimeRange(tr.getMin(), tr.getMax());
		}

		if(familyMap != null){
			for(byte[] fam: familyMap.keySet())
			{
				NavigableSet<byte[]> cols = familyMap.get(fam);

					for(byte[] col: cols)
					{
						incr.addColumn(fam, col, value);
					}
				
			}
		}
		return incr;		
	}
	
	/**
	 * Creates the append.
	 *
	 * @param row the row
	 * @param families the families
	 * @param columns the columns
	 * @param value the value
	 * @return the append
	 */
	protected Append createAppend(byte[] row, List<byte[]> families, List<byte[]> columns, byte[] value){
		Append op = new Append(row);
		for(byte[] f: families){
			for(byte[] c: columns){
				op.addColumn(f, c, value);
			}
		}
		return op;
	}
	
	/**
	 * Creates the delete.
	 *
	 * @param values the values
	 * @return the delete
	 */
	protected Delete createDelete(List<Cell> values)
	{
	  Cell c = values.get(0);
		Delete del = new Delete(c.getRowArray(), c.getRowOffset(), c.getRowLength());
		for(Cell kv: values)
		{
			try {
        del.add(kv);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
		}
		return del;
	}
	
	/**
	 *  Creates simple scan 
	 * @param startRow
	 * @param stopRow
	 * @return scan
	 */
	protected Scan createSimpleScan (byte[] startRow, byte[] stopRow) {
	  Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);
	  scan.addFamily(FAMILIES[0]);
	  return scan;
	}
	
	/**
	 * Creates the delete.
	 *
	 * @param row the row
	 * @return the delete
	 */
	protected Delete createDelete(byte[] row)
	{
		Delete del = new Delete(row);
		return del;
	}
	
	/**
	 * Creates the delete.
	 *
	 * @param row the row
	 * @param families the families
	 * @return the delete
	 */
	protected Delete createDelete(byte[] row, List<byte[]> families)
	{
		Delete del = new Delete(row);
		for(byte[] f: families)
		{
			del.addFamily(f);
		}
		return del;
	}

	/**
	 * Equals.
	 *
	 * @param list1 the list1
	 * @param list2 the list2
	 * @return true, if successful
	 */
	protected static boolean equals(List<Cell> list1, List<Cell> list2)
	{
		if(list1.size() != list2.size()) return false;
		Collections.sort(list1, CellComparator.getInstance());
		Collections.sort(list2, CellComparator.getInstance());	
		for(int i=0; i < list1.size(); i++){
		  Cell first = list1.get(i);
		  Cell second = list2.get(i);
			if(first.equals(second) == false) return false;
		}
		return true;
	}
	
	 protected boolean equalsNoTS(List<Cell> list1, List<Cell> list2)
	  {
	    if(list1.size() != list2.size()) return false;
	    Collections.sort(list1, CellComparator.getInstance());
	    Collections.sort(list2, CellComparator.getInstance()); 
	    for(int i=0; i < list1.size(); i++){
	      Cell first = list1.get(i);
	      Cell second = list2.get(i);
	      
	      int r1 = Bytes.compareTo(getRow(first), getRow(second));
	      if(r1 != 0) return false;
	      int r2 = Bytes.compareTo(getFamily(first), getFamily(second));
	      if(r2 != 0) return false;
	      int r3 = Bytes.compareTo(getQualifier(first), getQualifier(second));
	      if(r3 != 0) return false;
	      int r4 = Bytes.compareTo(getValue(first), getValue(second));
	      if(r4 != 0) {
	        return false;
	      } 
	    }
	    return true;
	  }
	/**
	 * Sub list.
	 *
	 * @param list the list
	 * @param family the family
	 * @return the list
	 */
	protected List<Cell> subList(List<Cell> list, byte[] family){
		List<Cell> result = new ArrayList<Cell>();
		for(Cell kv : list){
			if(Bytes.equals(family, getFamily(kv))){
				result.add(kv);
			}
		}
		return result;
	}
	
	/**
	 * Sub list.
	 *
	 * @param list the list
	 * @param family the family
	 * @param cols the cols
	 * @return the list
	 */
	protected List<Cell> subList(List<Cell> list, byte[] family, List<byte[]> cols){
		List<Cell> result = new ArrayList<Cell>();
		for(Cell kv : list){
			if(Bytes.equals(family, getFamily(kv))){
				byte[] col = getQualifier(kv);
				for(byte[] c: cols){					
					if(Bytes.equals(col, c)){
						result.add(kv); break;
					}
				}
			}
		}
		return result;
	}
	
	/**
	 * Sub list.
	 *
	 * @param list the list
	 * @param families the families
	 * @param cols the cols
	 * @return the list
	 */
	protected List<Cell> subList(List<Cell> list, List<byte[]> families, List<byte[]> cols){
		List<Cell> result = new ArrayList<Cell>();
		for(Cell kv : list){
			for(byte[] family: families){				
				if(Bytes.equals(family, getFamily(kv))){
					byte[] col = getQualifier(kv);
					for(byte[] c: cols){					
						if(Bytes.equals(col, c)){
							result.add(kv); break;
						}
					}				
				}
			}
		}
		return result;
	}
	
	
	/**
	 * Sub list.
	 *
	 * @param list the list
	 * @param families the families
	 * @param cols the cols
	 * @param max the max
	 * @return the list
	 */
	protected List<Cell> subList(List<Cell> list, List<byte[]> families, List<byte[]> cols, int max){
		List<Cell> result = new ArrayList<Cell>();
		for(Cell kv : list){
			for(byte[] family: families){				
				if(Bytes.equals(family, getFamily(kv))){
					byte[] col = getQualifier(kv);
					for(byte[] c: cols){					
						if(Bytes.equals(col, c)){
							result.add(kv); break;
						}
					}				
				}
			}
		}
		
		int current = 0;
		byte[] f = getFamily(result.get(0));
		byte[] c = getQualifier(result.get(0));
		
		List<Cell> ret = new ArrayList<Cell>();
		
		for(Cell kv : result ){
			byte[] fam = getFamily(kv);
			byte[] col = getQualifier(kv);
			if(Bytes.equals(f, fam) ){
				if( Bytes.equals(c, col)){
					if( current < max){
						ret.add(kv);
					}
					current++;
				} else{
					c = col; current = 1;
					ret.add(kv);
				}
			} else {
				f = fam; c = col; current = 1;
				ret.add(kv);
			}
		}
		return ret;
	}
	/**
	 * Gets the column set.
	 *
	 * @param cols the cols
	 * @return the column set
	 */
	protected NavigableSet<byte[]> getColumnSet(List<byte[]> cols)
	{
		if(cols == null) return null;
		TreeSet<byte[]> set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		for(byte[] c: cols){
			set.add(c);
		}
		return set;
	}
	
	/**
	 * Dump.
	 *
	 * @param list the list
	 */
	protected void dump(List<Cell> list)
	{
		for(Cell kv : list){
			dump(kv);
		}
	}
	
	/**
	 * Dump.
	 *
	 * @param kv the kv
	 */
	protected void dump(Cell kv)
	{
		LOG.info("row="+new String(getRow(kv))+" family="+ new String(getFamily(kv))+
				" column="+new String(getQualifier(kv)) + " ts="+ kv.getTimestamp());
	}
	
	/**
	 * Patch row.
	 *
	 * @param kv the kv
	 * @param patch the patch
	 */
	protected void patchRow(KeyValue kv, byte[] patch)
	{
		int off = kv.getRowOffset();
		System.arraycopy(patch, 0, kv.getBuffer(), off, patch.length);
	}
	
}
