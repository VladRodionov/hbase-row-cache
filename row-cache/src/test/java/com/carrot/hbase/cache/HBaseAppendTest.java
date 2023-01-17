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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * The Class HBaseAppendTest.
 */
public class HBaseAppendTest {

	
	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(CoprocessorBaseTest.class);	  
	
	/** The util. */
	private static HBaseTestingUtility UTIL = new HBaseTestingUtility();	
	
	/** The table a. */
	protected byte[] TABLE_A = "TABLE_A".getBytes();
	
	/** The families. */
	protected byte[][] FAMILIES = new byte[][]
	    {"fam_a".getBytes(), "fam_b".getBytes(), "fam_c".getBytes()};
	
	/** The columns. */
	protected  byte[][] COLUMNS = 
		{"col_a".getBytes(), "col_b".getBytes(),  "col_c".getBytes()};
	
	/** The versions. */
	int VERSIONS = 10;	
	
	/** The cluster. */
	MiniHBaseCluster cluster;
	
	/** The table desc. */
	TableDescriptor tableDesc; 	
	
	/** The table. */
	Table table;
	
	Connection conn;
	
	Admin admin;
	
	@Before
	public void setUp() throws Exception {
		Configuration conf = UTIL.getConfiguration();
		conf.set("hbase.zookeeper.useMulti", "false");
		UTIL.startMiniCluster(1);
		cluster = UTIL.getMiniHBaseCluster();
	  createHBaseTable();	    
	}	
	
	@After
	public void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
	}
	
	/**
	 * Creates the HBase table.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
  protected void createHBaseTable() throws IOException {

    LOG.error("Create HBase table and put data");
    ColumnFamilyDescriptor famA =
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[0]).setMaxVersions(VERSIONS).build();
    ColumnFamilyDescriptor famB =
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[1]).setMaxVersions(VERSIONS).build();
    ColumnFamilyDescriptor famC =
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[2]).setMaxVersions(VERSIONS).build();
    tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_A)).setColumnFamily(famA)
        .setColumnFamily(famB).setColumnFamily(famC).build();
    
    Configuration cfg = cluster.getConf();

    conn = ConnectionFactory.createConnection(cfg);
    admin = conn.getAdmin();
    if (admin.tableExists(tableDesc.getTableName()) == false) {
      admin.createTable(tableDesc);
      LOG.error("Created table " + tableDesc);
    }
    table = conn.getTable(TableName.valueOf(TABLE_A));

    // Create row
    List<Cell> rowData = generateRowData();
    Put put = createPut(rowData);
    // Put data
    table.put(put);
    LOG.error("Finished.");

  }
	
	/**
	 * Creates the put.
	 *
	 * @param values the values
	 * @return the put
	 * @throws IOException 
	 */
	protected Put createPut(List<Cell> values) throws IOException
	{
		Put put = new Put(TestUtils.getRow(values.get(0)));
		for(Cell kv: values)
		{
			put.add(kv);
		}
		return put;
	}
		
	/**
	 * Generate row data.
	 *
	 * @return the list
	 */
	List<Cell> generateRowData(){
		byte[] row = "row".getBytes();
		byte[] value = "value".getBytes();		
		long startTime = System.currentTimeMillis();
		ArrayList<Cell> list = new ArrayList<Cell>();
		int count = 0;
		for(byte[] f: FAMILIES){
			for(byte[] c: COLUMNS){
				count = 0;
				for(; count < VERSIONS; count++){
					KeyValue kv = new KeyValue(row, f, c, startTime - 1000*(count),  value);	
					list.add(kv);
				}
			}
		}		
		Collections.sort(list, CellComparator.getInstance());		
		return list;
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
	 * Test append.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testAppend() throws IOException
	{
		LOG.error("Test append started. Testing row: 'row'");
		
		byte[] row ="row".getBytes();
		byte[] toAppend = "_appended".getBytes();
		
		Get get = new Get(row);
		get.readVersions(Integer.MAX_VALUE);
		Result result = table.get(get);	
		assertEquals(90, result.size() );
		
		Append append = createAppend(row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS), toAppend);
		Result r = table.append(append);		
		assertEquals(9, r.size());

		get = new Get(row);
		get.readVersions(Integer.MAX_VALUE);
		result = table.get(get);
		assertEquals (90, result.size());
		LOG.error("Test append finished.");
	}
}
