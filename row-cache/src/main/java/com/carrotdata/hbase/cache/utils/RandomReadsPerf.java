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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;


public class RandomReadsPerf {
    /** The Constant LOG. */
    static final Logger LOG = LoggerFactory.getLogger(RandomReadsPerf.class);	
    private static TableName TABLE = TableName.valueOf("usertable".getBytes());
    
	private static String THREADS = "-t";
	private static String OPS     = "-ops";
	private static String BATCH   = "-b";
	private static String REUSE_CONFIG = "-rc";
	private static String SEED    = "-s"; 
	private static String TOTAL_RECORDS = "-rec";
	private static String TOTAL_READ    = "-tr";

	
	private static int threads = 8;
	private static int ops     = 100000;
	private static int batchSize = 1;
	
	private static AtomicLong  startTime = new AtomicLong(0);
	private static AtomicLong  endTime   = new AtomicLong(0);
	private static AtomicLong secondPassStartTime = new AtomicLong(0);
	private static AtomicLong  failed = new AtomicLong(0);
	private static AtomicLong  completed = new AtomicLong(0);
	private static AtomicLong  totalSize = new AtomicLong(0);
	private static Timer timer ;
	private static boolean reuseConfig = false;
	private static boolean seedSpecified = false;
	private static boolean singlePass = false;
	private static int seed;
	private static long totalRecords;
	private static long totalRead;
	private static CountDownLatch latch ;
	
	
	static class Stats extends TimerTask
	{

		
		@Override
		public void run() {
			long start = startTime.get();
			
			if(start > 0 && completed.get() > 0){
				long current = System.currentTimeMillis();
				if( singlePass == false){
					if(secondPassStartTime.get() == 0){
						LOG.info("First pass: "+((double) completed.get() * 1000)/ (current - start)+" RPS. failed="+failed.get()+
								" Avg size="+ (totalSize.get()/completed.get())+ " completed="+completed.get()+" of "+ totalRead+" %%="+ (completed.get() * 100)/totalRead);
					} else{
						long compl = completed.get() - totalRead;
						start = secondPassStartTime.get();
						LOG.info("Second pass: "+((double) compl * 1000)/ (current - start)+" RPS. failed="+failed.get()+
							" Avg size="+ (totalSize.get()/completed.get())+ " completed="+compl+" of "+ 
							(totalRead )+" %%="+ (compl * 100)/(totalRead));
					
					}
				} else {
					LOG.info("Stats : "+((double) completed.get() * 1000)/ (current - start)+" RPS. failed="+failed.get()+
							" Avg size="+ (totalSize.get()/completed.get())+ " completed="+completed.get()+" of "+ totalRead+" %%="+ (completed.get() * 100)/totalRead);
				}
			}
			
		}
		
	}
	
	static class Worker extends Thread
	{
		Configuration cfg;
		Random random ;
		long toRead;
		int id = 0;
		long done = 0;
		long opsDone =0;
		private long rSeed = System.currentTimeMillis();

		public Worker(int id, long toRead)
		{
			super("read-no-reuse#"+id);
			this.cfg =  HBaseConfiguration.create();
			//random = new Random(id);
			this.toRead = toRead;
			this.id = id;
			if(seedSpecified) rSeed = 0;
			
			
		}
		
		public Worker(int id, Configuration cfg, long toRead)
		{
			super("read-reuse#"+id);
			this.cfg =  cfg;
			this.toRead = toRead;
			//random = new Random(id);
			this.id = id;
			
		}
		
		public void run()
		{
			LOG.info(Thread.currentThread().getName()+" starting ...");
			
			try (Connection connection = ConnectionFactory.createConnection(cfg);
	         Admin admin = connection.getAdmin()) {
				Table table =  connection.getTable(TABLE);
				LOG.info(Thread.currentThread().getName()+" acquired HTable instance and started.");
				startTime.compareAndSet(0, System.currentTimeMillis());
				int counter = 0;
				int numPasses = singlePass? 1: 2;
				
				LOG.info(Thread.currentThread().getName()+" warm-up starts");
				while( counter++ < numPasses){
					opsDone  = 0;
					random = new Random(rSeed + id + seed*threads);
					if( counter == 2){
						
						latch.countDown();
						try {
							// wait for all threads finish up first pass
							latch.await();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						secondPassStartTime.compareAndSet(0, System.currentTimeMillis());
						LOG.info(Thread.currentThread().getName()+" second pass starts.");
						//toRead = totalRead;
						
					}
					while(opsDone < toRead){
						// First pass warm - up
						List<Get> gets = createBatch();
						Result[] r = table.get(gets);
						checkFailed(r);
						completed.addAndGet(batchSize);
						for(Result res: r){
						
							totalSize.addAndGet(getLength(res));
						}
					
					}
				}
				LOG.info(Thread.currentThread().getName()+" finished");
				endTime.set(System.currentTimeMillis());
			} catch (IOException e) {
				LOG.error(Thread.currentThread().getName(), e);
			}
			
		}

		/**
		 * This is approximate length
		 * 
		 * @param r
		 * @return length 
		 */
		private int getLength(Result r)
		{
			int len =0;
			for(Cell c: r.listCells()){
				len += c.getFamilyLength() + c.getQualifierLength() + c.getRowLength() + c.getValueLength();
			}
			return len;
		}
		
		private void checkFailed(Result[] r) {			
			for(Result res: r){
				if(res.isEmpty()) failed.incrementAndGet();
			}			
		}

		private List<Get> createBatch() {
			List<Get> gets = new ArrayList<Get>();
			for(int i=0; i < batchSize; i++){
				long index = Math.abs(random.nextLong()) % totalRecords;
				byte[] key = ("user" + Utils.hash(index)).getBytes();
				gets.add(new Get(key));
				opsDone++;
			}
			return gets;
		}
	}
	
	public static void main(String[] args) throws IOException{
		parseArgs(args);
		
		latch = new CountDownLatch(threads);
		
		Configuration cfg = HBaseConfiguration.create();
						
		Worker[] workers = new Worker[threads];
		for(int i=0; i < threads; i++){
			workers[i] = (reuseConfig == false)?new Worker(i, totalRead/threads): new Worker(i, cfg, totalRead/threads);
			workers[i].start();
		}
		
		// Start stats
		timer = new Timer();
		timer.schedule( new Stats(), 5000, 5000);
		// Join all workers
		for(int i =0; i < threads; i++){
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
		
		if( singlePass == false){
			LOG.info("Finished: first pass : "+((double) completed.get() * 1000)/ (2*(secondPassStartTime.get() - startTime.get()))+" RPS");
			LOG.info("Finished: second pass: "+((double) completed.get() * 1000)/ (2*(endTime.get() - secondPassStartTime.get()))+" RPS");
		} else{
			LOG.info("Finished: "+((double) completed.get() * 1000)/ ((endTime.get() - startTime.get()))+" RPS");
		}
		System.exit(0);
	}





	private static void parseArgs(String[] args) {
	      try{
	            
	            for(int i=0; i < args.length; i++)
	            {   
	                String arg= args[i];
	                if(arg.equals(THREADS)){  
	                    threads  = Integer.parseInt(args[++i]);	                    	                    
	                } else if(arg.equals(OPS)){
	                    ops = Integer.parseInt(args[++i]);
	                } else if(arg.equals(BATCH)){
	                    batchSize = Integer.parseInt(args[++i]);
	                } else if(arg.equals(REUSE_CONFIG)){
	                    reuseConfig = true;
	                } else if(arg.equals(SEED)){
	                    seedSpecified = true;
	                    singlePass = true;
	                	seed = Integer.parseInt(args[++i]);
	                } else if(arg.equals(TOTAL_RECORDS)){
	                    totalRecords = Long.parseLong(args[++i]);
	                } else if(arg.equals(TOTAL_READ)){
	                    totalRead = Long.parseLong(args[++i]);
	                } else{
	                    LOG.error("Unrecognized argument: "+arg);
	                    System.exit(-1);
	                }
	            }               
	            
	        }catch(Exception e){
	            LOG.error("Wrong input arguments", e);
	           
	            System.exit(-1);
	        }
	        
	        LOG.info("Threads       =" + threads);
	        LOG.info("Operations    =" + ops);
	        LOG.info("Batch size    =" + batchSize);
	        LOG.info("Reuse config  =" + reuseConfig);
	        LOG.info("Seed          =" + seed);
	        LOG.info("Total records =" + totalRecords);
	        LOG.info("Total reads    =" + totalRead);
		
	}
}
