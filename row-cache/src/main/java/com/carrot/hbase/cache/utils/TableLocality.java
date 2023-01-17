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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

// TODO: Auto-generated Javadoc
/**
 * The Class Configurer.
 */
@SuppressWarnings("deprecation")
public class TableLocality {

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(TableLocality.class);

  /**
   * The Enum Command.
   */
  private static enum Command {
    /** The status. */
    REGION_INFO
  }

  /** The Constant STATUS_COMMAND. */
  public final static String OPTIMIZE_COMMAND = "-opt";

  public final static String HELP_COMMAND = "-help";

  /** The command. */
  private static Command command;

  /** The table. */
  private static TableName table;

  private static Admin admin;
  private static Table hTable;
  private static FileSystem fs;
  private static Path tableRoot;

  /**
   * The main method.
   * @param args the arguments
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    parseArgs(args);
    init();
    if (admin == null) {
      System.exit(-1);
    }

    executeCommand(command);
  }

  private static void init() {
    Configuration config = HBaseConfiguration.create();
    try

    {
      Connection connection = ConnectionFactory.createConnection(config);
      admin = connection.getAdmin();
      hTable = connection.getTable(table);
      fs = FileSystem.get(new URI(config.get("hbase.rootdir")), new Configuration());
      tableRoot = new Path(config.get("hbase.rootdir"), table.getNameAsString());

    } catch (MasterNotRunningException e) {
      LOG.error(e);
    } catch (ZooKeeperConnectionException e) {
      LOG.error(e);
    } catch (IOException e) {
      LOG.error(e);
    } catch (URISyntaxException e) {
      LOG.error(e);
    }

  }

  /**
   * Execute command.
   * @param cmd the cmd
   * @throws IOException
   */
  private static void executeCommand(Command cmd) throws IOException {
    switch (cmd) {
      case REGION_INFO:
        dumpRegionsWithLocality(table);
        break;

    }

  }

  private static void dumpRegionsWithLocality(TableName t) throws IOException {
    // TODO Auto-generated method stub
    ;
    List<HRegionInfo> regions = admin.getTableRegions(t);
    Map<String, List<Path>> serverMap = new HashMap<String, List<Path>>();
    for (HRegionInfo ri : regions) {
      HRegionLocation loc = hTable.getRegionLocator().getRegionLocation(ri.getStartKey(), false);
      String name = ri.getEncodedName();
      String host = loc.getHostname();
      List<Path> pp = serverMap.get(host);
      if (pp == null) {
        pp = new ArrayList<Path>();
        serverMap.put(host, pp);

      }
      pp.add(new Path(tableRoot, name));
      // LOG.info("SERVER="+ loc.getHostname()+" : " + ri);

    }
    LOG.info("HDFS locality for table: " + table);
    for (String server : serverMap.keySet()) {
      LOG.info(server + " = " + localityForServer(server, serverMap.get(server)));
    }

    serverMap = optimizeLocality(serverMap);
    reassignRegions(serverMap);

  }

  private static void reassignRegions(Map<String, List<Path>> serverMap) throws IOException {
    LOG.info("Reassigning regions");
    // admin.disableTable(table.getBytes());

    Collection<ServerName> servers = admin.getClusterStatus().getServers();
    for (String server : serverMap.keySet()) {
      ServerName sn = find(server, servers);
      List<Path> regions = serverMap.get(server);
      for (Path p : regions) {
        LOG.info("Moving " + p.getName() + " to " + sn);
        admin.move(p.getName().getBytes(), sn.toString().getBytes());
      }
    }

    // admin.enableTable(table.getBytes());

  }

  private static ServerName find(String server, Collection<ServerName> servers) {
    for (ServerName sn : servers) {
      if (server.equals(sn.getHostname())) {
        return sn;
      }
    }
    return null;
  }

  private static Map<String, List<Path>> optimizeLocality(Map<String, List<Path>> serverMap)
      throws FileNotFoundException, IOException {
    LOG.info("Optimize locality starts");
    Set<String> servers = serverMap.keySet();
    Collection<List<Path>> regions = serverMap.values();
    ArrayList<Path> allRegions = new ArrayList<Path>();
    for (List<Path> rr : regions) {
      allRegions.addAll(rr);
    }

    // For all regions find server with max locality index
    int max = 0;
    Map<String, List<Path>> opt = new HashMap<String, List<Path>>();
    for (Path r : allRegions) {

      max = 0;
      String h = null;
      List<BlockLocation> blocks = getAllBlockLocations(r);
      for (String host : servers) {
        int locality = localityForServerBlocks(host, blocks);
        if (locality > max) {
          max = locality;
          h = host;
        }

      }

      LOG.info(r + " s=" + h + " locality=" + max);

      List<Path> pp = opt.get(h);
      if (pp == null) {
        pp = new ArrayList<Path>();
        opt.put(h, pp);
      }
      pp.add(r);
    }

    for (String s : opt.keySet()) {
      LOG.info(s + " r=" + opt.get(s).size() + " locality=" + localityForServer(s, opt.get(s)));

    }

    return opt;
  }

  private static int localityForServerBlocks(String host, List<BlockLocation> blocks)
      throws FileNotFoundException, IOException {
    long totalBlocks = 0;
    long localBlocks = 0;

    for (BlockLocation bl : blocks) {
      if (localBlock(bl, host)) {
        localBlocks++;
      }
      totalBlocks++;
    }

    return (int) (localBlocks * 100 / totalBlocks);
  }

  private static List<BlockLocation> getAllBlockLocations(Path r)
      throws FileNotFoundException, IOException {
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(r, true);
    List<BlockLocation> list = new ArrayList<BlockLocation>();

    while (it.hasNext()) {
      LocatedFileStatus st = it.next();
      BlockLocation[] locs = st.getBlockLocations();
      for (BlockLocation bl : locs) {
        list.add(bl);
      }
    }
    return list;
  }

  private static int localityForServer(String host, List<Path> regions)
      throws FileNotFoundException, IOException {
    int totalLocality = 0;
    int totalRegions = regions.size();

    for (Path r : regions) {
      List<BlockLocation> blocks = getAllBlockLocations(r);
      totalLocality += localityForServerBlocks(host, blocks);
    }
    return (totalLocality / totalRegions);
  }

  private static boolean localBlock(BlockLocation loc, String host) throws IOException {
    String[] hosts = loc.getHosts();
    for (String h : hosts) {
      if (h.equals(host)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parses the args.
   * @param args the args
   */
  private static void parseArgs(String[] args) {
    try {

      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (arg.equals(OPTIMIZE_COMMAND)) {
          command = Command.REGION_INFO;
          table = TableName.valueOf(args[++i]);
        } else if (arg.equals(HELP_COMMAND)) {
          usage();
        } else {
          LOG.error("Unrecognized argument: " + arg);
          System.exit(-1);
        }
      }

    } catch (Exception e) {
      LOG.error("Wrong input arguments", e);
      usage();
      System.exit(-1);
    }
  }

  /**
   * Usage.
   */
  private static void usage() {
    LOG.info("Usage\n"
        + " rowcache.sh command [table_name] [colfamily]\ncommand - one of -list, -status, -disable, -enable, -help");
  }

}
