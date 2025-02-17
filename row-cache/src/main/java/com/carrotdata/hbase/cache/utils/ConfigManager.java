/*******************************************************************************
* Copyright (c) 2013 Vladimir Rodionov. All Rights Reserved
*
* This code is released under the GNU Affero General Public License.
*
* See: http://www.fsf.org/licensing/licenses/agpl-3.0.html
*
* VLADIMIR RODIONOV MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY
* OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
* IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR
* NON-INFRINGEMENT. Vladimir Rodionov SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED
* BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR
* ITS DERIVATIVES.
*
* Author: Vladimir Rodionov
*
*******************************************************************************/
package com.carrotdata.hbase.cache.utils;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;

import com.carrotdata.hbase.cache.RConstants;


// TODO: Auto-generated Javadoc
/**
 * The Class Configurer.
 */
@SuppressWarnings("deprecation")
public class ConfigManager {

    /** The Constant LOG. */
    static final Logger LOG = LoggerFactory.getLogger(ConfigManager.class);	 
	  
	/**
	 * The Enum Command.
	 */
	private static enum Command{
		
		/** The status. */
		STATUS ,
		
		/** The list. */
		LIST,
		
		/** The enable. */
		ENABLE,		
		
		/** The disable. */
		DISABLE 				
	}
	
	/** The Constant STATUS_COMMAND. */
	public final static String STATUS_COMMAND  = "status";
	
	/** The Constant ENABLE_COMMAND. */
	public final static String ENABLE_COMMAND  = "enable";
	
	/** The Constant DISABLE_COMMAND. */
	public final static String DISABLE_COMMAND = "disable";
	
	/** The Constant LIST_COMMAND. */
	public final static String LIST_COMMAND    = "list";
	
	public final static String HELP_COMMAND = "help";
	
	/** The command. */
	private static Command command;
	
	/** The table. */
	private static String  table;
	
	/** The cf. */
	private static String  cf;
	
	private static Admin admin;
	
	private static Connection connection;
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		parseArgs(args);
		initAdmin();
		if( admin == null){
			System.exit(-1);
		}
		
		executeCommand(command);
	}

	private static void initAdmin()
	{
		Configuration config = HBaseConfiguration.create();
		try 
		{
		  connection = ConnectionFactory.createConnection(config);
			admin = connection.getAdmin();
		} catch (MasterNotRunningException e) {
			LOG.error("", e);
		} catch (ZooKeeperConnectionException e) {
			LOG.error("", e);
		} catch (IOException e) {
			LOG.error("", e);
	  }
	}
	
	/**
	 * Execute command.
	 *
	 * @param cmd the cmd
	 */
	private static void executeCommand(Command cmd) {
		switch(cmd){
			case DISABLE: disableRowCache(table, cf); break;
			case ENABLE : enableRowCache(table, cf); break;
			case STATUS : statusTable(table); break;
			case LIST   : listTables(); break; 
		}
		
	}

    /**
     * Parses the args.
     *
     * @param args the args
     */
    private static void parseArgs(String[] args)
    {
        try{
            
            for(int i=0; i < args.length; i++)
            {   
                String arg= args[i];
                if(arg.equals(DISABLE_COMMAND)){
                    command = Command.DISABLE;                    
                    table = args[++i];
                    
                    if( i + 1 < args.length){
                    	cf = args[++i];
                    }
                    
                } else if(arg.equals(ENABLE_COMMAND)){
                    command = Command.ENABLE;
                    table = args[++i];
                    if( i + 1 < args.length){
                    	cf = args[++i];
                    
                    }
                } else if(arg.equals(LIST_COMMAND)){
                    command = Command.LIST;

                } else if(arg.equals(STATUS_COMMAND)){
                    command = Command.STATUS;                   
                    table = args[++i];                   
 
                } else if(arg.equals(HELP_COMMAND)){
                                      
                	usage();                   
 
                } else{
                    LOG.error("Unrecognized argument: "+arg);
                    System.exit(-1);
                }
            }               
            
        }catch(Exception e){
            LOG.error("Wrong input arguments", e);
            usage();
            System.exit(-1);
        }
    }
	
	/**
	 * Usage.
	 */
	private static void usage() {
		LOG.info("Usage\n"+ " rowcache.sh command [table_name] [colfamily]\ncommand - one of -list, -status, -disable, -enable, -help");		
	}

	/**
	 * List tables.
	 */
	private static void listTables() {
		
		try{
			LOG.info("LIST:\n");
			List<TableDescriptor> tables = admin.listTableDescriptors();
			for(TableDescriptor t: tables){
				LOG.info("{}", t.getTableName());
			}
		}catch(Exception e){
			LOG.error("", e);
		}
	}

	/**
	 * Status table.
	 *
	 * @param table2 the table2
	 * @throws IOException 
	 * @throws TableNotFoundException 
	 */
  private static void statusTable(String tableName)  {

		TableName tn = TableName.valueOf(tableName);
		try{
			TableDescriptor tableDesc = admin.getDescriptor(tn);
			byte[] v = tableDesc.getValue(RConstants.ROWCACHE);
			LOG.info("Table "+tableName+": ROWCACHE="+ ((v == null)?("false"):("true")));
			ColumnFamilyDescriptor[] cols = tableDesc.getColumnFamilies();
			for(ColumnFamilyDescriptor c: cols){
				v = c.getValue(RConstants.ROWCACHE);
				boolean status = (v == null)? false: "true".equals( new String(v));
				LOG.info("Family "+new String(c.getName())+": ROWCACHE=" + status);
			}
		}catch(Exception e){
			LOG.error("", e);
		}
		
	}

	/**
	 * Enable row cache.
	 *
	 * @param table2 the table2
	 * @param cf2 the cf2
	 */
	private static void enableRowCache(String tableName, String cf) {
		byte[] name = tableName.getBytes();
		TableName tn =TableName.valueOf(name);
		try{
			HTableDescriptor tableDesc = new HTableDescriptor(admin.getDescriptor(tn));
			if(cf == null){
				tableDesc.setValue(RConstants.ROWCACHE, "true".getBytes());
				admin.disableTable(tn);
				admin.modifyTable(tn, tableDesc);
				admin.enableTable(tn);
				LOG.info("Enabled row-cache for table: "+tableName);
			} else{
				byte[] cn = cf.getBytes();
				HColumnDescriptor c = tableDesc.getFamily(cn);
				if( c != null){
					c.setValue(RConstants.ROWCACHE, "true".getBytes());
					admin.disableTable(tn);
					admin.modifyColumn(tn, c);
					admin.enableTable(tn);
					LOG.info("Enabled row-cache for table: "+tableName+" : family="+ cf);
				} else{
					LOG.error("Family "+cf+" does not exists");
				}
			}

		}catch(Exception e){
			LOG.error("", e);
		}		
		
	}

	/**
	 * Disable row cache.
	 *
	 * @param t the t
	 * @param f the f
	 */
	private static void disableRowCache(String tableName, String cf) {
		TableName tn = TableName.valueOf(tableName);
		try{
			HTableDescriptor tableDesc = new HTableDescriptor(admin.getTableDescriptor(tn));
			if(cf == null){
				tableDesc.setValue(RConstants.ROWCACHE, "false".getBytes());
				admin.disableTable(tn);
				admin.modifyTable(tn, tableDesc);
				admin.enableTable(tn);
				LOG.info("Disabled row-cache for table: "+tableName);
			} else{
				byte[] cn = cf.getBytes();
				HColumnDescriptor c = tableDesc.getFamily(cn);
				if( c != null){
					c.setValue(RConstants.ROWCACHE, "false".getBytes());
					admin.disableTable(tn);
					admin.modifyColumn(tn, c);
					admin.enableTable(tn);
					LOG.info("Disabled row-cache for table: "+tableName+" : family="+ cf);
				} else{
					LOG.error("Family "+cf+" does not exists");
				}
			}

		}catch(Exception e){
			LOG.error("", e);
		}		
		
	}

}
