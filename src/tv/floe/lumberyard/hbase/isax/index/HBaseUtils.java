/**
   Copyright [2011] [Josh Patterson]

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

 */

package tv.floe.lumberyard.hbase.isax.index;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Set of Utils to make working with HBase specific operations easier.
 * 
 * TODO
 * 	-	Speed improvements, allow for caching of config?
 * 
 * 
 * @author jpatterson
 *
 */
public class HBaseUtils {



	public static void DeleteRow( String key, String TableName ) throws IOException {
		
		
		Configuration config = HBaseConfiguration.create();
		
		HTable hbase_table = new HTable(config, TableName);

		Delete d = new Delete( Bytes.toBytes(key) );
				
		hbase_table.delete(d);
		
		hbase_table.close();
		
		
	}
	
	public static void Put( String key, String TableName, String ColumnFamily, String ColumnName, byte[] value ) throws IOException {
		
		Configuration config = HBaseConfiguration.create();
		
		HTable hbase_table = new HTable(config, TableName);
		
		Put put1 = new Put( Bytes.toBytes(key));
		put1.add(Bytes.toBytes(ColumnFamily), Bytes.toBytes(ColumnName), value);
		
		hbase_table.put(put1);
		
		hbase_table.close();
		
	}
	
	public static byte[] Get( String key, String TableName, String ColumnFamily, String ColumnName ) throws IOException {
		
		Configuration config = HBaseConfiguration.create();
		
		HTable hbase_table = new HTable(config, TableName);
		Get get_cell = new Get( Bytes.toBytes(key));		
		
		Result result = hbase_table.get(get_cell);

		
		hbase_table.close();
		
		
		return result.value();
		
	}
	
	public static boolean CreateNewTable( String TableName) throws IOException {
		
		Configuration config = HBaseConfiguration.create();
		
//		HTable hbase_table = new HTable(config, TableName);
		//Get get_cell = new Get( Bytes.toBytes(key));		
		
		//Result result = hbase_table.get(get_cell);
		
		HBaseAdmin admin = new HBaseAdmin(config);

		HTableDescriptor table_desc = new HTableDescriptor( TableName );
		
		HColumnDescriptor col_family = new HColumnDescriptor( "node" );
		
		table_desc.addFamily(col_family);
		
		admin.createTable( table_desc );
		//hbase_table.close();
		
		
		//return result.value();
			

		return true;
	}

	
	
	public static boolean DoesTableExist ( String TableName) throws IOException {
		
		Configuration config = HBaseConfiguration.create();
		
		//HTable hbase_table = new HTable(config, TableName);
		//Get get_cell = new Get( Bytes.toBytes(key));		
		
		//Result result = hbase_table.get(get_cell);

		
		//hbase_table.close();
		
		HBaseAdmin admin = new HBaseAdmin(config);

		return admin.tableExists(TableName);
		
		
		//return result.value();
			

		//return false;
	}	
	
	public static boolean DropTable( String TableName ) throws IOException {
		
		Configuration config = HBaseConfiguration.create();
		
//		HTable hbase_table = new HTable(config, TableName);
		
//		hbase_table.close();
	
		HBaseAdmin admin = new HBaseAdmin(config);

		
		admin.disableTable(Bytes.toBytes(TableName));
		admin.deleteTable(TableName);

		return true;
		
		
	}
	
	public static void ListTables() {
		
		System.out.println( "List HBase Tables:" );
		
		Configuration config = HBaseConfiguration.create();
			
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if ( null == admin) {
			System.out.println( "Can't Access HBase, quitting..." );
		}
		
		try {
			HTableDescriptor[] tables = admin.listTables();
			
			for ( int x = 0; x < tables.length; x++ ) {
				
				System.out.println( "\tTable: " + tables[ x ].getNameAsString() );
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}
