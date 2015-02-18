package Design.Batch;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.MasterNotRunningException;
import org.apache.hadoop.hbase.exceptions.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseService {
	
	public static boolean CreateTable(String tableName){
		try {
			// Instantiating configuration class
			Configuration conf = HBaseConfiguration.create();
			
			// Instantiating HBaseAdmin class
			HBaseAdmin admin = new HBaseAdmin(conf);
			
			// Verifying the table doesn't exist
			boolean tableExists = admin.tableExists(tableName);

			if(tableExists)
			{
				admin.close();
				return true;
			}
			else
			{
					// Instantiating table descriptor class
					HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

					// Adding column families to table descriptor
					//To optimize performance, keeping names as small as possible
					tableDescriptor.addFamily(new HColumnDescriptor("d"));

					// Execute the table through admin
					admin.createTable(tableDescriptor);
			}
			admin.close();
			
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	//Creating a table for sensor readings. each row is a sensor reading for a given sensor id and timestamp
	//Rowkey is sensorId-Timestamp to ensure that all data for one sensor is close by to optimize scans
	//first record for id will be oldest timestamp based on HBase sorting
	public static boolean AddData(String tableName, String rowKey, String columnFamily, String columnQualifier, String value)
	{
		try {
			// Instantiating Configuration class
			  Configuration config = HBaseConfiguration.create();

			  // Instantiating HTable class
			  HTable hTable = new HTable(config, tableName);

			  // Instantiating Put class
			  // accepts a row name.
			  Put p = new Put(Bytes.toBytes(rowKey)); 

			  // adding values using add() method
			  // accepts column family name, qualifier/row name ,value
			  p.add(Bytes.toBytes(columnFamily),
			  Bytes.toBytes(columnQualifier),Bytes.toBytes(value));
			  
			  // Saving the put Instance to the HTable.
			  hTable.put(p);
			  System.out.println("data inserted");
			  
			  // closing HTable
			  hTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	      return true;
	} 
}