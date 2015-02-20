package Design.Batch;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BatchLayerReducer_HBaseSensorIdReads
extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context)
					throws IOException, InterruptedException {

		//create table
		String tableName = "SensorView";
		HBaseService.CreateTable(tableName);

		//iterate through values and write to table
		for (Text value : values) {
			//Value format: Timestamp:Value. Value is sensor reading
			String [] valueArray = value.toString().split(":");
			
			//rowKey is sensorId plus timestamp. 
			//Ensures that records are sorted by timestamp and are next to each other
			String rowKey = key.toString() + "-" + valueArray[1];
			
			//column family static from HBaseService.CreateTable
			String columnFamily = "d";
			
			//dynamically created on data input. set to optimize memory so saving keystrokes on name.
			String columnQualifier = "val";
			
			//cell value is the reading from the sensor
			String sensorReading = valueArray[1];
			
			boolean dataAdded = HBaseService.AddData(tableName, rowKey, columnFamily, columnQualifier, sensorReading);
			
			if(!dataAdded)
				System.out.println("Data didn't get added");
			else
				System.out.println("Data outputted to HBase table: " + tableName);
		}
		//outputs the key which is year and the max value
		context.write(key, new Text("Got to end"));
	}
}