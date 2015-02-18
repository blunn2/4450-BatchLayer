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
		HBaseService.CreateTable("SensorView");

		//iterate through values and write to table
		for (Text value : values) {
			
		}
		//outputs the key which is year and the max value
		context.write(key, new Text());
	}
}