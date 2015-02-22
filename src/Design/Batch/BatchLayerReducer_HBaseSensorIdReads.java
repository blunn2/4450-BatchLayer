package Design.Batch;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class BatchLayerReducer_HBaseSensorIdReads extends
		TableReducer<Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// iterate through values and write to table
		for (Text value : values) {
			// Value format: Timestamp|Value. Value is sensor reading
			String[] valueArray = value.toString().split("\\|");

			// rowKey is sensorId plus timestamp.
			// Ensures that records are sorted by sensor Id first then timestamp
			String rowKey = key.toString() + "-" + valueArray[0];
			
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes("d"), Bytes.toBytes("val"),
					Bytes.toBytes(valueArray[1]));

			context.write(new Text(rowKey), put);
		}
	}
}