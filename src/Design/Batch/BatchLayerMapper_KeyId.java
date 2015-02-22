package Design.Batch;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BatchLayerMapper_KeyId extends
		Mapper<LongWritable, Text, Text, Text> {
	@Override
	// Takes in input data and maps it to a key/value pair
	// Data format: PK|SensorID|VALUE|TIMESTAMP
	// Output: key is SensorID and value is TIMESTAMP:VALUE
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		
		// split string on |
		String[] lineValues = line.split("\\|");
		
		// save key
		String sensorId = lineValues[1];

		// save value. value is TIMESTAMP:VALUE
		String mapValue = lineValues[3] + "|" + lineValues[2];
		
		// takes key as sensorId and value as the mapReduceValue created above
		if (sensorId != "" && mapValue != "") {
			context.write(new Text(sensorId), new Text(mapValue));
		}
	}
}
