package Design.Batch;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class BatchLayerMapper_KeyId extends
Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//Data format: PK|ID|VALUE|TIMESTAMP
		
		String line = value.toString();
		
		//split string on |
		String[] lineValues = line.split("|");
		
		//save key
		String sensorId = lineValues[1];
		
		//save value
		String mapReduceValue = lineValues[3] + ":" + lineValues[2];
		
		//takes key as sensorId and value as the mapReduceValue created above
		if (sensorId != "" && mapReduceValue != "") {
			context.write(new Text(sensorId), new Text(mapReduceValue));
		}
	}
}
