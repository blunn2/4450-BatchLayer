package Design.Batch;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class BatchLayerReducer_HBaseSensorIdReads
extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context)
					throws IOException, InterruptedException {

		//we have a key and bunch of temperature values
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		//outputs the key which is year and the max value
		context.write(key, new IntWritable(maxValue));
	}
}