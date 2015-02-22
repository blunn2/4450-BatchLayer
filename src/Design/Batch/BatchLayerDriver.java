package Design.Batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*This class is responsible for running map reduce job*/
public class BatchLayerDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err
					.println("Usage: View Creation One <input path> <outputpath>");
			System.exit(-1);
		}
		
		Configuration config = HBaseConfiguration.create();
		
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		
		/* TODO: set name for job */

		Job job = new Job(config, "View One");
		job.setJarByClass(BatchLayerDriver.class);
		
		//Sets default input format. Mapper will read one line of input file at a time
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(BatchLayerMapper_KeyId.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		TableMapReduceUtil.initTableReducerJob("SensorValues",
				BatchLayerReducer_HBaseSensorIdReads.class, job);

		job.setReducerClass(BatchLayerReducer_HBaseSensorIdReads.class);
		job.waitForCompletion(true);
		
		job.setNumReduceTasks(1);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		BatchLayerDriver driver = new BatchLayerDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}