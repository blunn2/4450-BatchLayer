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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*This class is responsible for running map reduce job*/
public class BatchLayerDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {

		if (args.length != 1) {
			System.err
					.println("Usage: View Creation One <input path>");
			System.exit(-1);
		}
		
		Configuration config = HBaseConfiguration.create();
		
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		
		/* TODO: set name for job */

		Job job = new Job(config, "View One");
		job.setJarByClass(BatchLayerDriver.class);
		
		//adds input path
		FileInputFormat.addInputPath(job, new Path(args[0]));

		//sets mapper values
		job.setMapperClass(BatchLayerMapper_KeyId.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//connects to Hbase table SensorValues
		TableMapReduceUtil.initTableReducerJob("SensorValues",
				BatchLayerReducer_HBaseSensorIdReads.class, job);

		job.setReducerClass(BatchLayerReducer_HBaseSensorIdReads.class);
		job.waitForCompletion(true);
		
		return 0;
	}
	
	//Entry point for the program
	public static void main(String[] args) throws Exception {
		BatchLayerDriver driver = new BatchLayerDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}