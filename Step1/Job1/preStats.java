package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class preStats extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
	  int exitCode = ToolRunner.run(new Configuration(), new preStats(), args);
	  System.exit(exitCode);  
  }

  @Override
  public int run(String[] args) throws Exception {
	  
	  if (args.length != 2) {
		  System.out.printf("Usage: preStats <input dir> <output dir>\n");
	      System.exit(-1);
	  }

	  Job job = new Job(getConf());
	  job.setJarByClass(preStats.class);
	  job.setJobName("Step 1 Job 1 Calculate Stats");
	  
	  FileInputFormat.setInputPaths(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  
	  job.setMapperClass(preStatsMapper.class);
	  job.setReducerClass(preStatsReducer.class);
	  job.setMapOutputKeyClass(Text.class);  
	  job.setMapOutputValueClass(DoubleWritable.class);
	  
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(DoubleWritable.class);
	    
	  boolean success = job.waitForCompletion(true);
	  return success ? 0 : 1;
	}
}
