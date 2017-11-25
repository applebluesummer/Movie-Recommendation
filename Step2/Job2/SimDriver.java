package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SimDriver extends Configured implements Tool{

  public static void main(String[] args) throws Exception {
	  int exitCode = ToolRunner.run(new Configuration(), new SimDriver(), args);
	  System.exit(exitCode); 
  }
  
  @Override
  public int run(String[] args) throws Exception {
	if (args.length != 2) {
	      System.out.printf(
	          "Usage: SimDriver <input dir> <output dir>\n");
	      System.exit(-1);
	    }
	
		Job job = new Job(getConf());
	    
	   
	    job.setJarByClass(SimDriver.class);
	    
	    job.setJobName("SimDriver");

	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	   
	    job.setMapperClass(SimMapper.class);
	    job.setReducerClass(SimReducer.class);
	    
	    //job.setInputFormatClass(KeyValueTextInputFormat.class);
	    
	    job.setMapOutputKeyClass(Text.class);  
	    job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    //job.setNumReduceTasks(3);
	    
	    

	    
	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	  }
}
