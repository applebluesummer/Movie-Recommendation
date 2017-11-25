package stubs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Prediction extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: TopNList <input dir> <output dir>\n");
      return -1;
    }
    
    // job 1 setting up
    Job job1 = new Job(getConf());
    job1.setJarByClass(Prediction.class);
    job1.setJobName("weighted average for prediction");

    FileInputFormat.setInputPaths(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    
    job1.setMapperClass(PredictionMapper.class);
    job1.setReducerClass(PredictionReducer.class);
    
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(DoubleWritable.class);
    
    boolean success1 = job1.waitForCompletion(true);
    return success1 ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new Prediction(), args);
    System.exit(exitCode);
  }
}
