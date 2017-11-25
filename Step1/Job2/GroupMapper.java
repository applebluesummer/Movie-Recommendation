package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GroupMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		
		  String line = value.toString();
		  String[] words = line.split("\\W+");
		  try {
			  context.write(new Text(words[1]), new Text(words[0] + "," + words[2]));
		  } catch (Exception e) {
			  return;
		  }
	  }
}
