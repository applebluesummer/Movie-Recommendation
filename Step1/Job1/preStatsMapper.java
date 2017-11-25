package stubs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class preStatsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	// use reusable objects
	Text itemId = new Text();
	DoubleWritable rating = new DoubleWritable();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  String[] info = line.split(",");
	  itemId = new Text(info[0]);
	  
	  rating  = new DoubleWritable(Double.parseDouble(info[2]));
	  context.write(itemId, rating);
  }
}