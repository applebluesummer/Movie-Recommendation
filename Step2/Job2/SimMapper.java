package stubs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimMapper extends Mapper<LongWritable, Text, Text, Text>{
	


  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	//((itemid,itemid),(rating,avg,rating,avg)) -> ((itemid,itemid),(rating-avg,rating-avg))
	    String line = value.toString();
	    String[] pair = line.split("\t");
	    String itemPair = pair[0];
	    String avgPair = pair[1];
	    String[] values = avgPair.split(",");
	    double rating1 = Double.parseDouble(values[0]);
	    double avg1 = Double.parseDouble(values[1]);
	    double rating2 = Double.parseDouble(values[2]);
	    double avg2 = Double.parseDouble(values[3]);
	    double v1 = rating1 - avg1;
	    double v2 = rating2 - avg2;
	    String output = v1 + "," + v2;

	    context.write(new Text(itemPair), new Text(output));
    
    
 
    

  }
}