package stubs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.PriorityQueue;
import java.util.Comparator;

public class TopNMapper extends Mapper<LongWritable, Text, Text, ItemWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		// input requirement: itemid,itemid \t similarity
		  String line = value.toString();
		  String[] words = line.split("\t");
		  String[] items = words[0].split("\\W+");
		  if (items[0].equals(items[1])) return;
		  if (words[1].equals("NaN")) {
//			  System.out.println("NaN encountered");
			  return;
		  }
		  double simi = Double.parseDouble(words[1]);
		  if (Math.abs(simi - 1) < 1e-8) {
//			  System.out.println("1 encountered");
			  return;
		  }
		  try {
			  context.write(new Text(items[0]), new ItemWritable(items[1], simi));
			  context.write(new Text(items[1]), new ItemWritable(items[0], simi));
		  } catch (Exception e) {
			  return;
		  }
	  }
}
