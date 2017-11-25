package stubs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;

public class PredictionReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		double simiSum = 0;
		try {
			for (Text value : values) {
				String[] pair = value.toString().split(",");
				double simi = Double.parseDouble(pair[0]);
				double rating = Double.parseDouble(pair[1]);
				if (simi * rating < 1e-8) continue;
				simiSum += simi;
				sum += simi * rating;
			}
//			System.out.println("user id and itemid are:" + key.toString());
//			System.out.println("sum is: " + sum + ", simiSum is: " + simiSum);
			context.write(key, new DoubleWritable(sum / simiSum));
		  } catch (Exception e) {
//			  System.out.println("Failed to read files: " + e.toString());
			  return;
		  }
	}
}
