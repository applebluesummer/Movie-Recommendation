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

public class TopNReducer extends Reducer<Text, ItemWritable, Text, Text> {
	
	int n;
	Set<String> set;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		n = conf.getInt("knumber", 1500);
		set = new HashSet<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File("TestingRatings.txt")));
			String s;
			while ((s = br.readLine()) != null) {
				  String[] words = s.split("\\W+");
				  set.add(words[0]);
//				  System.out.println(words[1]);
			  }
			  br.close();
		  } catch (Exception e) {
			  System.out.println("Failed to read files: " + e.toString());
			  return;
		  }
	}
	
	@Override
	public void reduce(Text key, Iterable<ItemWritable> values, Context context)
			throws IOException, InterruptedException {
		if (!set.contains(key.toString())) return;
		StringBuilder sb = new StringBuilder();
		PriorityQueue<ItemWritable> pq = new PriorityQueue<ItemWritable>(n, new Comparator<ItemWritable>() {
			public int compare(ItemWritable a, ItemWritable b) { return Double.compare(a.getRate(), b.getRate()); }
		});
		try {
			for (ItemWritable value : values) {
				pq.add(new ItemWritable(value.id, value.value));
				if (pq.size() > n) pq.poll();
			}
			Stack<ItemWritable> stack = new Stack<>();
			while (!pq.isEmpty()) stack.push(pq.poll());
			while (!stack.isEmpty()) {
				ItemWritable item = stack.pop();
				sb.append(item.id + "," + item.value);
				sb.append("\t");
			}
			context.write(key, new Text(sb.toString()));
		  } catch (Exception e) {
//			  System.out.println("Failed to read files: " + e.toString());
			  return;
		  }
	}
}
