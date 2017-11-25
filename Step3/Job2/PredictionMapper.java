package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.*;

public class PredictionMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Map<String, ArrayList<String>> goals;
	Map<String, ArrayList<String>> simis;
	
	@Override
	public void setup(Context context) {
		goals = new HashMap<>();
		simis = new HashMap<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File("TestingRatings.txt")));
			String s;
			while ((s = br.readLine()) != null) {
				  String[] words = s.split("\\W+");
				  if (goals.get(words[1]) == null) goals.put(words[1], new ArrayList<String>());
				  goals.get(words[1]).add(words[0]);
//				  System.out.println("size of goal map is: " + goals.size());
			  }
			  br.close();
			  br = new BufferedReader(new FileReader(new File("part-r-00000")));
			  while ((s = br.readLine()) != null) {
				  String[] words = s.split("\t");
//				  System.out.println(words[0]);
				  if (!simis.containsKey(words[0])) simis.put(words[0], new ArrayList<String>());
				  ArrayList<String> cur = simis.get(words[0]);
				  for (int i = 1; i < words.length; i++) {
					  if (words[i] != "") cur.add(words[i]);
//					  System.out.println(words[0] + "," + words[i]);
				  }
			  }
			  br.close();
		  } catch (Exception e) {
			  System.out.println("Failed to read files: " + e.toString());
			  return;
		  }
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		// input requirement: userId \t [(itemId, rating), \t ...]
		  String line = value.toString();
		  String[] words = line.split("\t");
		  String userId = words[0];
		  ArrayList<String> itemGoals = goals.get(userId);
		  if (itemGoals == null) return;
//		  System.out.println("goals map contains:" + userId);
		  Map<String, String> orgRating = new HashMap<>(); 
		  for (int i = 1; i < words.length; i++) {
			  String[] pair = words[i].split(",");
			  orgRating.put(pair[0], pair[1]);
		  }
		  for (String item : itemGoals) {
			  if (!simis.containsKey(item)) {
				  context.write(new Text(item + "," + userId), new Text("0,0"));
//				  System.out.println(item + "doesn't have simi, emitting 0.0");
				  continue;
			  }
			  ArrayList<String> topK = simis.get(item);
			  if (topK.size() == 0) {
				  context.write(new Text(item + "," + userId), new Text("0,0"));
//				  System.out.println(item + " doesn't have simi, emitting 0.0");
				  continue;
			  }
//			  System.out.println("simis contains itemId: " + item + ", size of topK is: " + topK.size());
			  for (String simi : topK) {
				  String[] pair = simi.split(",");
				  String rating;
				  if ((rating = orgRating.get(pair[0])) == null) {
					  context.write(new Text(item + "," + userId), new Text("0,0"));
					  continue;
				  }
				  try {
					  context.write(new Text(item + "," + userId), new Text(pair[1] + "," + rating));
//					  System.out.println(item + "," + userId + "," + pair[1] + "," + rating +" emitted.");
				  } catch (Exception e) {
					  return;
				  }
			  }
		  }
		  
		  
	  }
}
