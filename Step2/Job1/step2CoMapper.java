package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class step2CoMapper extends Mapper<LongWritable, Text, Text, Text> {
	/*
	 * This job get the (itemid, userid, rating), (itemid	avgRating) from pre-processed files,
	 * and use JOIN operation to emit (userid, (itemid, stats)) pairs
	 */

	private Configuration conf;
	private Map<String, String> avgRatingLookup;
	// using reusable objects
	private Text key;
	private Text value;

	@Override
	public void setup(Context context){
		// initialization
		this.conf = context.getConfiguration();
		avgRatingLookup = new HashMap<String, String>();
		key = new Text();
		value = new Text();
		
		// read <itemid, avgRating> from file and store as lookup table
		try{
			String fileName = "part-r-00000"; // change whenever possible
			File avgRatingFile = new File(fileName); // load from local directory
			BufferedReader reader = new BufferedReader(new FileReader(avgRatingFile));
			String str;
			while((str = reader.readLine())!= null){
				String[] items = str.split("\\W+");
				avgRatingLookup.put(items[0], items[1]);
			}
			reader.close();
		}catch(IOException e){
			throw new RuntimeException("Illegal file reading " + e.getMessage());
		}	
	}
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  // value is (userid, itemid, rating)
	  String line = value.toString();
	  if( line.length() > 0){
		  String[] values = line.split(",");
		  
		  String userId = values[1];
		  String itemId = values[0];
		  String rating = values[2];
		  
		  // user StringBuilder to append (itemId, stats)
		  StringBuilder valueResult = new StringBuilder();
		  String avgRating = avgRatingLookup.get(itemId);
		  // append all results into one StringBuildr
		  valueResult.append(itemId + ",");
		  valueResult.append(rating + ",");
		  valueResult.append(avgRating);
		  
		  
		  this.key.set(userId);
		  this.value.set(valueResult.toString());

		  context.write(this.key, this.value);    
	  }
	  
  }
}
