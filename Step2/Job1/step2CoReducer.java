package stubs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class step2CoReducer extends Reducer<Text, Text, Text, Text> {
	

  /*
   * Reducer input: (userId, [(itemId, stats), ...])
   * Reducer output: ((itemId, itemId), (rating-avgRating, rating-avgRating))
   */
  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
	  
	  // use a TreeMap to store each (itemId, stats) pair
	  List<String[]> list = new ArrayList<String[]>();
	  for(Text value : values){
		  String[] itemValues = value.toString().split(",");
		  
		  String fullRating = itemValues[1] + "," + itemValues[2];
		  String[] result = new String[2];
		  
		  result[0] = itemValues[0]; //itemId
		  result[1] = fullRating; // rating,avgRating
		  
		  list.add(result);
	  }

	  for(int i = 0; i < list.size()-1; i++){
		  for(int j = i+1; j < list.size(); j++){
			  String[] itemOne = list.get(i);
			  String[] itemTwo = list.get(j);
			  
			  String fullKey = "" + itemOne[0] + "," + itemTwo[0];
			  String fullValue = "" + itemOne[1] + "," + itemTwo[1];
			  //emit
			  context.write(new Text(fullKey), new Text(fullValue));
		  }
	  }
	}
}