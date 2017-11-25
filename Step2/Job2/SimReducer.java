package stubs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

  
public class SimReducer extends Reducer<Text, Text, Text, DoubleWritable> {

  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
	     //((itemid,itemid),(rating-avg,rating-avg))
	     double squareSum1 = 0.0;
	     double squareSum2 = 0.0;
	     double productSum = 0.0;

		for (Text value : values) {
			String str = value.toString();
			String[] avgs = str.split(",");
			double stats1 = Double.parseDouble(avgs[0]);
			//stats1 = ((int)(stats1*100))/100;
			double stats2 = Double.parseDouble(avgs[1]);
			//stats2 = ((int)(stats2*100))/100;
			squareSum1 += stats1 * stats1;
			squareSum2 += stats2 * stats2;
			productSum += stats1 * stats2;
			
		}
		
		double pearson = productSum / (Math.sqrt(squareSum1) * Math.sqrt(squareSum2));
		
		if(Double.isNaN(pearson)){
			pearson = 0;
		}
//		if(pearson == Double.NaN){
//			pearson = 0;
//		}
		
		if(pearson != 1 && pearson != -1){
			context.write(key, new DoubleWritable(pearson));
		}
	}
}
