package stubs;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

  
public class preStatsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException {
		double sum = 0.0;
		int count = 0;
		
		for (DoubleWritable value : values) {
			count += 1;
			sum += value.get();
		}
		
		/*
		 *   Call the write method on the Context object to emit a key
		 *   and a value from the reduce method. 
		 */
		context.write(key, new DoubleWritable(sum/count));
	}
}