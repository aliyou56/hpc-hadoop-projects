package minmaxcount;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCountReducer extends Reducer<IntWritable, MinMaxCountTuple, IntWritable, MinMaxCountTuple> {

    private MinMaxCountTuple result = new MinMaxCountTuple();

	public void reduce(IntWritable key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException { 
        result.setMin(null);
        result.setMax(null);
        result.setCount(0);
        int sum = 0;
        
        for (MinMaxCountTuple tuple : values) {
            if( result.getMin() == null || tuple.getMin().isBefore(result.getMin()) ) {
                result.setMin(tuple.getMin());
            }
            if( result.getMax() == null || tuple.getMax().isAfter(result.getMax()) ) {
                result.setMax(tuple.getMax());
            }
            // if (result.getMin() == null  || tuple.getMin().compareTo(result.getMin()) < 0) {
            //     result.setMin(tuple.getMin());
            // }
            // if (result.getMax() == null || tuple.getMax().compareTo(result.getMax()) > 0) {
            //     result.setMax(tuple.getMax());
            // }
            // Add to our sum the count for value
            sum += tuple.getCount();
	    }
        // Set our count to the number of input values
        result.setCount(sum);
        context.write(key, result);
    }
}