package top10uaa;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UserAccAnsReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        IntWritable outValue = new IntWritable();
        int count = 0;
        for(IntWritable val : values) {
            count += val.get();
        }
        outValue.set(count);
        context.write(key, outValue);
    }
}