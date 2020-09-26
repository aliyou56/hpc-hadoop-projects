package average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageCombiner extends Reducer<IntWritable, AverageTuple, IntWritable, AverageTuple> {

    @Override
    protected void reduce(IntWritable key, Iterable<AverageTuple> values, Context context) throws IOException, InterruptedException {
        AverageTuple outTuple = new AverageTuple();
        for(AverageTuple tuple : values) {
            outTuple.add(tuple);
        }
        context.write(key, outTuple);
    }
}