package average;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<IntWritable, AverageTuple, IntWritable, AverageTuple /*Text*/> {
    
    // // AverageTuple outTuple = new AverageTuple();
    Text outText = new Text();

    public static HashMap<Integer, AverageTuple> result = new HashMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<AverageTuple> values, Context context) throws IOException, InterruptedException {
        AverageTuple outTuple = new AverageTuple();
        // int sum = 0;
        // int count = 0;
        for(AverageTuple tuple : values) {
            outTuple.add(tuple);
            // sum += tuple.getLength();
            // count += tuple.getCount();
        }
        // result.put(key.get(), outTuple);
        // outText.set(outTuple.toString());
        context.write(key, outTuple);
        // outText.set("count: "+ count +"\t avg: "+ sum/count);
        // outTuple.setCount(count);
        // outTuple.setLength(sum);
        // outTuple.setAvg(sum/count);
    }

    // public void cleanup(Context context) throws IOException, InterruptedException {
    //     for(Integer key : result.keySet()) {
    //         System.out.println(key + " " + result.get(key));
    //         // context.write(new IntWritable(key), result.get(key));
    //     }
    // }


}