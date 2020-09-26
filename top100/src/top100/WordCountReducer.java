package top100;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable out = new IntWritable();

    // public static enum ReduceCounters {
    //     INDEXED_WORDS, // nombre de mots qui ont été indexés
    // };

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException{ 
        
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        out.set(sum);
        // context.getCounter(ReduceCounters.INDEXED_WORDS).increment(1);
        context.write(key, out);
    }
}