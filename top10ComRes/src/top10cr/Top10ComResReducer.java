package top10cr;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10ComResReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    // id | nombre de comment 
    private TreeMap<Integer, IntWritable> treeMap = new TreeMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
        for(IntWritable value : values) {
            treeMap.put(key.get(), new IntWritable(value.get())); //new IntWritable(value)
            if(treeMap.size() > 10) {
                treeMap.remove(treeMap.firstKey());
            }
        }
    } 

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable outKey = new IntWritable();
        for(Integer key : treeMap.descendingKeySet()) {
            outKey.set(key);
            context.write(treeMap.get(key), outKey);
        }
    }
}