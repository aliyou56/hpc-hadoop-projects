package top10cr;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10ComResMapper extends Mapper<Text, Text, IntWritable, IntWritable> {

    //nombre de comment | user Id
    private TreeMap<Integer, IntWritable> treeMap = new TreeMap<>();

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        treeMap.put(Integer.parseInt(value.toString()), new IntWritable(Integer.parseInt(key.toString())));
        if(treeMap.size() > 10) {
            treeMap.remove(treeMap.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable outKey = new IntWritable();
        for(Integer key : treeMap.keySet()) {
            outKey.set(key);
            context.write(outKey, treeMap.get(key));
        }
    }
}