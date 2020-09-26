package top100;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class Top100Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

  private TreeMap<Integer, Text> treeMap = new TreeMap<>();

  @Override
  protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
    for(Text value : values) {
      treeMap.put(key.get(), new Text(value));
      if(treeMap.size() > 100) {
        treeMap.remove(treeMap.firstKey());
      }
    }
  } 

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    IntWritable outKey = new IntWritable();
    for(Integer key : treeMap.descendingKeySet()) {
      outKey.set(key);
      context.write(outKey, treeMap.get(key));
    }
  }
  
}