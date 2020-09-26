package top100;

import java.io.IOException;
import java.util.TreeMap;
 
import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top100Mapper extends Mapper<Text, Text, IntWritable, Text> { 
  
  private TreeMap<Integer, Text> treeMap = new TreeMap<>();
  
  @Override
  public void map(Text key, Text value, Context context) throws IOException, InterruptedException { 
    treeMap.put(Integer.parseInt(value.toString()), new Text(key));
    if(treeMap.size() > 100) {
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