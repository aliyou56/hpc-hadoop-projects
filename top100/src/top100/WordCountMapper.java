package top100;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
  
  private static final IntWritable ONE = new IntWritable(1);
  private static final HashSet<String> STOP_WORDS_SET = new HashSet<>();
  private Text outText = new Text();

  // public static enum MapCounters {
  //   ACCEPT_CNT,
  //   REJECT_CNT,
  // };

  @Override
  public void setup(Context context) throws IOException {
    if( context.getCacheFiles() != null && context.getCacheFiles().length > 0 ) {
      try(BufferedReader br = new BufferedReader(new FileReader("stopWords"))) {
        String line;
        while( (line = br.readLine()) != null) {
          STOP_WORDS_SET.addAll(Arrays.asList(line.split("\\s")));
        }
      }
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException { 
    StringTokenizer st = new StringTokenizer(value.toString());
    while(st.hasMoreTokens()) {
      String word = st.nextToken();
      if(!STOP_WORDS_SET.contains(word)) {
        outText.set(word);
        context.write(outText, ONE);
        // context.getCounter(MapCounters.ACCEPT_CNT).increment(1);
      } //else {
      //   context.getCounter(MapCounters.REJECT_CNT).increment(1);
      // }
    }
  }

}