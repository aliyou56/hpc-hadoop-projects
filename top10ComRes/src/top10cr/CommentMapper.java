package top10cr;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable outKey = new IntWritable();
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, String> parsed = XmlUtils.getAttributesMap(value.toString());
        String strUserId = parsed.get("UserId");
        if(strUserId != null) {
            outKey.set(Integer.parseInt(strUserId));
            context.write(outKey, ONE);
        } else {
            System.out.println(" CommentMapper.map (null)-> strUserId="+ strUserId);
        }
    }
}