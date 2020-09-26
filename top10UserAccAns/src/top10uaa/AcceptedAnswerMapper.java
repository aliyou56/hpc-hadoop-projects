package top10uaa;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AcceptedAnswerMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    private IntWritable outKey = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, String> parsed = XmlUtils.getAttributesMap(value.toString());
        String strPostTypeId = parsed.get("PostTypeId");
        if(strPostTypeId != null && strPostTypeId.equals("1")) {
            String strAcceptedAnswerId = parsed.get("AcceptedAnswerId");
            if(strAcceptedAnswerId != null && !strAcceptedAnswerId.trim().isEmpty()) { // response to a comment
                outKey.set(Integer.parseInt(strAcceptedAnswerId));
                context.write(outKey, NullWritable.get());
            } else {
                System.out.println(" ** AcceptedAnswerMapper.map (no accAnnid) -> strAcceptedAnswerId="+strAcceptedAnswerId + " | strPostTypeId="+ strPostTypeId);
            }
        }
    }
}