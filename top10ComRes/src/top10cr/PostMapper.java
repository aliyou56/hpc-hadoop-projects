package top10cr;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PostMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable outKey = new IntWritable();
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, String> parsed = XmlUtils.getAttributesMap(value.toString());
        String ownerUserId = parsed.get("OwnerUserId");
        String postTypeId = parsed.get("PostTypeId");
        if(ownerUserId != null && postTypeId != null) {
            if(postTypeId.equals("2")) { // response to a comment
                outKey.set(Integer.parseInt(ownerUserId));
                context.write(outKey, ONE);
            } 
        } else {
             System.out.println(" PostMapper.map (null)-> postTypeId=" +postTypeId + " | ownerUserID="+ ownerUserId);
        }
    }
}