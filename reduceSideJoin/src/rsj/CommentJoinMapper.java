package rsj;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {
    
    private Text outkey = new Text();
    private Text outvalue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String, String> parsed= XmlUtils.getAttributesMap(value.toString());
        String userId = parsed.get("UserId");
        if (userId == null) return;
        // The foreign join key is the user ID
        outkey.set(userId);
        // Flag this record for the reducer and then output
        outvalue.set("B" + value.toString());
        context.write(outkey, outvalue);

    }
}