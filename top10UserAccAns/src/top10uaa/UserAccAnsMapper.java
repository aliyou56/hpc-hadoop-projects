package top10uaa;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserAccAnsMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable outKey = new IntWritable();
    private static final IntWritable ONE = new IntWritable(1);
    private Set<Integer> acceptedAnswerIds;

    @Override
    public void setup(Context context) throws IOException {
        acceptedAnswerIds = new HashSet<>();
        if( context.getCacheFiles() != null && context.getCacheFiles().length > 0 ) {
            try(BufferedReader br = new BufferedReader(new FileReader("acceptedAnswerId"))) {
                String line;
                while( (line = br.readLine()) != null) {
                    try {
                        acceptedAnswerIds.add(Integer.parseInt(line.split("\\s")[0]));
                    } catch(NumberFormatException ex) {
                        System.err.println(" Error while converting a line ("+line+") -> " + ex.getMessage());
                    }
                }
            }
        }
        // System.out.println(" ! set size=" + acceptedAnswerIds.size());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, String> parsed = XmlUtils.getAttributesMap(value.toString());
        String strPostId = parsed.get("Id");
        String strOwnerUserId = parsed.get("OwnerUserId");
        String strPostTypeId = parsed.get("PostTypeId");
        if(strPostId != null && strOwnerUserId != null && strPostTypeId != null && strPostTypeId.equals("2")) {
            if(acceptedAnswerIds.contains(Integer.parseInt(strPostId))) {
                outKey.set(Integer.parseInt(strOwnerUserId));
                context.write(outKey, ONE);
            } 
        }
    }
}