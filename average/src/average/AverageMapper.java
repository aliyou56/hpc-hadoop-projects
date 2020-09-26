package average;

import java.io.IOException;
import java.util.Map;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;

public class AverageMapper extends Mapper<LongWritable, Text, IntWritable, AverageTuple> {
    
    // public static final Log LOG = LogFactory.getLog(AverageMapper.class);

    AverageTuple outTuple = new AverageTuple();
    IntWritable outHour = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Map<String, String> parsed = XmlUtils.getAttributesMap(value.toString());
        String strCreationDate = parsed.get("CreationDate");
        String commentBody = parsed.get("Text");
        if(commentBody != null && strCreationDate != null && !strCreationDate.trim().isEmpty()) {
            try {
                LocalDateTime creationDate = LocalDateTime.parse(strCreationDate);
                outHour.set(creationDate.getHour());
                outTuple.setCount(1);
                outTuple.setLength(commentBody.length());
                context.write(outHour, outTuple);
            } catch(DateTimeParseException ex) {
                System.err.println("Error when parsing creationDate: " + ex.getMessage());
            }
        } else { // just for debugging reason 
            System.out.println(" *** Incoherence : Text='"+commentBody+"' | creationDate='"+strCreationDate+"' | lineNumber -> " + key.get());
        }
    }
}