package minmaxcount;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends Mapper<Object, Text, IntWritable, MinMaxCountTuple> { 

	// private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private IntWritable outUserId = new IntWritable();
    private MinMaxCountTuple outTuple = new MinMaxCountTuple();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Map<String, String> parsed = XmlUtils.getAttributesMap(value.toString());

        String strUserId = parsed.get("UserId");
        String strDate = parsed.get("CreationDate");
        if( strUserId != null && !strUserId.isEmpty() && strDate != null && !strDate.isEmpty()) {
            try {
                LocalDateTime creationDate = LocalDateTime.parse(parsed.get("CreationDate"));
                // Date creationDate = sdf.parse(strDate);
                outTuple.setMin(creationDate);
                outTuple.setMax(creationDate);
                outTuple.setCount(1);
                outUserId.set(Integer.parseInt(strUserId));
                context.write(outUserId, outTuple);
            // } catch (ParseException e) { System.err.println("Date error " + e.getMessage()); }
			} catch(DateTimeParseException dtpe) {
                System.err.println(dtpe.getMessage());
            }
        }

		// if((parsed.containsKey("CreationDate")) && (parsed.containsKey("UserId"))){
		// 	MinMaxCountTuple tuple = new MinMaxCountTuple();
		// 	try{

        //         LocalDateTime date = LocalDateTime.parse(parsed.get("CreationDate"));
		// 		// Date date = sdf.parse(parsed.get("CreationDate"));
		// 		tuple.setMin(date);
		// 		tuple.setMax(date);
		// 		tuple.setCount(1);
		// 	// } catch(ParseException e){}
        //     } catch(DateTimeParseException dtpe) {
        //         System.err.println(dtpe.getMessage());
        //         dtpe.printStackTrace(System.err);
        //     }

		// 	if((parsed.get("UserId") != null) && (!parsed.get("UserId").isEmpty())){
		// 		context.write(new IntWritable(Integer.parseInt(parsed.get("UserId"))), tuple);
		// 	}
		// }
	}  
}