package cbs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountNumUsersByStateMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
    
    public static final String STATE_COUNTER = "State";
    private String[] statesArray = new String[] {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
        "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
        "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
        "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
        "WI", "WY" 
    };
    
    private HashSet<String> states;

    protected void setup(Context context) throws IOException, InterruptedException{
        states = new HashSet<String>(Arrays.asList(statesArray));
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, String> parsed= XmlUtils.getAttributesMap(value.toString());
        // Get the value for the Location attribute
        String location = parsed.get("Location");
        if (location != null && !location.isEmpty()) {
            boolean unknown = true;
            // Make location uppercase and split on white space
            String[] tokens = location.toUpperCase().split("\\s");
            for (String state : tokens) { // For each token check if it is a state
                if (states.contains(state)) {// If so, increment the counter
                    context.getCounter(STATE_COUNTER, state).increment(1);
                    unknown = false;
                    break;
                }
            }
        // If the state is unknown, increment the counter
            if (unknown) {
                context.getCounter(STATE_COUNTER, "Unknown").increment(1);
            }
        } else {// If it is empty or null, increment the counter by 1
            context.getCounter(STATE_COUNTER, "NullOrEmpty").increment(1);
        }
    }
}