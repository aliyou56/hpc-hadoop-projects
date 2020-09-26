package pagerank;

import java.io.IOException;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExtractLinksMapper extends Mapper<Text, Text, Text, TupleWritable> {
    
    static final Pattern PATTERN_INTERNAL_LINK = Pattern.compile("(\\[{2})([^\\]]+)(\\]{2})"); 

    public static enum MapCounters {
        PAGE_CNT,
        LINK_CNT,
    }

    private LinkedList<String> outList;

    public void setup(Context context) {
	    outList = new LinkedList<>();
    }
	
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        outList.clear();
        outList.add("1.0"); // rank

        Matcher matcher = PATTERN_INTERNAL_LINK.matcher(value.toString());
        while (matcher.find()) {
            String linkname = matcher.group(2);
	        linkname = (linkname.contains("|")) ? linkname.substring(0, linkname.indexOf("|")) : linkname;
            if(!linkname.trim().isEmpty()) {
                outList.add(linkname);
                context.getCounter(MapCounters.LINK_CNT).increment(1);
            }
        }
        if(outList.size() > 1) {
            context.write(key, new TupleWritable(outList));
        }
        context.getCounter(MapCounters.PAGE_CNT).increment(1);
     
    }

}
