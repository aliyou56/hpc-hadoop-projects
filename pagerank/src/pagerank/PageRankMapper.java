package pagerank;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, TupleWritable, Text, TupleWritable> {

    public static enum MapCounters {
        PAGE_CNT,
    }

    private Text outKey = new Text();
    LinkedList<String> outList = new LinkedList<>(); 

    @Override
    public void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
        // key: pageTitle  |  value: <rank>, <linkToPages>
        outList.clear();
        outList.addAll(Arrays.asList(value.get()));
        outList.set(0, "###");
        context.write(key, new TupleWritable(outList)); // value: <###>, <linkToPages>

        String fromPage = key.toString();
        String rank = value.get(0);
        int totalLinks = value.size() - 1; // the first element is the rank

        if(totalLinks > 0) {
            for(int i=1; i<value.size(); i++) { // iteration begins from the first linkname
                outKey.set(value.get(i));
                context.write(outKey, new TupleWritable(fromPage, rank, String.valueOf(totalLinks)));
            }
        }

        context.getCounter(MapCounters.PAGE_CNT).increment(1);
    }
}
