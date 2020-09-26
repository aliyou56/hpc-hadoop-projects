package pagerank;

import static pagerank.PageRankMR.D_FACTOR;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, TupleWritable, Text, TupleWritable> {

    public static enum ReduceCounters {
        PAGE_RANK,
    }

    LinkedList outList = new LinkedList<>();

    @Override
    protected void reduce(Text key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {
        
        outList.clear();
        double rankByOuts = 0.0; // sum of ranks of pages pointing to P by their number of output links
        
        for(TupleWritable tuple : values) { 
            if( "###".equals(tuple.get(0)) ) { // tuple ->  <###>, <LinksToPage>
                outList.addAll(Arrays.asList(tuple.get()));
            } else {  // tuple -> <fromPage>, <rank>, <totalLinks>
                try {
                    double rank = Double.parseDouble(tuple.get(1));
                    int totalLinks = Integer.parseInt(tuple.get(2)); // allways > 0
                    rankByOuts += rank/totalLinks;
                } catch(NumberFormatException e){ System.err.print(e); }
            }
        }

        if(!outList.isEmpty()) {
            double newRank = (1-D_FACTOR) + (D_FACTOR*rankByOuts); // compute the newRank
            outList.set(0, String.valueOf(newRank)); // replace the first elt (###) with the new rank
            context.write(key, new TupleWritable(outList)); // writing back
            context.getCounter(ReduceCounters.PAGE_RANK).increment((long) (newRank*1000));
        } 

    }
}
