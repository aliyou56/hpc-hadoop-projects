package cbs;

// import java.nio.Path;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountNumUsersByStateDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count By State");
        job.setJarByClass(CountNumUsersByStateDriver.class);
        job.setMapperClass(CountNumUsersByStateMapper.class);

        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/data/stackoverflow/Users.xml"));
        Path output_path = new Path("output-cbs");
        FileOutputFormat.setOutputPath(job,output_path);

    
        int code = job.waitForCompletion(true) ? 0 : 1;
        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(CountNumUsersByStateMapper.STATE_COUNTER)) {
                System.out.println(counter.getDisplayName()+"\t"+ counter.getValue());
            }
        }
        // Clean up empty output directory
        // FileSystem.get(conf).delete(outputDir, true);
        FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf);
        hdfs.delete(output_path, true);
        System.exit(code);
    }
}
    

