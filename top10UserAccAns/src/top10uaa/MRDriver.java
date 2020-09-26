package top10uaa;

// import java.io.IOException;
import java.net.URI;
// import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MRDriver(), args);  
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {

        // Configuration conf0 = this.getConf();
        // Job job0 = Job.getInstance(conf0, "getAcceptedAnswerId Job");
        // job0.setJarByClass(MRDriver.class);

        // // Setup MapReduce job
        // job0.setMapperClass(AcceptedAnswerMapper.class);
        // job0.setReducerClass(AcceptedAnswerReducer.class);
 
        // // Specify key / value
        // job0.setOutputKeyClass(IntWritable.class);
        // job0.setOutputValueClass(NullWritable.class);
 
        // // Input
        // FileInputFormat.addInputPath(job0, new Path("/data/stackoverflow/Posts.xml"));
        // job0.setInputFormatClass(TextInputFormat.class);
 
        // // Output
        // FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf0);
        // Path job0_output_path = new Path("output-aai");
        // FileOutputFormat.setOutputPath(job0, job0_output_path);
        // hdfs.delete(job0_output_path, true); // supprime le répertoire de sortie
 
        // /*return*/ job0.waitForCompletion(true)/* ? 0 : 1*/;

        Configuration conf1 = this.getConf();
        Job job1 = Job.getInstance(conf1, "UserAcceptedAnswer Job");
        job1.setJarByClass(MRDriver.class);

        // Setup MapReduce job
        job1.setMapperClass(UserAccAnsMapper.class);
        job1.setReducerClass(UserAccAnsReducer.class);
        // job.setCombinerClass(Top100Reducer.class);
 
        // Specify key / value
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
 
        job1.addCacheFile(new URI("output-aai/part-r-00000" + "#acceptedAnswerId"));
        // Input
        FileInputFormat.addInputPath(job1, new Path("/data/stackoverflow/Posts.xml"));
        job1.setInputFormatClass(TextInputFormat.class);
 
        // Output
        FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf1);
        Path job1_output_path = new Path("output-uaa");
        FileOutputFormat.setOutputPath(job1, job1_output_path);
        hdfs.delete(job1_output_path, true); // supprime le répertoire de sortie
 
        // Execute job and return status
        /*return*/ job1.waitForCompletion(true) /*? 0 : 1*/;

        Configuration conf2 = this.getConf();
            //   conf2.setInt("mapreduce.job.reduces", 1);
            //   conf2.setInt("mapreduce.map.maxattempts", 1);
            //   conf2.setInt("mapreduce.reduce.mxattempts", 1);
            //   conf2.setBoolean("mapreduce.map.speculative", true);
            //   conf2.setBoolean("mapreduce.reduce.speculative", true);
 
        Job job2 = Job.getInstance(conf2, "Top 10 UserAccAns Job");
        job2.setJarByClass(MRDriver.class);

        // Setup MapReduce job
        job2.setMapperClass(Top10UserAccAnsMapper.class);
        job2.setReducerClass(Top10UserAccAnsReducer.class);
        // job.setCombinerClass(Top10UserAccAnsReducer.class);
 
        // Specify key / value
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
 
        // Input
        FileInputFormat.addInputPath(job2, job1_output_path);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
 
        // Output
        Path job2_output_path = new Path("output-top10uaa");
        FileOutputFormat.setOutputPath(job2, job2_output_path);
        hdfs.delete(job2_output_path, true); // supprime le répertoire de sortie
 
        // Execute job and return status
        return job2.waitForCompletion(true) ? 0 : 1;
    }
}