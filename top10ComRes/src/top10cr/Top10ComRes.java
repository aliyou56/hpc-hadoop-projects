package top10cr;

// import java.util.Date;
// import java.io.IOException;
import java.net.URI;
// import java.net.URISyntaxException;
// import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Top10ComRes extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Top10ComRes(), args);  
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
      
        Configuration conf1 = this.getConf();
 
        // Create job
        Job job1 = Job.getInstance(conf1, "CommentAnswerUserId Job");
        job1.setJarByClass(Top10ComRes.class);
        // Input
        MultipleInputs.addInputPath(job1, new Path("/data/stackoverflow/Comments.xml"), TextInputFormat.class, CommentMapper.class);
        MultipleInputs.addInputPath(job1, new Path("/data/stackoverflow/Posts.xml"), TextInputFormat.class, PostMapper.class);
        // Setup MapReduce job
        job1.setReducerClass(ComResReducer.class);
        job1.setCombinerClass(ComResReducer.class);
        // Specify key / value
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        // Output
        FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf1);
        Path job1_output_path = new Path("output-caui");
        FileOutputFormat.setOutputPath(job1, job1_output_path);
        hdfs.delete(job1_output_path, true); // supprime le répertoire de sortie
 
        // Execute job and return status
        /*return */job1.waitForCompletion(true) /*? 0 : 1*/;

        Configuration conf = this.getConf();
              conf.setInt("mapreduce.job.reduces", 1);
              conf.setInt("mapreduce.map.maxattempts", 1);
              conf.setInt("mapreduce.reduce.mxattempts", 1);
              conf.setBoolean("mapreduce.map.speculative", true);
              conf.setBoolean("mapreduce.reduce.speculative", true);
 
        Job job = Job.getInstance(conf, "Top 10 CommentAnswerUserId Job");
        job.setJarByClass(Top10ComRes.class);
        // Setup MapReduce job
        job.setMapperClass(Top10ComResMapper.class);
        job.setReducerClass(Top10ComResReducer.class);
        // job.setCombinerClass(Top100Reducer.class);
        // Specify key / value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // Input
        FileInputFormat.addInputPath(job, job1_output_path);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // Output
        Path output_path = new Path("output-top10caui");
        FileOutputFormat.setOutputPath(job, output_path);
        hdfs.delete(output_path, true); // supprime le répertoire de sortie
        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }
}