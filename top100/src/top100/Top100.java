
package top100;

import java.util.Date;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;  

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Top100 extends Configured implements Tool {

    public static void main(String[] args) throws Exception { 
      int exitCode = ToolRunner.run(new Top100(), args);  
      System.exit(exitCode);
    }
 
    public int run(String[] args) throws Exception {
      
        Configuration conf = this.getConf();
  //       conf.setInt("mapreduce.job.reduces", 1);
  //       conf.setInt("mapreduce.map.maxattempts", 1);
  //       conf.setInt("mapreduce.reduce.mxattempts", 1);
  //       conf.setBoolean("mapreduce.map.speculative", true);
  //       conf.setBoolean("mapreduce.reduce.speculative", true);
 
        // Create job
        Job job = Job.getInstance(conf, "Top 100 Job");
        job.setJarByClass(Top100.class);
 
        // Setup MapReduce job
        job.setMapperClass(Top100Mapper.class);
        job.setReducerClass(Top100Reducer.class);
        job.setCombinerClass(Top100Reducer.class);
 
        // Specify key / value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
 
        // Input
        FileInputFormat.addInputPath(job, new Path("output-wc"));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
 
        // Output
        FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf);
        Path output_path = new Path("output-top100");
        FileOutputFormat.setOutputPath(job, output_path);
        hdfs.delete(output_path, true); // supprime le répertoire de sortie
 
        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

  // public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {


  //       Job job = Job.getInstance(conf, "Job Top100");

  //       job.setJarByClass(Top100.class);
  //       job.setMapperClass(Top100Mapper.class);
  //       // job.setCombinerClass(Top100.class);
  //       job.setReducerClass(Top100Reducer.class);

  //       job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

  //       // Input and outupt format
  //       job.setInputFormatClass(KeyValueTextInputFormat.class); 
  //       job.setOutputFormatClass(TextOutputFormat.class);

  //       // Mapper Output
  //       job.setMapOutputKeyClass(IntWritable.class);       
  //       job.setMapOutputValueClass(Text.class); 

  //       job.setOutputKeyClass(IntWritable.class); 
  //       job.setOutputValueClass(Text.class);

  //       FileInputFormat.setInputPaths(job, new Path("output-wc"));
        
  //       FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf);
  //       Path output_path = new Path("output-top100");
  //       FileOutputFormat.setOutputPath(job, output_path);
  //       hdfs.delete(output_path, true); // supprime le répertoire de sortie

  //       job.waitForCompletion(true); 

  // }
  // public int run(String[] args) throws Exception {
  //     JobControl jobControl = new JobControl("jobChain"); 
  //     // Configuration conf1 = getConf();
  //     Configuration conf1 = new Configuration();  
  //     conf1.setInt("mapreduce.job.reduces", 1);
  //     conf1.setInt("mapreduce.map.maxattempts", 1);
  //     conf1.setInt("mapreduce.reduce.maxattempts", 1);
  //     conf1.setBoolean("mapreduce.map.speculative", true);
  //     conf1.setBoolean("mapreduce.reduce.speculative", true);

  //     Job job1 = Job.getInstance(conf1, "wordCount");  
  //     job1.setJarByClass(Top100.class);

  //     job1.addCacheFile(new URI("/data/stop-words/stop-words-english4.txt" + "#stopWords"));

  //     FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf1);
  //     Path job1_output_path = new Path("output-wc");
  //     hdfs.delete(job1_output_path, true); // supprime le répertoire de sortie

  //     FileInputFormat.setInputPaths(job1, new Path("/data/Gutenberg"));
  //     FileOutputFormat.setOutputPath(job1, job1_output_path);

  //     job1.setMapperClass(WordCountMapper.class);
  //     job1.setCombinerClass(WordCountReducer.class);
  //     job1.setReducerClass(WordCountReducer.class);

  //   job1.setMapOutputKeyClass(Text.class);
  //   job1.setMapOutputValueClass(IntWritable.class);
  //     job1.setOutputKeyClass(Text.class);
  //     job1.setOutputValueClass(IntWritable.class);

  //   job1.setInputFormatClass(TextInputFormat.class);
  //   job1.setOutputFormatClass(TextOutputFormat.class);

  //     ControlledJob controlledJob1 = new ControlledJob(conf1);
  //     controlledJob1.setJob(job1);

  //     jobControl.addJob(controlledJob1);


  //   // Configuration conf2 = getConf();
  //     Configuration conf2 = new Configuration();  
  //     conf2.setInt("mapreduce.job.reduces", 1);
  //     conf2.setInt("mapreduce.map.maxattempts", 1);
  //     conf2.setInt("mapreduce.reduce.maxattempts", 1);
  //     conf2.setBoolean("mapreduce.map.speculative", true);
  //     conf2.setBoolean("mapreduce.reduce.speculative", true);

  //     Job job2 = Job.getInstance(conf2, "job-top100");
  //     job2.setJarByClass(Top100.class);

  //     FileInputFormat.setInputPaths(job2, job1_output_path);
  //     FileOutputFormat.setOutputPath(job2, new Path("output-Tp100"));

  //     job2.setMapperClass(Top100Mapper.class);
  //     job2.setCombinerClass(Top100Reducer.class);
  //     job2.setReducerClass(Top100Reducer.class);

  //   job2.setMapOutputKeyClass(LongWritable.class);
  //   job2.setMapOutputValueClass(Text.class);
  //     job2.setOutputKeyClass(LongWritable.class);
  //     job2.setOutputValueClass(Text.class);

  //     job2.setInputFormatClass(KeyValueTextInputFormat.class);
  //   job2.setOutputFormatClass(TextOutputFormat.class);

  //     job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

  //     ControlledJob controlledJob2 = new ControlledJob(conf2);
  //     controlledJob2.setJob(job2);

  //     // make job2 dependent on job1
  //     controlledJob2.addDependingJob(controlledJob1); 
  //     // add the job to the job control
  //     jobControl.addJob(controlledJob2);
  //     Thread jobControlThread = new Thread(jobControl);
  //     jobControlThread.start();

  //     while (!jobControl.allFinished()) {
  //       System.out.println(new Date());
  //         System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
  //         System.out.println("Jobs in ready state  : " + jobControl.getReadyJobsList().size());
  //         System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
  //         System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
  //         System.out.println("Jobs in failed state : " + jobControl.getFailedJobList().size());
  //         try {
  //           Thread.sleep(30000);
  //         } catch (Exception e) {}
  //     } 
  //     System.exit(0);  
  //     return (job2.waitForCompletion(true) ? 0 : 1);   
  // } 
  
  // public static void main(String[] args) throws Exception { 
  //   int exitCode = ToolRunner.run(new Top100(), args);  
  //   System.exit(exitCode);
  // }
}









  //   Configuration conf = new Configuration();  
  //   conf.setInt("mapreduce.job.reduces", 1);
  //   conf.setInt("mapreduce.map.maxattempts", 1);
  //   conf.setInt("mapreduce.reduce.maxattempts", 1);
  //   conf.setBoolean("mapreduce.map.speculative", true);
  //   conf.setBoolean("mapreduce.reduce.speculative", true);

  //   Job job = Job.getInstance(conf, "wordCount");
  //   job.setJarByClass(Top100.class);
  //   job.setMapperClass(WordCountMapper.class);
  //   job.setCombinerClass(WordCountReducer.class);
  //   job.setReducerClass(WordCountReducer.class);

  //   job.addCacheFile(new URI("/data/stop-words/stop-words-english4.txt" + "#stopWords"));

  //   job.setMapOutputKeyClass(Text.class);
  //   job.setMapOutputValueClass(IntWritable.class);
  //   job.setOutputKeyClass(Text.class);
  //   job.setOutputValueClass(IntWritable.class);

  //   job.setInputFormatClass(TextInputFormat.class);
  //   job.setOutputFormatClass(TextOutputFormat.class);

  //   FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf);
  //   Path job1_output_path = new Path("output-wordcount");
  //   hdfs.delete(job1_output_path, true); // supprime le répertoire de sortie

  //   FileInputFormat.setInputPaths(job, new Path("/data/Gutenberg"));
  //   FileOutputFormat.setOutputPath(job, job1_output_path);

  //   ControlledJob job1Ctrl = new ControlledJob(conf);
  //   job1Ctrl.setJob(job);


  //   Configuration conf2 = new Configuration();  
  //   conf2.setInt("mapreduce.job.reduces", 1);
  //   conf2.setInt("mapreduce.map.maxattempts", 1);
  //   conf2.setInt("mapreduce.reduce.maxattempts", 1);
  //   conf2.setBoolean("mapreduce.map.speculative", true);
  //   conf2.setBoolean("mapreduce.reduce.speculative", true);

  //   Job job2 = Job.getInstance(conf2, "top100");
  //   job2.setJarByClass(Top100.class);
  //   job2.setMapperClass(Top100Mapper.class);
  //   job2.setCombinerClass(Top100Reducer.class);
  //   job2.setReducerClass(Top100Reducer.class);

  //   job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

  //   job2.setMapOutputKeyClass(Text.class);
  //   job2.setMapOutputValueClass(IntWritable.class);
  //   job2.setOutputKeyClass(Text.class);
  //   job2.setOutputValueClass(IntWritable.class);

  //   job2.setInputFormatClass(KeyValueTextInputFormat.class);
  //   job2.setOutputFormatClass(TextOutputFormat.class);

  //   FileSystem hdfs2 = FileSystem.get(new URI("hdfs://hnn:9000"), conf2);
  //   Path job2_output_path = new Path("output-Top100");
  //   // hdfs2.delete(job2_output_path, true); // supprime le répertoire de sortie

  //   FileInputFormat.setInputPaths(job2, job1_output_path);
  //   FileOutputFormat.setOutputPath(job2, job2_output_path);

  //   ControlledJob job2Ctrl = new ControlledJob(conf2);
  //   job2Ctrl.setJob(job2);

  //   JobControl jobControl = new JobControl("job-control");
  //   jobControl.addJob(job1Ctrl);
  //   jobControl.addJob(job2Ctrl);
  //   job2Ctrl.addDependingJob(job1Ctrl);

  //   Thread jobControlThread = new Thread(jobControl);
  //   jobControlThread.start();
  //   jobControlThread.join(); 

  //   // job.waitForCompletion(true);

  //   // Counter match_counter = job.getCounters().findCounter(MapCounters.MATCHED_WORDS);
  //   // System.out.println("number of matched words= " + match_counter.getValue());
  //   // Counter index_counter = job.getCounters().findCounter(ReduceCounters.INDEXED_WORDS);
  //   // System.out.println("number of indexed words= " + index_counter.getValue());

  //   // Counter accept_counter = job.getCounters().findCounter(MapCounters.ACCEPT_CNT);
  //   // System.out.println("number of accepted words= " + accept_counter.getValue());
  //   // Counter reject_counter = job.getCounters().findCounter(MapCounters.REJECT_CNT);
  //   // System.out.println("number of rejected words= " + reject_counter.getValue());
  //   // Counter index_counter = job.getCounters().findCounter(ReduceCounters.INDEXED_WORDS);
  //   // System.out.println("number of indexed words= " + index_counter.getValue());
  // }

// }
