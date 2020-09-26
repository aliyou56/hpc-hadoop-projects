package average;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Average {
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        Configuration conf = new Configuration();  
        conf.setInt("mapreduce.job.reduces", 1);
        conf.setInt("mapreduce.map.maxattempts", 1);
        conf.setInt("mapreduce.reduce.mxattempts", 1);
        conf.setBoolean("mapreduce.map.speculative", true);
        conf.setBoolean("mapreduce.reduce.speculative", true);

        Job job = Job.getInstance(conf, "Job Average");

        job.setJarByClass(Average.class);
        job.setMapperClass(AverageMapper.class);
        job.setCombinerClass(AverageCombiner.class);
        job.setReducerClass(AverageReducer.class);

        // Input and outupt format
        job.setInputFormatClass(TextInputFormat.class); 
        job.setOutputFormatClass(TextOutputFormat.class);

        // Mapper Output
        job.setMapOutputKeyClass(IntWritable.class);       
        job.setMapOutputValueClass(AverageTuple.class); 

        job.setOutputKeyClass(IntWritable.class); 
        job.setOutputValueClass(AverageTuple.class);

        FileInputFormat.setInputPaths(job, new Path("/data/stackoverflow/Comments.xml"));
        Path output_path = new Path("output-avg");
        FileOutputFormat.setOutputPath(job, output_path);
        
        FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf);
        hdfs.delete(output_path, true); // supprime le répertoire de sortie

        job.waitForCompletion(true);  
    }
}