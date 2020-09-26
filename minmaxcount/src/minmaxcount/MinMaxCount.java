package minmaxcount;

// import java.io.IOException;
// import java.net.URI;
// import java.net.URISyntaxException;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Counter;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
// import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// public class MinMaxCount {

//     public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

//         Configuration conf = new Configuration();  

//         conf.setInt("mapred.reduce.tasks",1);
//         conf.setInt("mapred.map.max.attempts",1);
//         conf.setInt("mapred.reduce.max.attempts",1);
//         conf.setBoolean("mapred.map.tasks.speculative.execution",true);
//         conf.setBoolean("mapred.reduce.tasks.speculative.execution",true);

//         Job job= Job.getInstance(conf,"MinMaxCount");
//         job.setJarByClass(MinMaxCount.class);

//         //Classe du mapper
//         job.setMapperClass(MinMaxCountMapper.class);
//         //Classe du combiner (même que le reducer)
//         job.setCombinerClass(MinMaxCountReducer.class);
//         //classe du reducer
//         job.setReducerClass(MinMaxCountReducer.class);

//         //Format d'entrée (ici on lit ligne par ligne un fichier)
//         job.setInputFormatClass(TextInputFormat.class);
//         //Type de clé en sortie de map
//         job.setMapOutputKeyClass(IntWritable.class);
//         //Type de la valeur en sortie de map
//         job.setMapOutputValueClass(MinMaxCountTuple.class);

//         //Format de sortie
//         job.setOutputFormatClass(TextOutputFormat.class);
//         //Type de la clé en sortie du combiner et du reducer
//         job.setOutputKeyClass(IntWritable.class);
//         //Type de la valeur en sortie du combiner et du reducer
//         job.setOutputValueClass(MinMaxCountTuple.class);

//         //Chemin du fichier à lire
//         FileInputFormat.setInputPaths(job, new Path("/data/stackoverflow/Comments.xml"));

//         FileSystem hdfs= FileSystem.get(new URI("hdfs://hnn:9000"),conf);

//         //Chemin de sortie
//         Path output_path= new Path("output-minmaxcount");
//         FileOutputFormat.setOutputPath(job, output_path);
//         //supprime le répertoire de sortie
//         hdfs.delete(output_path, true); 


//         job.waitForCompletion(true);
//     }

// }


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MinMaxCount {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        Configuration conf = new Configuration();  
        conf.setInt("mapreduce.job.reduces", 1);
        conf.setInt("mapreduce.map.maxattempts", 1);
        conf.setInt("mapreduce.reduce.maxattempts", 1);
        conf.setBoolean("mapreduce.map.speculative", true);
        conf.setBoolean("mapreduce.reduce.speculative", true);

        Job job = Job.getInstance(conf, "Min Max Count");

        job.setJarByClass(MinMaxCount.class);
        job.setMapperClass(MinMaxCountMapper.class);
        job.setCombinerClass(MinMaxCountReducer.class);
        job.setReducerClass(MinMaxCountReducer.class);

        // Input and outupt format
        job.setInputFormatClass(TextInputFormat.class); 
        job.setOutputFormatClass(TextOutputFormat.class);

        // Mapper Output
        job.setMapOutputKeyClass(IntWritable.class);       
        job.setMapOutputValueClass(MinMaxCountTuple.class); 

        job.setOutputKeyClass(IntWritable.class); 
        job.setOutputValueClass(MinMaxCountTuple.class);

        Path output_path = new Path("output-minmaxcount");
        FileOutputFormat.setOutputPath(job, output_path);
        FileInputFormat.setInputPaths(job, new Path("/data/stackoverflow/Comments.xml"));
        
        FileSystem hdfs = FileSystem.get(new URI("hdfs://hnn:9000"), conf);
        hdfs.delete(output_path, true); // supprime le répertoire de sortie

        job.waitForCompletion(true);   
    }
}