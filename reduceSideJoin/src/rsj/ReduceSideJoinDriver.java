package rsj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceSideJoinDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        if (args.length != 4){
            System.err.println("Usage: ReduceSideJoin <user> <comment> <out> [inner|left|right|full|anti]");
            System.exit(1);
        }
        String joinType = args[3];
        if (!(joinType.equals("inner")|| joinType.equals("left")||
            joinType.equals("right")|| joinType.equals("full")||
            joinType.equals("anti"))) {
            System.err.println("Join type not set correctly");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Reduce Side Join");
        // Configure the join type
        job.getConfiguration().set("join.type", joinType);
        job.setJarByClass(ReduceSideJoinDriver.class);
        // Use multiple inputs to set which input uses what mapper
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentJoinMapper.class);

        job.setReducerClass(UserJoinReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 3);

    }
}