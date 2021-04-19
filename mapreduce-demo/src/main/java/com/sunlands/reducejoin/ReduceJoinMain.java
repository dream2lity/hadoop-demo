package com.sunlands.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author chijiuwang@sunlands.com
 */
public class ReduceJoinMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "reduce_join");

        job.setJarByClass(ReduceJoinMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        /*TextInputFormat.addInputPath(job, new Path("file:///D:\\workspace\\projects\\hadoop-demo\\mapreduce-demo\\input\\reduce_join"));*/
        TextInputFormat.addInputPath(job, new Path("hdfs://172.16.117.73:9000/user/eagle/test/reduce_join_input"));

        job.setMapperClass(ReduceJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        /*TextOutputFormat.setOutputPath(job, new Path("file:///D:\\workspace\\projects\\hadoop-demo\\mapreduce-demo\\output\\reduce_join"));*/
        TextOutputFormat.setOutputPath(job, new Path("hdfs://172.16.117.73:9000/user/eagle/test/reduce_join_output"));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new ReduceJoinMain(), args);
        System.exit(run);
    }
}
