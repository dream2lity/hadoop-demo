package com.sunlands.categoryout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author chijiuwang@sunlands.com
 */
public class CategoryOutMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "category_out");

        job.setJarByClass(CategoryOutMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///D:\\workspace\\projects\\hadoop-demo\\mapreduce-demo\\input\\category_out"));

        job.setMapperClass(CategoryOutMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(MyOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///D:\\workspace\\projects\\hadoop-demo\\mapreduce-demo\\output\\category_out\\category_out"));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new CategoryOutMain(), args);
        System.exit(run);
    }
}
