package com.sunlands.categoryout;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FSDataOutputStream one = fileSystem.create(new Path("file:///D:\\workspace\\projects\\hadoop-demo\\mapreduce-demo\\output\\category_out\\category_out_1\\1.txt"));
        FSDataOutputStream two = fileSystem.create(new Path("file:///D:\\workspace\\projects\\hadoop-demo\\mapreduce-demo\\output\\category_out\\category_out_2\\2.txt"));

        return new MyRecordWriter(one, two);
    }
}
