package com.sunlands.categoryout;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author chijiuwang@sunlands.com
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream one;
    private FSDataOutputStream two;

    public MyRecordWriter() {
    }

    public MyRecordWriter(FSDataOutputStream one, FSDataOutputStream two) {
        this.one = one;
        this.two = two;
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String s = key.toString().split(",")[1];
        if ("1".equals(s)) {
            one.write(key.toString().getBytes(StandardCharsets.UTF_8));
            one.write("\r\n".getBytes(StandardCharsets.UTF_8));
        } else {
            two.write(key.toString().getBytes(StandardCharsets.UTF_8));
            two.write("\r\n".getBytes(StandardCharsets.UTF_8));

        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeQuietly(one);
        IOUtils.closeQuietly(two);
    }
}
