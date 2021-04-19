package com.sunlands.smallfilemerge;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private boolean isRead = false;
    private Configuration configuration;
    private FileSystem fileSystem;
    private FileSplit fileSplit;
    private FSDataInputStream fsDataInputStream;
    private byte[] bytes;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
        fileSplit = (FileSplit) split;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!isRead) {
            fileSystem = FileSystem.newInstance(configuration);
            fsDataInputStream = fileSystem.open(fileSplit.getPath());

            int fileLength = (int) fileSplit.getLength();
            bytes = new byte[fileLength];
            IOUtils.readFully(fsDataInputStream, bytes, 0, fileLength);

            isRead = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        BytesWritable bytesWritable = new BytesWritable();
        bytesWritable.set(bytes, 0, (int) fileSplit.getLength());
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        fsDataInputStream.close();
        fileSystem.close();
    }
}
