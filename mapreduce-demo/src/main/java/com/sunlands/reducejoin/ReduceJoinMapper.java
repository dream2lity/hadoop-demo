package com.sunlands.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String[] split = value.toString().split(",");
        if ("org.txt".equals(fileName)) {
            context.write(new Text(split[0]), value);
        } else {
            context.write(new Text(split[3]), value);
        }
    }
}
