package com.sunlands.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text mapOutputKey = new Text();
        LongWritable mapOutputValue = new LongWritable();
        String[] split = value.toString().split(",");
        for (String s : split) {
            mapOutputKey.set(s);
            mapOutputValue.set(1);
            context.write(mapOutputKey, mapOutputValue);
        }
    }
}
