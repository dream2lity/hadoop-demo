package com.sunlands.commonfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author chijiuwang@sunlands.com
 */
public class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String[] strings = split[0].split("-");
        Arrays.sort(strings);
        for (int i = 0; i < strings.length; i++) {
            for (int j = i + 1; j < strings.length; j++) {
                context.write(new Text(strings[i] + "-" + strings[j]), new Text(split[1]));
            }
        }
    }
}
