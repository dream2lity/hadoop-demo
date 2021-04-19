package com.sunlands.commonfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class Step1Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        for (Text value : values) {
            stringBuilder.append(value.toString()).append("-");
        }
        context.write(new Text(stringBuilder.toString()), key);
    }
}
