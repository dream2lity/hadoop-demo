package com.sunlands.commonfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class Step2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        values.forEach(val -> stringBuilder.append(val).append("-"));
        context.write(key, new Text(stringBuilder.toString()));
    }
}
