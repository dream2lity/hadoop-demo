package com.sunlands.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder users = new StringBuilder();
        String org = "";
        for (Text value : values) {
            if (value.toString().split(",").length == 3) {
                org = value.toString();
            } else {
                users.append("\t").append(value.toString());
            }
        }
        context.write(key, new Text(org + users.toString()));
    }
}
