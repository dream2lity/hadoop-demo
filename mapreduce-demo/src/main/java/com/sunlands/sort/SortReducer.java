package com.sunlands.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class SortReducer extends Reducer<DemoBean, NullWritable, DemoBean, NullWritable> {
    @Override
    protected void reduce(DemoBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
