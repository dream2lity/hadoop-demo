package com.sunlands.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class SortMapper extends Mapper<LongWritable, Text, DemoBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        DemoBean bean = new DemoBean();
        bean.setName(split[0]);
        bean.setAge(Integer.parseInt(split[1]));
        context.write(bean, NullWritable.get());
    }
}
