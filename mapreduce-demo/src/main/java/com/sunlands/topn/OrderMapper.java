package com.sunlands.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        OrderBean order = new OrderBean();
        order.setId(split[0]);
        order.setPrice(Double.parseDouble(split[2]));
        context.write(order, value);
    }
}
