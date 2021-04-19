package com.sunlands.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author chijiuwang@sunlands.com
 */
public class MyPartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int numPartitions) {
        String s = text.toString().split("\t")[1];
        if (Integer.parseInt(s) > 15) {
            return 1;
        } else {
            return 0;
        }
    }
}
