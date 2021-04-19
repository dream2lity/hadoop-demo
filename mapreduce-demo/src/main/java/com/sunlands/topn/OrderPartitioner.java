package com.sunlands.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author chijiuwang@sunlands.com
 */
public class OrderPartitioner extends Partitioner<OrderBean, Text> {
    @Override
    public int getPartition(OrderBean orderBean, Text text, int numPartitions) {
        return (orderBean.getId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
