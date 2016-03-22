package com.epam.bigdata.second;

import com.epam.bigdata.second.model.RowNumberWritable;
import org.apache.hadoop.io.ByteWritable;

/**
 * Created by vsfmqueen on 10/4/15.
 */

public class LinePartitioner extends org.apache.hadoop.mapreduce.Partitioner<ByteWritable, RowNumberWritable> {
    @Override
    public int getPartition(ByteWritable key, RowNumberWritable value, int numPartitions) {
        if (key.get() == LineDriver.COUNTER_MARKER) {
            return value.getPartition();
        } else {
            return LinePartitioner.partitionForValue(value, numPartitions);
        }
    }

    public static int partitionForValue(RowNumberWritable value, int numPartitions) {
        return (value.getValue().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}