package com.epam.bigdata.second;

import com.epam.bigdata.second.model.RowNumberWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LineMapper extends Mapper<LongWritable, Text, ByteWritable, RowNumberWritable> {
    private long[] counters;
    private int numReduceTasks;

    private RowNumberWritable outputValue = new RowNumberWritable();
    private ByteWritable outputKey = new ByteWritable();

    protected void setup(Context context) throws IOException, InterruptedException {
        numReduceTasks = context.getNumReduceTasks();
        counters = new long[numReduceTasks];
        outputKey.set(LineDriver.VALUE_MARKER);
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        outputValue.setValue(value);
        context.write(outputKey, outputValue);
        counters[LinePartitioner.partitionForValue(outputValue, numReduceTasks)]++;
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputKey.set(LineDriver.COUNTER_MARKER);
        for(int c = 0; c < counters.length - 1; c++) {
            //calculating the global offset
            if (counters[c] > 0) {
                //adding the value of previous counter to the current one
                outputValue.setCounter(c + 1, counters[c]);
                context.write(outputKey, outputValue);
            }
            counters[c + 1] += counters[c];
        }
    }
}