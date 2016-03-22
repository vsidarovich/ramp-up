package com.epam.bigdata.second;

import com.epam.bigdata.second.model.RowNumberWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class LineReducer extends Reducer<ByteWritable, RowNumberWritable, Text, Text> {
    private Text outputKey = new Text();

    protected void reduce(ByteWritable key, Iterable<RowNumberWritable> values, Context context)
            throws IOException, InterruptedException {
        Iterator<RowNumberWritable> itr = values.iterator();
        if (!itr.hasNext()) {
            return;
        }

        long offset = 0;
        RowNumberWritable value = itr.next();

        while (itr.hasNext() && value.getCount() > 0) {
            offset += value.getCount();
            value = itr.next();
        }

        outputKey.set(Long.toString(offset++));
        context.write(outputKey, value.getValue());

        while(itr.hasNext()) {
            value = itr.next();
            outputKey.set(Long.toString(offset++));
            context.write(outputKey, value.getValue());
        }
    }
}